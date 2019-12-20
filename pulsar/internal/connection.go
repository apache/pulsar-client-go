// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/apache/pulsar-client-go/pulsar/internal/auth"
	"github.com/apache/pulsar-client-go/pulsar/internal/pb"
)

type TLSOptions struct {
	TrustCertsFilePath      string
	AllowInsecureConnection bool
	ValidateHostname        bool
}

// ConnectionListener is a user of a connection (eg. a producer or
// a consumer) that can register itself to get notified
// when the connection is closed.
type ConnectionListener interface {
	// ReceivedSendReceipt receive and process the return value of the send command.
	ReceivedSendReceipt(response *pb.CommandSendReceipt)

	// ConnectionClosed close the TCP connection.
	ConnectionClosed()
}

// Connection is a interface of client cnx.
type Connection interface {
	SendRequest(requestID uint64, req *pb.BaseCommand, callback func(*pb.BaseCommand, error))
	SendRequestNoWait(req *pb.BaseCommand)
	WriteData(data []byte)
	RegisterListener(id uint64, listener ConnectionListener)
	UnregisterListener(id uint64)
	AddConsumeHandler(id uint64, handler ConsumerHandler)
	DeleteConsumeHandler(id uint64)
	ID() string
	Close()
}

type ConsumerHandler interface {
	MessageReceived(response *pb.CommandMessage, headersAndPayload Buffer) error

	// ConnectionClosed close the TCP connection.
	ConnectionClosed()
}

type connectionState int

const (
	connectionInit connectionState = iota
	connectionConnecting
	connectionTCPConnected
	connectionReady
	connectionClosed
)

func (s connectionState) String() string {
	switch s {
	case connectionInit:
		return "Initializing"
	case connectionConnecting:
		return "Connecting"
	case connectionTCPConnected:
		return "TCPConnected"
	case connectionReady:
		return "Ready"
	case connectionClosed:
		return "Closed"
	default:
		return "Unknown"
	}
}

const keepAliveInterval = 30 * time.Second

type request struct {
	id       *uint64
	cmd      *pb.BaseCommand
	callback func(command *pb.BaseCommand, err error)
}

type incomingCmd struct {
	cmd               *pb.BaseCommand
	headersAndPayload Buffer
}

type connection struct {
	sync.Mutex
	cond              *sync.Cond
	state             connectionState
	connectionTimeout time.Duration

	logicalAddr  *url.URL
	physicalAddr *url.URL
	cnx          net.Conn

	writeBufferLock sync.Mutex
	writeBuffer     Buffer
	reader          *connectionReader

	lastDataReceivedLock sync.Mutex
	lastDataReceivedTime time.Time
	pingTicker           *time.Ticker
	pingCheckTicker      *time.Ticker

	log *log.Entry

	requestIDGenerator uint64

	incomingRequestsCh chan *request
	incomingCmdCh      chan *incomingCmd
	closeCh            chan interface{}
	writeRequestsCh    chan []byte

	pendingReqs map[uint64]*request
	listeners   map[uint64]ConnectionListener

	consumerHandlersLock sync.RWMutex
	consumerHandlers     map[uint64]ConsumerHandler

	tlsOptions *TLSOptions
	auth       auth.Provider
}

func newConnection(logicalAddr *url.URL, physicalAddr *url.URL, tlsOptions *TLSOptions,
	connectionTimeout time.Duration, auth auth.Provider) *connection {
	cnx := &connection{
		state:                connectionInit,
		connectionTimeout:    connectionTimeout,
		logicalAddr:          logicalAddr,
		physicalAddr:         physicalAddr,
		writeBuffer:          NewBuffer(4096),
		log:                  log.WithField("remote_addr", physicalAddr),
		pendingReqs:          make(map[uint64]*request),
		lastDataReceivedTime: time.Now(),
		pingTicker:           time.NewTicker(keepAliveInterval),
		pingCheckTicker:      time.NewTicker(keepAliveInterval),
		tlsOptions:           tlsOptions,
		auth:                 auth,

		closeCh:            make(chan interface{}),
		incomingRequestsCh: make(chan *request, 10),
		incomingCmdCh:      make(chan *incomingCmd, 10),
		writeRequestsCh:    make(chan []byte, 10),
		listeners:          make(map[uint64]ConnectionListener),
		consumerHandlers:   make(map[uint64]ConsumerHandler),
	}
	cnx.reader = newConnectionReader(cnx)
	cnx.cond = sync.NewCond(cnx)
	return cnx
}

func (c *connection) start() {
	// Each connection gets its own goroutine that will
	go func() {
		if c.connect() {
			if c.doHandshake() {
				c.run()
			} else {
				c.changeState(connectionClosed)
			}
		} else {
			c.changeState(connectionClosed)
		}
	}()
}

func (c *connection) connect() bool {
	c.log.Info("Connecting to broker")

	var (
		err       error
		cnx       net.Conn
		tlsConfig *tls.Config
	)

	if c.tlsOptions == nil {
		// Clear text connection
		cnx, err = net.DialTimeout("tcp", c.physicalAddr.Host, c.connectionTimeout)
	} else {
		// TLS connection
		tlsConfig, err = c.getTLSConfig()
		if err != nil {
			c.log.WithError(err).Warn("Failed to configure TLS ")
			return false
		}

		d := &net.Dialer{Timeout: c.connectionTimeout}
		cnx, err = tls.DialWithDialer(d, "tcp", c.physicalAddr.Host, tlsConfig)
	}

	if err != nil {
		c.log.WithError(err).Warn("Failed to connect to broker.")
		c.Close()
		return false
	}

	c.Lock()
	c.cnx = cnx
	c.log = c.log.WithField("local_addr", c.cnx.LocalAddr())
	c.log.Info("TCP connection established")
	c.Unlock()

	c.changeState(connectionTCPConnected)

	return true
}

func (c *connection) doHandshake() bool {
	// Send 'Connect' command to initiate handshake
	version := int32(pb.ProtocolVersion_v13)

	authData, err := c.auth.GetData()
	if err != nil {
		c.log.WithError(err).Warn("Failed to load auth credentials")
		return false
	}

	// During the initial handshake, the internal keep alive is not
	// active yet, so we need to timeout write and read requests
	c.cnx.SetDeadline(time.Now().Add(keepAliveInterval))

	c.writeCommand(baseCommand(pb.BaseCommand_CONNECT, &pb.CommandConnect{
		ProtocolVersion: &version,
		ClientVersion:   proto.String("Pulsar Go 0.1"),
		AuthMethodName:  proto.String(c.auth.Name()),
		AuthData:        authData,
	}))

	cmd, _, err := c.reader.readSingleCommand()
	if err != nil {
		c.log.WithError(err).Warn("Failed to perform initial handshake")
		return false
	}

	// Reset the deadline so that we don't use read timeouts
	c.cnx.SetDeadline(time.Time{})

	if cmd.Connected == nil {
		c.log.Warnf("Failed to perform initial handshake - Expecting 'Connected' cmd, got '%s'",
			cmd.Type)
		return false
	}

	c.log.Info("Connection is ready")
	c.changeState(connectionReady)
	return true
}

func (c *connection) waitUntilReady() error {
	c.Lock()
	defer c.Unlock()

	for c.state != connectionReady {
		c.log.Debug("Wait until connection is ready. State: ", c.state)
		if c.state == connectionClosed {
			return errors.New("connection error")
		}
		// wait for a new connection state change
		c.cond.Wait()
	}

	return nil
}

func (c *connection) run() {
	// All reads come from the reader goroutine
	go c.reader.readFromConnection()
	go c.runPingCheck()

	for {
		select {
		case <- c.closeCh:
			c.Close()
			return

		case req := <-c.incomingRequestsCh:
			if req == nil {
				return
			}
			c.internalSendRequest(req)

		case cmd := <-c.incomingCmdCh:
			c.internalReceivedCommand(cmd.cmd, cmd.headersAndPayload)

		case data := <-c.writeRequestsCh:
			if data == nil {
				return
			}
			c.internalWriteData(data)

		case _ = <-c.pingTicker.C:
			c.sendPing()
		}
	}
}

func (c *connection) runPingCheck() {
	for {
		select {
		case <- c.closeCh:
			return
		case _ = <-c.pingCheckTicker.C:
			if c.lastDataReceived().Add(2 * keepAliveInterval).Before(time.Now()) {
				// We have not received a response to the previous Ping request, the
				// connection to broker is stale
				c.log.Warn("Detected stale connection to broker")
				c.TriggerClose()
				return
			}
		}
	}
}

func (c *connection) WriteData(data []byte) {
	c.writeRequestsCh <- data
}

func (c *connection) internalWriteData(data []byte) {
	c.log.Debug("Write data: ", len(data))
	if _, err := c.cnx.Write(data); err != nil {
		c.log.WithError(err).Warn("Failed to write on connection")
		c.Close()
	}
}

func (c *connection) writeCommand(cmd proto.Message) {
	// Wire format
	// [FRAME_SIZE] [CMD_SIZE][CMD]
	cmdSize := uint32(proto.Size(cmd))
	frameSize := cmdSize + 4

	c.writeBufferLock.Lock()
	defer c.writeBufferLock.Unlock()

	c.writeBuffer.Clear()
	c.writeBuffer.WriteUint32(frameSize)
	c.writeBuffer.WriteUint32(cmdSize)
	serialized, err := proto.Marshal(cmd)
	if err != nil {
		c.log.WithError(err).Fatal("Protobuf serialization error")
	}

	c.writeBuffer.Write(serialized)
	data := c.writeBuffer.ReadableSlice()
	c.internalWriteData(data)
}

func (c *connection) receivedCommand(cmd *pb.BaseCommand, headersAndPayload Buffer) {
	c.incomingCmdCh <- &incomingCmd{cmd, headersAndPayload}
}

func (c *connection) internalReceivedCommand(cmd *pb.BaseCommand, headersAndPayload Buffer) {
	c.log.Debugf("Received command: %s -- payload: %v", cmd, headersAndPayload)
	c.setLastDataReceived(time.Now())

	switch *cmd.Type {
	case pb.BaseCommand_SUCCESS:
		c.handleResponse(cmd.Success.GetRequestId(), cmd)

	case pb.BaseCommand_PRODUCER_SUCCESS:
		c.handleResponse(cmd.ProducerSuccess.GetRequestId(), cmd)

	case pb.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		c.handleResponse(cmd.PartitionMetadataResponse.GetRequestId(), cmd)

	case pb.BaseCommand_LOOKUP_RESPONSE:
		lookupResult := cmd.LookupTopicResponse
		c.handleResponse(lookupResult.GetRequestId(), cmd)

	case pb.BaseCommand_CONSUMER_STATS_RESPONSE:
		c.handleResponse(cmd.ConsumerStatsResponse.GetRequestId(), cmd)

	case pb.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE:
		c.handleResponse(cmd.GetLastMessageIdResponse.GetRequestId(), cmd)

	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE_RESPONSE:
		c.handleResponse(cmd.GetTopicsOfNamespaceResponse.GetRequestId(), cmd)

	case pb.BaseCommand_GET_SCHEMA_RESPONSE:
		c.handleResponse(cmd.GetSchemaResponse.GetRequestId(), cmd)

	case pb.BaseCommand_ERROR:
		c.handleResponseError(cmd.GetError())

	case pb.BaseCommand_CLOSE_PRODUCER:
		c.handleCloseProducer(cmd.GetCloseProducer())
	case pb.BaseCommand_CLOSE_CONSUMER:
		c.handleCloseConsumer(cmd.GetCloseConsumer())

	case pb.BaseCommand_SEND_RECEIPT:
		c.handleSendReceipt(cmd.GetSendReceipt())

	case pb.BaseCommand_SEND_ERROR:

	case pb.BaseCommand_MESSAGE:
		c.handleMessage(cmd.GetMessage(), headersAndPayload)

	case pb.BaseCommand_PING:
		c.handlePing()
	case pb.BaseCommand_PONG:
		c.handlePong()

	case pb.BaseCommand_ACTIVE_CONSUMER_CHANGE:

	default:
		c.log.Errorf("Received invalid command type: %s", cmd.Type)
		c.Close()
	}
}

func (c *connection) Write(data []byte) {
	c.writeRequestsCh <- data
}

func (c *connection) SendRequest(requestID uint64, req *pb.BaseCommand, callback func(command *pb.BaseCommand, err error)) {
	c.incomingRequestsCh <- &request{
		id:       &requestID,
		cmd:      req,
		callback: callback,
	}
}

func (c *connection) SendRequestNoWait(req *pb.BaseCommand) {
	c.incomingRequestsCh <- &request{
		id:       nil,
		cmd:      req,
		callback: nil,
	}
}

func (c *connection) internalSendRequest(req *request) {
	if req.id != nil {
		c.pendingReqs[*req.id] = req
	}
	c.writeCommand(req.cmd)
}

func (c *connection) handleResponse(requestID uint64, response *pb.BaseCommand) {
	request, ok := c.pendingReqs[requestID]
	if !ok {
		c.log.Warnf("Received unexpected response for request %d of type %s", requestID, response.Type)
		return
	}

	delete(c.pendingReqs, requestID)
	request.callback(response, nil)
}

func (c *connection) handleResponseError(serverError *pb.CommandError) {
	requestID := serverError.GetRequestId()
	request, ok := c.pendingReqs[requestID]
	if !ok {
		c.log.Warnf("Received unexpected error response for request %d of type %s",
			requestID, serverError.GetError())
		return
	}

	delete(c.pendingReqs, requestID)

	request.callback(nil,
		errors.New(fmt.Sprintf("server error: %s: %s", serverError.GetError(), serverError.GetMessage())))
}

func (c *connection) handleSendReceipt(response *pb.CommandSendReceipt) {
	producerID := response.GetProducerId()
	if producer, ok := c.listeners[producerID]; ok {
		producer.ReceivedSendReceipt(response)
	} else {
		c.log.WithField("producerID", producerID).Warn("Got unexpected send receipt for message: ", response.MessageId)
	}
}

func (c *connection) handleMessage(response *pb.CommandMessage, payload Buffer) {
	c.log.Debug("Got Message: ", response)
	consumerID := response.GetConsumerId()
	if consumer, ok := c.consumerHandler(consumerID); ok {
		err := consumer.MessageReceived(response, payload)
		if err != nil {
			c.log.WithField("consumerID", consumerID).Error("handle message err: ", response.MessageId)
		}
	} else {
		c.log.WithField("consumerID", consumerID).Warn("Got unexpected message: ", response.MessageId)
	}
}

func (c *connection) lastDataReceived() time.Time {
	c.lastDataReceivedLock.Lock()
	defer c.lastDataReceivedLock.Unlock()
	t := c.lastDataReceivedTime
	return t
}

func (c *connection) setLastDataReceived(t time.Time) {
	c.lastDataReceivedLock.Lock()
	defer c.lastDataReceivedLock.Unlock()
	c.lastDataReceivedTime = t
}

func (c *connection) sendPing() {
	c.log.Debug("Sending PING")
	c.writeCommand(baseCommand(pb.BaseCommand_PING, &pb.CommandPing{}))
}

func (c *connection) handlePong() {
	c.log.Debug("Received PONG response")
}

func (c *connection) handlePing() {
	c.log.Debug("Responding to PING request")
	c.writeCommand(baseCommand(pb.BaseCommand_PONG, &pb.CommandPong{}))
}

func (c *connection) handleCloseConsumer(closeConsumer *pb.CommandCloseConsumer) {
	c.log.Infof("Broker notification of Closed consumer: %d", closeConsumer.GetConsumerId())
	consumerID := closeConsumer.GetConsumerId()
	if consumer, ok := c.consumerHandler(consumerID); ok {
		consumer.ConnectionClosed()
	} else {
		c.log.WithField("consumerID", consumerID).Warnf("Consumer with ID not found while closing consumer")
	}
}

func (c *connection) handleCloseProducer(closeProducer *pb.CommandCloseProducer) {
	c.log.Infof("Broker notification of Closed consumer: %d", closeProducer.GetProducerId())
	producerID := closeProducer.GetProducerId()
	if producer, ok := c.listeners[producerID]; ok {
		producer.ConnectionClosed()
	} else {
		c.log.WithField("producerID", producerID).Warn("Producer with ID not found while closing producer")
	}
}

func (c *connection) RegisterListener(id uint64, listener ConnectionListener) {
	c.Lock()
	defer c.Unlock()

	c.listeners[id] = listener
}

func (c *connection) UnregisterListener(id uint64) {
	c.Lock()
	defer c.Unlock()

	delete(c.listeners, id)
}

// Triggers the connection close by forcing the socket to close and
// broadcasting the notification on the close channel
func (c *connection) TriggerClose() {
	cnx := c.cnx
	if cnx != nil {
		cnx.Close()
	}

	select {
		case <- c.closeCh:
			return
	default:
		close(c.closeCh)
	}

}

func (c *connection) Close() {
	c.Lock()
	defer c.Unlock()

	c.cond.Broadcast()

	if c.state == connectionClosed {
		return
	}

	c.log.Info("Connection closed")
	c.state = connectionClosed
	if c.cnx != nil {
		c.cnx.Close()
	}
	c.pingTicker.Stop()
	c.pingCheckTicker.Stop()

	for _, listener := range c.listeners {
		listener.ConnectionClosed()
	}

	for _, req := range c.pendingReqs {
		req.callback(nil, errors.New("connection closed"))
	}

	consumerHandlers := make(map[uint64]ConsumerHandler)
	c.consumerHandlersLock.RLock()
	for id, handler := range c.consumerHandlers {
		consumerHandlers[id] = handler
	}
	c.consumerHandlersLock.RUnlock()

	for _, handler := range consumerHandlers {
		handler.ConnectionClosed()
	}
}

func (c *connection) changeState(state connectionState) {
	c.Lock()
	c.state = state
	c.cond.Broadcast()
	c.Unlock()
}

func (c *connection) newRequestID() uint64 {
	return atomic.AddUint64(&c.requestIDGenerator, 1)
}

func (c *connection) getTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: c.tlsOptions.AllowInsecureConnection,
	}

	if c.tlsOptions.TrustCertsFilePath != "" {
		caCerts, err := ioutil.ReadFile(c.tlsOptions.TrustCertsFilePath)
		if err != nil {
			return nil, err
		}

		tlsConfig.RootCAs = x509.NewCertPool()
		ok := tlsConfig.RootCAs.AppendCertsFromPEM(caCerts)
		if !ok {
			return nil, errors.New("failed to parse root CAs certificates")
		}
	}

	if c.tlsOptions.ValidateHostname {
		tlsConfig.ServerName = c.physicalAddr.Hostname()
	}

	cert, err := c.auth.GetTLSCertificate()
	if err != nil {
		return nil, err
	}

	if cert != nil {
		tlsConfig.Certificates = []tls.Certificate{*cert}
	}

	return tlsConfig, nil
}

func (c *connection) AddConsumeHandler(id uint64, handler ConsumerHandler) {
	c.consumerHandlersLock.Lock()
	defer c.consumerHandlersLock.Unlock()
	c.consumerHandlers[id] = handler
}

func (c *connection) DeleteConsumeHandler(id uint64) {
	c.consumerHandlersLock.Lock()
	defer c.consumerHandlersLock.Unlock()
	delete(c.consumerHandlers, id)
}

func (c *connection) consumerHandler(id uint64) (ConsumerHandler, bool) {
	c.consumerHandlersLock.RLock()
	defer c.consumerHandlersLock.RUnlock()
	h, ok := c.consumerHandlers[id]
	return h, ok
}

func (c *connection) ID() (string) {
	return fmt.Sprintf("%s -> %s", c.cnx.LocalAddr(), c.cnx.RemoteAddr())
}

