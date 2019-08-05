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
	"io/ioutil"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/apache/pulsar-client-go/pkg/auth"
	"github.com/apache/pulsar-client-go/pkg/pb"
	log "github.com/sirupsen/logrus"
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
	SendRequest(requestID uint64, req *pb.BaseCommand, callback func(command *pb.BaseCommand))
	WriteData(data []byte)
	RegisterListener(id uint64, listener ConnectionListener)
	UnregisterListener(id uint64)
	AddConsumeHandler(id uint64, handler ConsumerHandler)
	DeleteConsumeHandler(id uint64)
	Close()
}

type ConsumerHandler interface {
    HandlerMessage(response *pb.CommandMessage, headersAndPayload []byte) error
}

type connectionState int

const (
	connectionInit connectionState = iota
	connectionConnecting
	connectionTCPConnected
	connectionReady
	connectionClosed
)

const keepAliveInterval = 30 * time.Second

type request struct {
	id       uint64
	cmd      *pb.BaseCommand
	callback func(command *pb.BaseCommand)
}

type connection struct {
	sync.Mutex
	cond  *sync.Cond
	state connectionState

	logicalAddr  *url.URL
	physicalAddr *url.URL
	cnx          net.Conn

	writeBuffer          Buffer
	reader               *connectionReader
	lastDataReceivedTime time.Time
	pingTicker           *time.Ticker

	log *log.Entry

	requestIDGenerator uint64

	incomingRequests chan *request
	writeRequests    chan []byte
	pendingReqs      map[uint64]*request
	listeners        map[uint64]ConnectionListener
	connWrapper      *ConnWrapper

	tlsOptions *TLSOptions
	auth       auth.Provider
}

func newConnection(logicalAddr *url.URL, physicalAddr *url.URL, tlsOptions *TLSOptions, auth auth.Provider) *connection {
	cnx := &connection{
		state:                connectionInit,
		logicalAddr:          logicalAddr,
		physicalAddr:         physicalAddr,
		writeBuffer:          NewBuffer(4096),
		log:                  log.WithField("raddr", physicalAddr),
		pendingReqs:          make(map[uint64]*request),
		lastDataReceivedTime: time.Now(),
		pingTicker:           time.NewTicker(keepAliveInterval),
		tlsOptions:           tlsOptions,
		auth:                 auth,

		incomingRequests: make(chan *request),
		writeRequests:    make(chan []byte),
		listeners:        make(map[uint64]ConnectionListener),
        connWrapper:      NewConnWrapper(),
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

func (c *connection) connect() (ok bool) {
	c.log.Info("Connecting to broker")

	var (
		err       error
		cnx       net.Conn
		tlsConfig *tls.Config
	)

	if c.tlsOptions == nil {
		// Clear text connection
		cnx, err = net.Dial("tcp", c.physicalAddr.Host)
	} else {
		// TLS connection
		tlsConfig, err = c.getTLSConfig()
		if err != nil {
			c.log.WithError(err).Warn("Failed to configure TLS ")
			return false
		}

		cnx, err = tls.Dial("tcp", c.physicalAddr.Host, tlsConfig)
	}

	if err != nil {
		c.log.WithError(err).Warn("Failed to connect to broker.")
		c.Close()
		return false
	}
	c.cnx = cnx
	c.log = c.log.WithField("laddr", c.cnx.LocalAddr())
	c.log.Debug("TCP connection established")
	c.state = connectionTCPConnected

	return true
}

func (c *connection) doHandshake() (ok bool) {
	// Send 'Connect' command to initiate handshake
	version := int32(pb.ProtocolVersion_v13)

	authData, err := c.auth.GetData()
	if err != nil {
		c.log.WithError(err).Warn("Failed to load auth credentials")
		return false
	}

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

	for {
		c.log.Debug("Wait until connection is ready. State: ", c.state)
		switch c.state {
		case connectionInit:
			fallthrough
		case connectionConnecting:
			fallthrough
		case connectionTCPConnected:
			// Wait for the state to change
			c.cond.Wait()

		case connectionReady:
			return nil

		case connectionClosed:
			return errors.New("connection error")
		}
	}
}

func (c *connection) run() {
	// All reads come from the reader goroutine
	go c.reader.readFromConnection()

	for {
		select {
		case req := <-c.incomingRequests:
			if req == nil {
				return
			}
			c.pendingReqs[req.id] = req
			c.writeCommand(req.cmd)

		case data := <-c.writeRequests:
			if data == nil {
				return
			}
			c.internalWriteData(data)

		case _ = <-c.pingTicker.C:
			c.sendPing()
		}
	}
}

func (c *connection) WriteData(data []byte) {
	c.writeRequests <- data
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

	c.writeBuffer.Clear()
	c.writeBuffer.WriteUint32(frameSize)
	c.writeBuffer.WriteUint32(cmdSize)
	serialized, err := proto.Marshal(cmd)
	if err != nil {
		c.log.WithError(err).Fatal("Protobuf serialization error")
	}

	c.writeBuffer.Write(serialized)
	c.internalWriteData(c.writeBuffer.ReadableSlice())
}

func (c *connection) receivedCommand(cmd *pb.BaseCommand, headersAndPayload []byte) {
	c.log.Debugf("Received command: %s -- payload: %v", cmd, headersAndPayload)
	c.lastDataReceivedTime = time.Now()
    var err error

	switch *cmd.Type {
	case pb.BaseCommand_SUCCESS:
		c.handleResponse(*cmd.Success.RequestId, cmd)

	case pb.BaseCommand_PRODUCER_SUCCESS:
		c.handleResponse(*cmd.ProducerSuccess.RequestId, cmd)

	case pb.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		c.handleResponse(*cmd.PartitionMetadataResponse.RequestId, cmd)

	case pb.BaseCommand_LOOKUP_RESPONSE:
		c.handleResponse(*cmd.LookupTopicResponse.RequestId, cmd)

	case pb.BaseCommand_CONSUMER_STATS_RESPONSE:
		c.handleResponse(*cmd.ConsumerStatsResponse.RequestId, cmd)

	case pb.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE:
		c.handleResponse(*cmd.GetLastMessageIdResponse.RequestId, cmd)

	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE_RESPONSE:
		c.handleResponse(*cmd.GetTopicsOfNamespaceResponse.RequestId, cmd)

	case pb.BaseCommand_GET_SCHEMA_RESPONSE:
		c.handleResponse(*cmd.GetSchemaResponse.RequestId, cmd)

	case pb.BaseCommand_ERROR:
	case pb.BaseCommand_CLOSE_PRODUCER:
	case pb.BaseCommand_CLOSE_CONSUMER:

	case pb.BaseCommand_SEND_RECEIPT:
		c.handleSendReceipt(cmd.GetSendReceipt())

	case pb.BaseCommand_SEND_ERROR:

	case pb.BaseCommand_MESSAGE:
        err = c.handleMessage(cmd.GetMessage(), headersAndPayload)
	case pb.BaseCommand_PING:
		c.handlePing()
	case pb.BaseCommand_PONG:
		c.handlePong()

	case pb.BaseCommand_ACTIVE_CONSUMER_CHANGE:

	default:
        if err != nil {
            c.log.Errorf("Received invalid command type: %s", cmd.Type)
        }
		c.Close()
	}
}

func (c *connection) Write(data []byte) {
	c.writeRequests <- data
}

func (c *connection) SendRequest(requestID uint64, req *pb.BaseCommand, callback func(command *pb.BaseCommand)) {
	c.incomingRequests <- &request{
		id:       requestID,
		cmd:      req,
		callback: callback,
	}
}

func (c *connection) internalSendRequest(req *request) {
	c.pendingReqs[req.id] = req
	c.writeCommand(req.cmd)
}

func (c *connection) handleResponse(requestID uint64, response *pb.BaseCommand) {
	request, ok := c.pendingReqs[requestID]
	if !ok {
		c.log.Warnf("Received unexpected response for request %d of type %s", requestID, response.Type)
		return
	}

	delete(c.pendingReqs, requestID)
	request.callback(response)
}

func (c *connection) handleSendReceipt(response *pb.CommandSendReceipt) {
	c.log.Debug("Got SEND_RECEIPT: ", response)
	producerID := response.GetProducerId()
	if producer, ok := c.listeners[producerID]; ok {
		producer.ReceivedSendReceipt(response)
	} else {
		c.log.WithField("producerId", producerID).Warn("Got unexpected send receipt for message: ", response.MessageId)
	}
}

func (c *connection) handleMessage(response *pb.CommandMessage, payload []byte) error {
    c.log.Debug("Got Message: ", response)
    consumerId := response.GetConsumerId()
    if consumer, ok := c.connWrapper.Consumers[consumerId]; ok {
        err := consumer.HandlerMessage(response, payload)
        if err != nil {
            c.log.WithField("consumerId", consumerId).Error("handle message err: ", response.MessageId)
            return errors.New("handler not found")
        }
    } else {
        c.log.WithField("consumerId", consumerId).Warn("Got unexpected message: ", response.MessageId)
    }
    return nil
}

func (c *connection) sendPing() {
	if c.lastDataReceivedTime.Add(2 * keepAliveInterval).Before(time.Now()) {
		// We have not received a response to the previous Ping request, the
		// connection to broker is stale
		c.log.Info("Detected stale connection to broker")
		c.Close()
		return
	}

	c.log.Debug("Sending PING")
	c.writeCommand(baseCommand(pb.BaseCommand_PING, &pb.CommandPing{}))
}

func (c *connection) handlePong() {
	c.writeCommand(baseCommand(pb.BaseCommand_PONG, &pb.CommandPong{}))
}

func (c *connection) handlePing() {
	c.writeCommand(baseCommand(pb.BaseCommand_PONG, &pb.CommandPong{}))
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
	close(c.incomingRequests)
	close(c.writeRequests)

	for _, listener := range c.listeners {
		listener.ConnectionClosed()
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

type ConnWrapper struct {
	Rwmu             sync.RWMutex
	Consumers        map[uint64]ConsumerHandler
}

func NewConnWrapper() *ConnWrapper {
	return &ConnWrapper{
		Consumers: make(map[uint64]ConsumerHandler),
	}
}

func (c *connection) AddConsumeHandler(id uint64, handler ConsumerHandler) {
    c.connWrapper.Rwmu.Lock()
    c.connWrapper.Consumers[id] = handler
    c.connWrapper.Rwmu.Unlock()
}

func (c *connection) DeleteConsumeHandler(id uint64) {
    c.connWrapper.Rwmu.Lock()
    delete(c.connWrapper.Consumers, id)
    c.connWrapper.Rwmu.Unlock()
}
