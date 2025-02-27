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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/auth"

	"google.golang.org/protobuf/proto"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

const (
	PulsarProtocolVersion = int32(pb.ProtocolVersion_v20)
)

type TLSOptions struct {
	KeyFile                 string
	CertFile                string
	TrustCertsFilePath      string
	AllowInsecureConnection bool
	ValidateHostname        bool
	ServerName              string
	CipherSuites            []uint16
	MinVersion              uint16
	MaxVersion              uint16
	TLSConfig               *tls.Config
}

var (
	errConnectionClosed        = errors.New("connection closed")
	errUnableRegisterListener  = errors.New("unable register listener when con closed")
	errUnableAddConsumeHandler = errors.New("unable add consumer handler when con closed")
)

// ConnectionListener is a user of a connection (eg. a producer or
// a consumer) that can register itself to get notified
// when the connection is closed.
type ConnectionListener interface {
	// ReceivedSendReceipt receive and process the return value of the send command.
	ReceivedSendReceipt(response *pb.CommandSendReceipt)

	// ConnectionClosed close the TCP connection.
	ConnectionClosed(closeProducer *pb.CommandCloseProducer)

	// SetRedirectedClusterURI set the redirected cluster URI for lookups
	SetRedirectedClusterURI(redirectedClusterURI string)
}

// Connection is a interface of client cnx.
type Connection interface {
	SendRequest(requestID uint64, req *pb.BaseCommand, callback func(*pb.BaseCommand, error))
	SendRequestNoWait(req *pb.BaseCommand) error
	WriteData(ctx context.Context, data Buffer)
	RegisterListener(id uint64, listener ConnectionListener) error
	UnregisterListener(id uint64)
	AddConsumeHandler(id uint64, handler ConsumerHandler) error
	DeleteConsumeHandler(id uint64)
	ID() string
	GetMaxMessageSize() int32
	Close()
	WaitForClose() <-chan struct{}
	IsProxied() bool
}

type ConsumerHandler interface {
	MessageReceived(response *pb.CommandMessage, headersAndPayload Buffer) error

	ActiveConsumerChanged(isActive bool)

	// ConnectionClosed close the TCP connection.
	ConnectionClosed(closeConsumer *pb.CommandCloseConsumer)

	// SetRedirectedClusterURI set the redirected cluster URI for lookups
	SetRedirectedClusterURI(redirectedClusterURI string)
}

type connectionState int32

const (
	connectionInit = iota
	connectionReady
	connectionClosed
)

func (s connectionState) String() string {
	switch s {
	case connectionInit:
		return "Initializing"
	case connectionReady:
		return "Ready"
	case connectionClosed:
		return "Closed"
	default:
		return "Unknown"
	}
}

type request struct {
	id       *uint64
	cmd      *pb.BaseCommand
	callback func(command *pb.BaseCommand, err error)
}

type dataRequest struct {
	ctx  context.Context
	data Buffer
}

type connection struct {
	started           int32
	connectionTimeout time.Duration
	closeOnce         sync.Once

	// mu protects the fields below against concurrency accesses.
	mu               sync.RWMutex
	state            atomic.Int32
	cnx              net.Conn
	listeners        map[uint64]ConnectionListener
	consumerHandlers map[uint64]ConsumerHandler

	logicalAddr  *url.URL
	physicalAddr *url.URL

	writeBufferLock sync.Mutex
	writeBuffer     Buffer
	reader          *connectionReader

	lastDataReceivedLock sync.Mutex
	lastDataReceivedTime time.Time

	log log.Logger

	incomingRequestsWG sync.WaitGroup
	incomingRequestsCh chan *request
	closeCh            chan struct{}
	readyCh            chan struct{}
	writeRequestsCh    chan *dataRequest

	pendingLock sync.Mutex
	pendingReqs map[uint64]*request

	tlsOptions *TLSOptions
	auth       auth.Provider

	maxMessageSize int32
	metrics        *Metrics

	keepAliveInterval time.Duration

	lastActive  time.Time
	description string
}

// connectionOptions defines configurations for creating connection.
type connectionOptions struct {
	logicalAddr       *url.URL
	physicalAddr      *url.URL
	tls               *TLSOptions
	connectionTimeout time.Duration
	auth              auth.Provider
	logger            log.Logger
	metrics           *Metrics
	keepAliveInterval time.Duration
	description       string
}

func newConnection(opts connectionOptions) *connection {
	cnx := &connection{
		connectionTimeout:    opts.connectionTimeout,
		keepAliveInterval:    opts.keepAliveInterval,
		logicalAddr:          opts.logicalAddr,
		physicalAddr:         opts.physicalAddr,
		writeBuffer:          NewBuffer(4096),
		log:                  opts.logger.SubLogger(log.Fields{"remote_addr": opts.physicalAddr}),
		pendingReqs:          make(map[uint64]*request),
		lastDataReceivedTime: time.Now(),
		tlsOptions:           opts.tls,
		auth:                 opts.auth,

		closeCh:            make(chan struct{}),
		readyCh:            make(chan struct{}),
		incomingRequestsCh: make(chan *request, 10),

		// This channel is used to pass data from producers to the connection
		// go routine. It can become contended or blocking if we have multiple
		// partition produces writing on a single connection. In general it's
		// good to keep this above the number of partition producers assigned
		// to a single connection.
		writeRequestsCh:  make(chan *dataRequest, 256),
		listeners:        make(map[uint64]ConnectionListener),
		consumerHandlers: make(map[uint64]ConsumerHandler),
		metrics:          opts.metrics,
		description:      opts.description,
	}
	cnx.state.Store(int32(connectionInit))
	cnx.reader = newConnectionReader(cnx)
	return cnx
}

func (c *connection) start() {
	if !atomic.CompareAndSwapInt32(&c.started, 0, 1) {
		c.log.Warnf("connection has already started")
		return
	}

	// Each connection gets its own goroutine that will
	go func() {
		if c.connect() {
			if c.doHandshake() {
				c.metrics.ConnectionsOpened.Inc()
				c.run()
			} else {
				c.metrics.ConnectionsHandshakeErrors.Inc()
				c.Close()
			}
		} else {
			c.metrics.ConnectionsEstablishmentErrors.Inc()
			c.Close()
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
		if c.connectionTimeout.Nanoseconds() > 0 {
			cnx, err = net.DialTimeout("tcp", c.physicalAddr.Host, c.connectionTimeout)
		} else {
			cnx, err = net.Dial("tcp", c.physicalAddr.Host)
		}
	} else {
		// TLS connection
		tlsConfig, err = c.getTLSConfig()
		if err != nil {
			c.log.WithError(err).Warn("Failed to configure TLS ")
			return false
		}

		// time.Duration is initialized to 0 by default, net.Dialer's default timeout is no timeout
		// therefore if c.connectionTimeout is 0, it means no timeout
		d := &net.Dialer{Timeout: c.connectionTimeout}
		cnx, err = tls.DialWithDialer(d, "tcp", c.physicalAddr.Host, tlsConfig)
	}

	if err != nil {
		c.log.WithError(err).Warn("Failed to connect to broker.")
		c.Close()
		return false
	}

	c.mu.Lock()
	c.cnx = cnx
	c.log = c.log.SubLogger(log.Fields{"local_addr": c.cnx.LocalAddr()})
	c.log.Info("TCP connection established")
	c.mu.Unlock()

	return true
}

func (c *connection) doHandshake() bool {
	// Send 'Connect' command to initiate handshake
	authData, err := c.auth.GetData()
	if err != nil {
		c.log.WithError(err).Warn("Failed to load auth credentials")
		return false
	}

	// During the initial handshake, the internal keep alive is not
	// active yet, so we need to timeout write and read requests
	c.cnx.SetDeadline(time.Now().Add(c.keepAliveInterval))
	cmdConnect := &pb.CommandConnect{
		ProtocolVersion: proto.Int32(PulsarProtocolVersion),
		ClientVersion:   proto.String(c.getClientVersion()),
		AuthMethodName:  proto.String(c.auth.Name()),
		AuthData:        authData,
		FeatureFlags: &pb.FeatureFlags{
			SupportsAuthRefresh:         proto.Bool(true),
			SupportsBrokerEntryMetadata: proto.Bool(true),
		},
	}

	if c.IsProxied() {
		cmdConnect.ProxyToBrokerUrl = proto.String(c.logicalAddr.Host)
	}
	c.writeCommand(baseCommand(pb.BaseCommand_CONNECT, cmdConnect))
	cmd, _, err := c.reader.readSingleCommand()
	if err != nil {
		c.log.WithError(err).Warn("Failed to perform initial handshake")
		return false
	}

	// Reset the deadline so that we don't use read timeouts
	c.cnx.SetDeadline(time.Time{})

	if cmd.Connected == nil {
		c.log.Warnf("Failed to establish connection with broker: '%s'",
			cmd.Error.GetMessage())
		return false
	}
	if cmd.Connected.MaxMessageSize != nil && *cmd.Connected.MaxMessageSize > 0 {
		c.log.Debug("Got MaxMessageSize from handshake response:", *cmd.Connected.MaxMessageSize)
		c.maxMessageSize = *cmd.Connected.MaxMessageSize
	} else {
		c.log.Debug("No MaxMessageSize from handshake response, use default: ", MaxMessageSize)
		c.maxMessageSize = MaxMessageSize
	}
	c.log.Info("Connection is ready")
	c.setLastDataReceived(time.Now())
	c.setStateReady()
	close(c.readyCh) // broadcast the readiness of the connection.
	return true
}

func (c *connection) getClientVersion() string {
	var clientVersion string
	if c.description == "" {
		clientVersion = ClientVersionString
	} else {
		clientVersion = fmt.Sprintf("%s-%s", ClientVersionString, c.description)
	}
	return clientVersion
}

func (c *connection) IsProxied() bool {
	return c.logicalAddr.Host != c.physicalAddr.Host
}

func (c *connection) waitUntilReady() error {
	select {
	case <-c.readyCh:
		return nil
	case <-c.closeCh:
		// Connection has been closed while waiting for the readiness.
		return errors.New("connection error")
	}
}

func (c *connection) failLeftRequestsWhenClose() {
	// wait for outstanding incoming requests to complete before draining
	// and closing the channel
	c.incomingRequestsWG.Wait()

	ch := c.incomingRequestsCh
	go func() {
		// send a nil message to drain instead of
		// closing the channel and causing a potential panic
		//
		// if other requests come in after the nil message
		// then the RPC client will time out
		ch <- nil
	}()
	for req := range ch {
		if nil == req {
			break // we have drained the requests
		}
		c.internalSendRequest(req)
	}
}

func (c *connection) run() {
	pingSendTicker := time.NewTicker(c.keepAliveInterval)
	pingCheckTicker := time.NewTicker(c.keepAliveInterval)

	defer func() {
		// stop tickers
		pingSendTicker.Stop()
		pingCheckTicker.Stop()

		// all the accesses to the pendingReqs should be happened in this run loop thread,
		// including the final cleanup, to avoid the issue
		// https://github.com/apache/pulsar-client-go/issues/239
		c.failPendingRequests(errConnectionClosed)
		c.Close()
	}()

	// All reads come from the reader goroutine
	go c.reader.readFromConnection()
	go c.runPingCheck(pingCheckTicker)

	c.log.Debugf("Connection run starting with request capacity=%d queued=%d",
		cap(c.incomingRequestsCh), len(c.incomingRequestsCh))

	for {
		select {
		case <-c.closeCh:
			c.failLeftRequestsWhenClose()
			return
		case req := <-c.incomingRequestsCh:
			if req == nil {
				return // TODO: this never gonna be happen
			}
			c.internalSendRequest(req)
		case req := <-c.writeRequestsCh:
			if req == nil {
				return
			}
			c.internalWriteData(req.ctx, req.data)

		case <-pingSendTicker.C:
			c.sendPing()
		}
	}
}

func (c *connection) runPingCheck(pingCheckTicker *time.Ticker) {
	for {
		select {
		case <-c.closeCh:
			return
		case <-pingCheckTicker.C:
			if c.lastDataReceived().Add(2 * c.keepAliveInterval).Before(time.Now()) {
				// We have not received a response to the previous Ping request, the
				// connection to broker is stale
				c.log.Warn("Detected stale connection to broker")
				c.Close()
				return
			}
		}
	}
}

func (c *connection) WriteData(ctx context.Context, data Buffer) {
	select {
	case c.writeRequestsCh <- &dataRequest{ctx: ctx, data: data}:
		// Channel is not full
		return
	case <-ctx.Done():
		c.log.Debug("Write data context cancelled")
		return
	default:
		// Channel full, fallback to probe if connection is closed
	}

	for {
		select {
		case c.writeRequestsCh <- &dataRequest{ctx: ctx, data: data}:
			// Successfully wrote on the channel
			return
		case <-ctx.Done():
			c.log.Debug("Write data context cancelled")
			return
		case <-time.After(100 * time.Millisecond):
			// The channel is either:
			// 1. blocked, in which case we need to wait until we have space
			// 2. the connection is already closed, then we need to bail out
			c.log.Debug("Couldn't write on connection channel immediately")

			if c.getState() != connectionReady {
				c.log.Debug("Connection was already closed")
				return
			}
		}
	}

}

func (c *connection) internalWriteData(ctx context.Context, data Buffer) {
	c.log.Debug("Write data: ", data.ReadableBytes())
	if ctx == nil {
		if _, err := c.cnx.Write(data.ReadableSlice()); err != nil {
			c.log.WithError(err).Warn("Failed to write on connection")
			c.Close()
		}

		return
	}

	select {
	case <-ctx.Done():
		return
	default:
		if _, err := c.cnx.Write(data.ReadableSlice()); err != nil {
			c.log.WithError(err).Warn("Failed to write on connection")
			c.Close()
		}
	}
}

func (c *connection) writeCommand(cmd *pb.BaseCommand) {
	// Wire format
	// [FRAME_SIZE] [CMD_SIZE][CMD]
	cmdSize := uint32(proto.Size(cmd))
	frameSize := cmdSize + 4

	c.writeBufferLock.Lock()
	defer c.writeBufferLock.Unlock()

	c.writeBuffer.Clear()
	c.writeBuffer.WriteUint32(frameSize)

	c.writeBuffer.WriteUint32(cmdSize)
	c.writeBuffer.ResizeIfNeeded(cmdSize)
	err := MarshalToSizedBuffer(cmd, c.writeBuffer.WritableSlice()[:cmdSize])
	if err != nil {
		c.log.WithError(err).Error("Protobuf serialization error")
		panic("Protobuf serialization error")
	}

	c.writeBuffer.WrittenBytes(cmdSize)
	c.internalWriteData(context.Background(), c.writeBuffer)
}

func (c *connection) receivedCommand(cmd *pb.BaseCommand, headersAndPayload Buffer) {
	c.log.Debugf("Received command: %s -- payload: %v", cmd, headersAndPayload)
	c.setLastDataReceived(time.Now())

	switch *cmd.Type {
	case pb.BaseCommand_SUCCESS:
		c.handleResponse(cmd.Success.GetRequestId(), cmd)

	case pb.BaseCommand_PRODUCER_SUCCESS:
		if !cmd.ProducerSuccess.GetProducerReady() {
			request, ok := c.findPendingRequest(cmd.ProducerSuccess.GetRequestId())
			if ok {
				request.callback(cmd, nil)
			}
		} else {
			c.handleResponse(cmd.ProducerSuccess.GetRequestId(), cmd)
		}
	case pb.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		c.checkServerError(cmd.PartitionMetadataResponse.Error)
		c.handleResponse(cmd.PartitionMetadataResponse.GetRequestId(), cmd)

	case pb.BaseCommand_LOOKUP_RESPONSE:
		lookupResult := cmd.LookupTopicResponse
		c.checkServerError(lookupResult.Error)
		c.handleResponse(lookupResult.GetRequestId(), cmd)

	case pb.BaseCommand_CONSUMER_STATS_RESPONSE:
		c.handleResponse(cmd.ConsumerStatsResponse.GetRequestId(), cmd)

	case pb.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE:
		c.handleResponse(cmd.GetLastMessageIdResponse.GetRequestId(), cmd)

	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE_RESPONSE:
		c.handleResponse(cmd.GetTopicsOfNamespaceResponse.GetRequestId(), cmd)

	case pb.BaseCommand_GET_SCHEMA_RESPONSE:
		c.handleResponse(cmd.GetSchemaResponse.GetRequestId(), cmd)

	case pb.BaseCommand_GET_OR_CREATE_SCHEMA_RESPONSE:
		c.handleResponse(cmd.GetOrCreateSchemaResponse.GetRequestId(), cmd)

	case pb.BaseCommand_ERROR:
		c.handleResponseError(cmd.GetError())

	case pb.BaseCommand_SEND_ERROR:
		c.handleSendError(cmd.GetSendError())

	case pb.BaseCommand_CLOSE_PRODUCER:
		c.handleCloseProducer(cmd.GetCloseProducer())

	case pb.BaseCommand_CLOSE_CONSUMER:
		c.handleCloseConsumer(cmd.GetCloseConsumer())

	case pb.BaseCommand_TOPIC_MIGRATED:
		c.handleTopicMigrated(cmd.GetTopicMigrated())

	case pb.BaseCommand_AUTH_CHALLENGE:
		c.handleAuthChallenge(cmd.GetAuthChallenge())

	case pb.BaseCommand_SEND_RECEIPT:
		c.handleSendReceipt(cmd.GetSendReceipt())

	case pb.BaseCommand_MESSAGE:
		c.handleMessage(cmd.GetMessage(), headersAndPayload)

	case pb.BaseCommand_ACK_RESPONSE:
		c.handleAckResponse(cmd.GetAckResponse())

	case pb.BaseCommand_PING:
		c.handlePing()
	case pb.BaseCommand_PONG:
		c.handlePong()
	case pb.BaseCommand_TC_CLIENT_CONNECT_RESPONSE:
		c.handleResponse(cmd.TcClientConnectResponse.GetRequestId(), cmd)
	case pb.BaseCommand_NEW_TXN_RESPONSE:
		c.handleResponse(cmd.NewTxnResponse.GetRequestId(), cmd)
	case pb.BaseCommand_ADD_PARTITION_TO_TXN_RESPONSE:
		c.handleResponse(cmd.AddPartitionToTxnResponse.GetRequestId(), cmd)
	case pb.BaseCommand_ADD_SUBSCRIPTION_TO_TXN_RESPONSE:
		c.handleResponse(cmd.AddSubscriptionToTxnResponse.GetRequestId(), cmd)
	case pb.BaseCommand_END_TXN_RESPONSE:
		c.handleResponse(cmd.EndTxnResponse.GetRequestId(), cmd)
	case pb.BaseCommand_ACTIVE_CONSUMER_CHANGE:
		c.handleActiveConsumerChange(cmd.GetActiveConsumerChange())

	default:
		c.log.Errorf("Received invalid command type: %s", cmd.Type)
		c.Close()
	}
}

func (c *connection) checkServerError(err *pb.ServerError) {
	if err == nil {
		return
	}

	if *err == pb.ServerError_ServiceNotReady {
		c.Close()
	}
}

func (c *connection) SendRequest(requestID uint64, req *pb.BaseCommand,
	callback func(command *pb.BaseCommand, err error)) {
	c.incomingRequestsWG.Add(1)
	defer c.incomingRequestsWG.Done()

	if c.getState() == connectionClosed {
		callback(req, ErrConnectionClosed)

	} else {
		select {
		case <-c.closeCh:
			callback(req, ErrConnectionClosed)

		case c.incomingRequestsCh <- &request{
			id:       &requestID,
			cmd:      req,
			callback: callback,
		}:
		}
	}
}

func (c *connection) SendRequestNoWait(req *pb.BaseCommand) error {
	c.incomingRequestsWG.Add(1)
	defer c.incomingRequestsWG.Done()

	if c.getState() == connectionClosed {
		return ErrConnectionClosed
	}

	select {
	case <-c.closeCh:
		return ErrConnectionClosed

	case c.incomingRequestsCh <- &request{
		id:       nil,
		cmd:      req,
		callback: nil,
	}:
		return nil
	}
}

func (c *connection) internalSendRequest(req *request) {
	if c.closed() {
		c.log.Warnf("internalSendRequest failed for connectionClosed")
		if req.callback != nil {
			req.callback(req.cmd, ErrConnectionClosed)
		}
	} else {
		c.pendingLock.Lock()
		if req.id != nil {
			c.pendingReqs[*req.id] = req
		}
		c.pendingLock.Unlock()
		c.writeCommand(req.cmd)
	}
}

func (c *connection) handleResponse(requestID uint64, response *pb.BaseCommand) {
	request, ok := c.deletePendingRequest(requestID)
	if !ok {
		c.log.Warnf("Received unexpected response for request %d of type %s", requestID, response.Type)
		return
	}

	request.callback(response, nil)
}

func (c *connection) handleResponseError(serverError *pb.CommandError) {
	requestID := serverError.GetRequestId()

	request, ok := c.deletePendingRequest(requestID)
	if !ok {
		c.log.Warnf("Received unexpected error response for request %d of type %s",
			requestID, serverError.GetError())
		return
	}

	errMsg := fmt.Sprintf("server error: %s: %s", serverError.GetError(), serverError.GetMessage())
	request.callback(nil, errors.New(errMsg))
}

func (c *connection) handleAckResponse(ackResponse *pb.CommandAckResponse) {
	requestID := ackResponse.GetRequestId()
	consumerID := ackResponse.GetConsumerId()

	request, ok := c.deletePendingRequest(requestID)
	if !ok {
		c.log.Warnf("AckResponse has complete when receive response! requestId : %d, consumerId : %d",
			requestID, consumerID)
		return
	}

	if ackResponse.GetMessage() == "" {
		request.callback(nil, nil)
		return
	}

	errMsg := fmt.Sprintf("ack response error: %s: %s", ackResponse.GetError(), ackResponse.GetMessage())
	request.callback(nil, errors.New(errMsg))
}

func (c *connection) handleSendReceipt(response *pb.CommandSendReceipt) {
	producerID := response.GetProducerId()

	c.mu.RLock()
	producer, ok := c.listeners[producerID]
	c.mu.RUnlock()

	if ok {
		producer.ReceivedSendReceipt(response)
	} else {
		c.log.
			WithField("producerID", producerID).
			Warn("Got unexpected send receipt for messageID=%+v", response.MessageId)
	}
}

func (c *connection) handleMessage(response *pb.CommandMessage, payload Buffer) {
	c.log.Debug("Got Message: ", response)
	consumerID := response.GetConsumerId()
	if consumer, ok := c.consumerHandler(consumerID); ok {
		err := consumer.MessageReceived(response, payload)
		if err != nil {
			c.log.
				WithError(err).
				WithField("consumerID", consumerID).
				Error("handle message Id: ", response.MessageId)
		}
	} else {
		c.log.WithField("consumerID", consumerID).Warn("Got unexpected message: ", response.MessageId)
	}
}

func (c *connection) deletePendingRequest(requestID uint64) (*request, bool) {
	c.pendingLock.Lock()
	defer c.pendingLock.Unlock()
	request, ok := c.pendingReqs[requestID]
	if ok {
		delete(c.pendingReqs, requestID)
	}
	return request, ok
}

func (c *connection) findPendingRequest(requestID uint64) (*request, bool) {
	c.pendingLock.Lock()
	defer c.pendingLock.Unlock()
	request, ok := c.pendingReqs[requestID]
	return request, ok
}

func (c *connection) failPendingRequests(err error) bool {
	c.pendingLock.Lock()
	defer c.pendingLock.Unlock()
	for id, req := range c.pendingReqs {
		req.callback(nil, err)
		delete(c.pendingReqs, id)
	}
	return true
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

func (c *connection) handleAuthChallenge(authChallenge *pb.CommandAuthChallenge) {
	c.log.Debugf("Received auth challenge from broker: %s", authChallenge.GetChallenge().GetAuthMethodName())

	// Get new credentials from the provider
	authData, err := c.auth.GetData()
	if err != nil {
		c.log.WithError(err).Warn("Failed to load auth credentials")
		c.Close()
		return
	}

	// Brokers expect authData to be not nil
	if authData == nil {
		authData = []byte{}
	}

	cmdAuthResponse := &pb.CommandAuthResponse{
		ProtocolVersion: proto.Int32(PulsarProtocolVersion),
		ClientVersion:   proto.String(c.getClientVersion()),
		Response: &pb.AuthData{
			AuthMethodName: proto.String(c.auth.Name()),
			AuthData:       authData,
		},
	}

	c.writeCommand(baseCommand(pb.BaseCommand_AUTH_RESPONSE, cmdAuthResponse))
}

func (c *connection) handleSendError(sendError *pb.CommandSendError) {
	c.log.Warnf("Received send error from server: [%v] : [%s]", sendError.GetError(), sendError.GetMessage())

	producerID := sendError.GetProducerId()

	switch sendError.GetError() {
	case pb.ServerError_NotAllowedError:
		_, ok := c.deletePendingProducers(producerID)
		if !ok {
			c.log.Warnf("Received unexpected error response for request %d of type %s",
				producerID, sendError.GetError())
			return
		}

		c.log.Warnf("server error: %s: %s", sendError.GetError(), sendError.GetMessage())
	case pb.ServerError_TopicTerminatedError:
		_, ok := c.deletePendingProducers(producerID)
		if !ok {
			c.log.Warnf("Received unexpected error response for producer %d of type %s",
				producerID, sendError.GetError())
			return
		}
		c.log.Warnf("server error: %s: %s", sendError.GetError(), sendError.GetMessage())
	default:
		// By default, for transient error, let the reconnection logic
		// to take place and re-establish the produce again
		c.Close()
	}
}

func (c *connection) deletePendingProducers(producerID uint64) (ConnectionListener, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	producer, ok := c.listeners[producerID]
	if ok {
		delete(c.listeners, producerID)
	}

	return producer, ok
}

func (c *connection) handleCloseConsumer(closeConsumer *pb.CommandCloseConsumer) {
	consumerID := closeConsumer.GetConsumerId()
	c.log.Infof("Broker notification of Closed consumer: %d", consumerID)

	if consumer, ok := c.consumerHandler(consumerID); ok {
		consumer.ConnectionClosed(closeConsumer)
		c.DeleteConsumeHandler(consumerID)
	} else {
		c.log.WithField("consumerID", consumerID).Warnf("Consumer with ID not found while closing consumer")
	}
}

func (c *connection) handleActiveConsumerChange(consumerChange *pb.CommandActiveConsumerChange) {
	consumerID := consumerChange.GetConsumerId()
	isActive := consumerChange.GetIsActive()
	if consumer, ok := c.consumerHandler(consumerID); ok {
		consumer.ActiveConsumerChanged(isActive)
	} else {
		c.log.WithField("consumerID", consumerID).Warnf("Consumer not found while active consumer change")
	}
}

func (c *connection) handleCloseProducer(closeProducer *pb.CommandCloseProducer) {
	c.log.Infof("Broker notification of Closed producer: %d", closeProducer.GetProducerId())
	producerID := closeProducer.GetProducerId()

	producer, ok := c.deletePendingProducers(producerID)
	// did we find a producer?
	if ok {
		producer.ConnectionClosed(closeProducer)
	} else {
		c.log.WithField("producerID", producerID).Warn("Producer with ID not found while closing producer")
	}
}

func (c *connection) getMigratedBrokerServiceURL(commandTopicMigrated *pb.CommandTopicMigrated) string {
	if c.tlsOptions == nil {
		if commandTopicMigrated.GetBrokerServiceUrl() != "" {
			return commandTopicMigrated.GetBrokerServiceUrl()
		}
	} else if commandTopicMigrated.GetBrokerServiceUrlTls() != "" {
		return commandTopicMigrated.GetBrokerServiceUrlTls()
	}
	return ""
}

func (c *connection) handleTopicMigrated(commandTopicMigrated *pb.CommandTopicMigrated) {
	resourceID := commandTopicMigrated.GetResourceId()
	migratedBrokerServiceURL := c.getMigratedBrokerServiceURL(commandTopicMigrated)
	if migratedBrokerServiceURL == "" {
		c.log.Warnf("Failed to find the migrated broker url for resource: %d, migratedBrokerUrl: %s, migratedBrokerUrlTls:%s",
			resourceID,
			commandTopicMigrated.GetBrokerServiceUrl(),
			commandTopicMigrated.GetBrokerServiceUrlTls())
		return
	}
	if commandTopicMigrated.GetResourceType() == pb.CommandTopicMigrated_Producer {
		c.mu.RLock()
		producer, ok := c.listeners[resourceID]
		c.mu.RUnlock()
		if ok {
			producer.SetRedirectedClusterURI(migratedBrokerServiceURL)
			c.log.Infof("producerID:{%d} migrated to RedirectedClusterURI:{%s}",
				resourceID, migratedBrokerServiceURL)
		} else {
			c.log.WithField("producerID", resourceID).Warn("Failed to SetRedirectedClusterURI")
		}
	} else {
		consumer, ok := c.consumerHandler(resourceID)
		if ok {
			consumer.SetRedirectedClusterURI(migratedBrokerServiceURL)
			c.log.Infof("consumerID:{%d} migrated to RedirectedClusterURI:{%s}",
				resourceID, migratedBrokerServiceURL)
		} else {
			c.log.WithField("consumerID", resourceID).Warn("Failed to SetRedirectedClusterURI")
		}
	}

}

func (c *connection) RegisterListener(id uint64, listener ConnectionListener) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.getState() == connectionClosed {
		c.log.Warnf("Connection closed unable register listener id=%+v", id)
		return errUnableRegisterListener
	}

	c.listeners[id] = listener
	return nil
}

func (c *connection) UnregisterListener(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.listeners, id)
}

func (c *connection) ResetLastActive() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastActive = time.Now()
}

func (c *connection) isIdle() bool {
	return len(c.listeners) == 0 &&
		len(c.consumerHandlers) == 0 &&
		len(c.incomingRequestsCh) == 0 &&
		len(c.writeRequestsCh) == 0
}

func (c *connection) CheckIdle(maxIdleTime time.Duration) bool {
	c.pendingLock.Lock()
	sizePendingReqs := len(c.pendingReqs)
	c.pendingLock.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if sizePendingReqs != 0 || !c.isIdle() {
		c.lastActive = time.Now()
	}

	return time.Since(c.lastActive) > maxIdleTime
}

func (c *connection) WaitForClose() <-chan struct{} {
	return c.closeCh
}

// Close closes the connection by
// closing underlying socket connection and closeCh.
// This also triggers callbacks to the ConnectionClosed listeners.
func (c *connection) Close() {
	c.closeOnce.Do(func() {
		listeners, consumerHandlers, cnx := c.closeAndEmptyObservers()

		if cnx != nil {
			_ = cnx.Close()
		}

		close(c.closeCh)

		// notify producers connection closed
		for _, listener := range listeners {
			listener.ConnectionClosed(nil)
		}

		// notify consumers connection closed
		for _, handler := range consumerHandlers {
			handler.ConnectionClosed(nil)
		}

		c.metrics.ConnectionsClosed.Inc()
	})
}

func (c *connection) closeAndEmptyObservers() ([]ConnectionListener, []ConsumerHandler, net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.setStateClosed()

	listeners := make([]ConnectionListener, 0, len(c.listeners))
	for _, listener := range c.listeners {
		listeners = append(listeners, listener)
	}

	handlers := make([]ConsumerHandler, 0, len(c.consumerHandlers))
	for _, handler := range c.consumerHandlers {
		handlers = append(handlers, handler)
	}

	return listeners, handlers, c.cnx
}

func (c *connection) getState() connectionState {
	return connectionState(c.state.Load())
}

func (c *connection) setStateReady() {
	c.state.CompareAndSwap(int32(connectionInit), int32(connectionReady))
}

func (c *connection) setStateClosed() {
	c.state.Store(int32(connectionClosed))
}

func (c *connection) closed() bool {
	return connectionClosed == c.getState()
}

func (c *connection) getTLSConfig() (*tls.Config, error) {
	if c.tlsOptions.TLSConfig != nil {
		return c.tlsOptions.TLSConfig, nil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: c.tlsOptions.AllowInsecureConnection,
		CipherSuites:       c.tlsOptions.CipherSuites,
		MinVersion:         c.tlsOptions.MinVersion,
		MaxVersion:         c.tlsOptions.MaxVersion,
	}

	if c.tlsOptions.TrustCertsFilePath != "" {
		caCerts, err := os.ReadFile(c.tlsOptions.TrustCertsFilePath)
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
		if c.tlsOptions.ServerName != "" {
			tlsConfig.ServerName = c.tlsOptions.ServerName
		} else {
			tlsConfig.ServerName = c.physicalAddr.Hostname()
		}
		c.log.Debugf("getTLSConfig(): setting tlsConfig.ServerName = %+v", tlsConfig.ServerName)
	}

	if c.tlsOptions.CertFile != "" && c.tlsOptions.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.tlsOptions.CertFile, c.tlsOptions.KeyFile)
		if err != nil {
			return nil, errors.New(err.Error())
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
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

func (c *connection) AddConsumeHandler(id uint64, handler ConsumerHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.getState() == connectionClosed {
		c.log.Warnf("Closed connection unable add consumer with id=%+v", id)
		return errUnableAddConsumeHandler
	}

	c.consumerHandlers[id] = handler
	return nil
}

func (c *connection) DeleteConsumeHandler(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.consumerHandlers, id)
}

func (c *connection) consumerHandler(id uint64) (ConsumerHandler, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	h, ok := c.consumerHandlers[id]
	return h, ok
}

func (c *connection) ID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return fmt.Sprintf("%s -> %s", c.cnx.LocalAddr(), c.cnx.RemoteAddr())
}

func (c *connection) GetMaxMessageSize() int32 {
	return c.maxMessageSize
}
