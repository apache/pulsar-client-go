//
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
//

package internal

import (
	"errors"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"net"
	"net/url"
	pb "pulsar-client-go/pulsar/internal/pulsar_proto"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionListener is a user of a connection (eg. a producer or
// a consumer) that can register itself to get notified
// when the connection is closed.
type ConnectionListener interface {
	ReceivedSendReceipt(response *pb.CommandSendReceipt)

	ConnectionClosed()
}

type Connection interface {
	SendRequest(requestId uint64, req *pb.BaseCommand, callback func(command *pb.BaseCommand))
	WriteData(data []byte)
	RegisterListener(id uint64, listener ConnectionListener)
	UnregisterListener(id uint64)
	Close()
}

type connectionState int

const (
	connectionInit connectionState = iota
	connectionConnecting
	connectionTcpConnected
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

	logicalAddr  string
	physicalAddr string
	cnx          net.Conn

	writeBuffer          Buffer
	reader               *connectionReader
	lastDataReceivedTime time.Time
	pingTicker           *time.Ticker

	log *log.Entry

	requestIdGenerator uint64

	incomingRequests chan *request
	writeRequests    chan []byte
	pendingReqs      map[uint64]*request
	listeners        map[uint64]ConnectionListener
}

func newConnection(logicalAddr *url.URL, physicalAddr *url.URL) *connection {
	cnx := &connection{
		state:                connectionInit,
		logicalAddr:          logicalAddr.Host,
		physicalAddr:         physicalAddr.Host,
		writeBuffer:          NewBuffer(4096),
		log:                  log.WithField("raddr", physicalAddr),
		pendingReqs:          make(map[uint64]*request),
		lastDataReceivedTime: time.Now(),
		pingTicker:           time.NewTicker(keepAliveInterval),

		incomingRequests: make(chan *request),
		writeRequests:    make(chan []byte),
		listeners:        make(map[uint64]ConnectionListener),
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

	var err error
	c.cnx, err = net.Dial("tcp", c.physicalAddr)
	if err != nil {
		c.log.WithError(err).Warn("Failed to connect to broker.")
		c.Close()
		return false
	} else {
		c.log = c.log.WithField("laddr", c.cnx.LocalAddr())
		c.log.Debug("TCP connection established")
		c.state = connectionTcpConnected
		return true
	}
}

func (c *connection) doHandshake() (ok bool) {
	// Send 'Connect' command to initiate handshake
	version := int32(pb.ProtocolVersion_v13)
	c.writeCommand(baseCommand(pb.BaseCommand_CONNECT, &pb.CommandConnect{
		ProtocolVersion: &version,
		ClientVersion:   proto.String("Pulsar Go 0.1"),
		// AuthMethodName: "token",
		// AuthData: authData,
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
		case connectionTcpConnected:
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
	c.writeRequests <- data
}

func (c *connection) SendRequest(requestId uint64, req *pb.BaseCommand, callback func(command *pb.BaseCommand)) {
	c.incomingRequests <- &request{
		id:       requestId,
		cmd:      req,
		callback: callback,
	}
}

func (c *connection) internalSendRequest(req *request) {
	c.pendingReqs[req.id] = req
	c.writeCommand(req.cmd)
}

func (c *connection) handleResponse(requestId uint64, response *pb.BaseCommand) {
	request, ok := c.pendingReqs[requestId]
	if !ok {
		c.log.Warnf("Received unexpected response for request %d of type %s", requestId, response.Type)
		return
	}

	delete(c.pendingReqs, requestId)
	request.callback(response)
}

func (c *connection) handleSendReceipt(response *pb.CommandSendReceipt) {
	c.log.Debug("Got SEND_RECEIPT: ", response)
	producerId := response.GetProducerId()
	if producer, ok := c.listeners[producerId]; ok {
		producer.ReceivedSendReceipt(response)
	} else {
		c.log.WithField("producerId", producerId).Warn("Got unexpected send receipt for message: ", response.MessageId)
	}
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

func (c *connection) newRequestId() uint64 {
	return atomic.AddUint64(&c.requestIdGenerator, 1)
}
