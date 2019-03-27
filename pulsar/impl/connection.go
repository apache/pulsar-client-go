package impl

import (
	"errors"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"net"
	pb "pulsar-client-go-native/pulsar/pulsar_proto"
	"sync"
	"sync/atomic"
)

type Connection interface {
	SendRequest(requestId uint64, req *pb.BaseCommand, callback func(command *pb.BaseCommand))
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

	writeBuffer Buffer
	reader      *connectionReader

	log *log.Entry

	requestIdGenerator uint64

	incomingRequests chan *request
	pendingReqs      map[uint64]*request
}

func newConnection(logicalAddr string, physicalAddr string) *connection {
	cnx := &connection{
		state:        connectionInit,
		logicalAddr:  logicalAddr,
		physicalAddr: physicalAddr,
		writeBuffer:  NewBuffer(4096),
		log:          log.WithField("raddr", physicalAddr),
		pendingReqs:  make(map[uint64]*request),
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
		c.internalClose()
		return false
	} else {
		c.log = c.log.WithField("laddr", c.cnx.LocalAddr())
		c.log.Info("TCP connection established")
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
		switch c.state {
		case connectionInit:
		case connectionConnecting:
		case connectionTcpConnected:
			// Wait for the state to change
			c.cond.Wait()
			break

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
			c.pendingReqs[req.id] = req
			c.writeCommand(req.cmd)
		}
	}
}

func (c *connection) writeCommand(cmd proto.Message) {
	// Wire format
	// [FRAME_SIZE] [CMD_SIZE][CMD]
	cmdSize := uint32(proto.Size(cmd))
	frameSize := cmdSize + 4
	bufferSize := frameSize + 4

	c.writeBuffer.Clear()
	if c.writeBuffer.WritableBytes() < bufferSize {
		c.writeBuffer.Resize(c.writeBuffer.Capacity() * 2)
	}

	c.writeBuffer.WriteUint32(frameSize)
	c.writeBuffer.WriteUint32(cmdSize)
	serialized, err := proto.Marshal(cmd)
	if err != nil {
		c.log.WithError(err).Fatal("Protobuf serialization error")
	}

	c.writeBuffer.Write(serialized)

	if _, err := c.cnx.Write(c.writeBuffer.ReadableSlice()); err != nil {
		c.log.WithError(err).Warn("Failed to write on connection")
		c.internalClose()
	}
}

func (c *connection) receivedCommand(cmd *pb.BaseCommand, headersAndPayload []byte) {
	c.log.Infof("Received command: %s -- payload: %v", cmd, headersAndPayload)

	switch *cmd.Type {
	case pb.BaseCommand_SUCCESS:
		c.handleResponse(*cmd.Success.RequestId, cmd)
		break

	case pb.BaseCommand_PRODUCER_SUCCESS:
		c.handleResponse(*cmd.ProducerSuccess.RequestId, cmd)
		break

	case pb.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		c.handleResponse(*cmd.PartitionMetadataResponse.RequestId, cmd)
		break

	case pb.BaseCommand_LOOKUP_RESPONSE:
		c.handleResponse(*cmd.LookupTopicResponse.RequestId, cmd)
		break

	case pb.BaseCommand_CONSUMER_STATS_RESPONSE:
		c.handleResponse(*cmd.ConsumerStatsResponse.RequestId, cmd)
		break

	case pb.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE:
		c.handleResponse(*cmd.GetLastMessageIdResponse.RequestId, cmd)
		break

	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE_RESPONSE:
		c.handleResponse(*cmd.GetTopicsOfNamespaceResponse.RequestId, cmd)
		break

	case pb.BaseCommand_GET_SCHEMA_RESPONSE:
		c.handleResponse(*cmd.GetSchemaResponse.RequestId, cmd)
		break

	case pb.BaseCommand_ERROR:
		break

	case pb.BaseCommand_CLOSE_PRODUCER:
	case pb.BaseCommand_CLOSE_CONSUMER:

	case pb.BaseCommand_SEND_RECEIPT:
		break
	case pb.BaseCommand_SEND_ERROR:
		break

	case pb.BaseCommand_MESSAGE:
		break

	case pb.BaseCommand_PONG:

	case pb.BaseCommand_ACTIVE_CONSUMER_CHANGE:
		// TODO
		break

	default:
		c.log.Errorf("Received invalid command type: %s", cmd.Type)
		c.Close()
	}
}

func (c *connection) SendRequest(requestId uint64, req *pb.BaseCommand, callback func(command *pb.BaseCommand)) {
	c.pendingReqs[requestId] = &request{
		id:       requestId,
		cmd:      req,
		callback: callback,
	}
	c.writeCommand(req)
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

func (c *connection) Close() {
	// TODO
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

func (c *connection) internalClose() {
	c.state = connectionClosed
	c.cond.Broadcast()

	if c.cnx != nil {
		c.cnx.Close()
	}
}
