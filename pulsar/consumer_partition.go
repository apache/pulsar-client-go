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

package pulsar

import (
	"fmt"
	"math"
	"time"

	"github.com/golang/protobuf/proto"

	log "github.com/sirupsen/logrus"

	"github.com/apache/pulsar-client-go/pkg/pb"
	"github.com/apache/pulsar-client-go/pulsar/internal"
)

type consumerState int

const (
	consumerInit consumerState = iota
	consumerReady
	consumerClosing
	consumerClosed
)

type partitionConsumerOpts struct {
	topic               string
	consumerName        string
	subscription        string
	subscriptionType    SubscriptionType
	subscriptionInitPos SubscriptionInitialPosition
	partitionIdx        int
	receiverQueueSize   int
	nackRedeliveryDelay time.Duration
}

type partitionConsumer struct {
	client *client

	// this is needed for sending ConsumerMessage on the messageCh
	parentConsumer Consumer
	state          consumerState
	options        *partitionConsumerOpts

	conn internal.Connection

	topic        string
	name         string
	consumerID   uint64
	partitionIdx int

	// shared channel
	messageCh chan ConsumerMessage

	// the number of message slots available
	availablePermits int32

	// the size of the queue channel for buffering messages
	queueSize int32
	queueCh   chan []*message

	eventsCh    chan interface{}
	connectedCh chan struct{}
	closeCh     chan struct{}

	nackTracker *negativeAcksTracker

	log *log.Entry
}

func newPartitionConsumer(parent Consumer, client *client, options *partitionConsumerOpts,
	messageCh chan ConsumerMessage) (*partitionConsumer, error) {
	pc := &partitionConsumer{
		state:          consumerInit,
		parentConsumer: parent,
		client:         client,
		options:        options,
		topic:          options.topic,
		name:           options.consumerName,
		consumerID:     client.rpcClient.NewConsumerID(),
		partitionIdx:   options.partitionIdx,
		eventsCh:       make(chan interface{}, 3),
		queueSize:      int32(options.receiverQueueSize),
		queueCh:        make(chan []*message, options.receiverQueueSize),
		connectedCh:    make(chan struct{}),
		messageCh:      messageCh,
		closeCh:        make(chan struct{}),
		log:            log.WithField("topic", options.topic),
	}
	pc.log = pc.log.WithField("name", pc.name).WithField("subscription", options.subscription)
	pc.nackTracker = newNegativeAcksTracker(pc, options.nackRedeliveryDelay)

	err := pc.grabConn()
	if err != nil {
		log.WithError(err).Errorf("Failed to create consumer")
		return nil, err
	}
	pc.log.Info("Created consumer")
	pc.state = consumerReady

	go pc.dispatcher()

	go pc.runEventsLoop()

	return pc, nil
}

func (pc *partitionConsumer) Unsubscribe() error {
	req := &unsubscribeRequest{doneCh: make(chan struct{})}
	pc.eventsCh <- req

	// wait for the request to complete
	<-req.doneCh
	return req.err
}

func (pc *partitionConsumer) internalUnsubscribe(unsub *unsubscribeRequest) {
	defer close(unsub.doneCh)

	requestID := pc.client.rpcClient.NewRequestID()
	cmdUnsubscribe := &pb.CommandUnsubscribe{
		RequestId:  proto.Uint64(requestID),
		ConsumerId: proto.Uint64(pc.consumerID),
	}
	_, err := pc.client.rpcClient.RequestOnCnx(pc.conn, requestID, pb.BaseCommand_UNSUBSCRIBE, cmdUnsubscribe)
	if err != nil {
		pc.log.WithError(err).Error("Failed to unsubscribe consumer")
		unsub.err = err
	}

	pc.conn.DeleteConsumeHandler(pc.consumerID)
}

func (pc *partitionConsumer) AckID(msgID *messageID) {
	if msgID != nil && msgID.ack() {
		req := &ackRequest{
			msgID: msgID,
		}
		pc.eventsCh <- req
	}
}

func (pc *partitionConsumer) NackID(msgID *messageID) {
	pc.nackTracker.Add(msgID)
}

func (pc *partitionConsumer) Redeliver(msgIds []messageID) {
	pc.eventsCh <- &redeliveryRequest{msgIds}
}

func (pc *partitionConsumer) internalRedeliver(req *redeliveryRequest) {
	msgIds := req.msgIds
	pc.log.Debug("Request redelivery after negative ack for messages", msgIds)

	msgIdDataList := make([]*pb.MessageIdData, len(msgIds))
	for i := 0; i < len(msgIds); i++ {
		msgIdDataList[i] = &pb.MessageIdData{
			LedgerId: proto.Uint64(uint64(msgIds[i].ledgerID)),
			EntryId:  proto.Uint64(uint64(msgIds[i].entryID)),
		}
	}

	pc.client.rpcClient.RequestOnCnxNoWait(pc.conn,
		pb.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES, &pb.CommandRedeliverUnacknowledgedMessages{
			ConsumerId: proto.Uint64(pc.consumerID),
			MessageIds: msgIdDataList,
		})
}

func (pc *partitionConsumer) Close() {
	if pc.state != consumerReady {
		return
	}

	req := &closeRequest{doneCh: make(chan struct{})}
	pc.eventsCh <- req

	// wait for request to finish
	<-req.doneCh
}

func (pc *partitionConsumer) internalAck(req *ackRequest) {
	msgId := req.msgID

	messageIDs := make([]*pb.MessageIdData, 1)
	messageIDs[0] = &pb.MessageIdData{
		LedgerId: proto.Uint64(uint64(msgId.ledgerID)),
		EntryId:  proto.Uint64(uint64(msgId.entryID)),
	}

	cmdAck := &pb.CommandAck{
		ConsumerId: proto.Uint64(pc.consumerID),
		MessageId:  messageIDs,
		AckType:    pb.CommandAck_Individual.Enum(),
	}

	pc.client.rpcClient.RequestOnCnxNoWait(pc.conn, pb.BaseCommand_ACK, cmdAck)
}

func (pc *partitionConsumer) MessageReceived(response *pb.CommandMessage, headersAndPayload internal.Buffer) error {
	pbMsgID := response.GetMessageId()

	reader := internal.NewMessageReader(headersAndPayload)
	msgMeta, err := reader.ReadMessageMetadata()
	if err != nil {
		// TODO send discardCorruptedMessage
		return err
	}

	numMsgs := 1
	if msgMeta.NumMessagesInBatch != nil {
		numMsgs = int(msgMeta.GetNumMessagesInBatch())
	}
	messages := make([]*message, numMsgs)
	var ackTracker *ackTracker
	// are there multiple messages in this batch?
	if numMsgs > 1 {
		ackTracker = newAckTracker(numMsgs)
	}
	for i := 0; i < numMsgs; i++ {
		smm, payload, err := reader.ReadMessage()
		if err != nil {
			// TODO send corrupted message
			return err
		}

		msgID := newTrackingMessageID(
			int64(pbMsgID.GetLedgerId()),
			int64(pbMsgID.GetEntryId()),
			i,
			pc.partitionIdx,
			ackTracker)
		var msg *message
		if smm != nil {
			msg = &message{
				publishTime: timeFromUnixTimestampMillis(msgMeta.GetPublishTime()),
				eventTime:   timeFromUnixTimestampMillis(smm.GetEventTime()),
				key:         smm.GetPartitionKey(),
				properties:  internal.ConvertToStringMap(smm.GetProperties()),
				topic:       pc.topic,
				msgID:       msgID,
				payLoad:     payload,
			}
		} else {
			msg = &message{
				publishTime: timeFromUnixTimestampMillis(msgMeta.GetPublishTime()),
				eventTime:   timeFromUnixTimestampMillis(msgMeta.GetEventTime()),
				key:         msgMeta.GetPartitionKey(),
				properties:  internal.ConvertToStringMap(msgMeta.GetProperties()),
				topic:       pc.topic,
				msgID:       msgID,
				payLoad:     payload,
			}
		}

		messages[i] = msg
	}

	// send messages to the dispatcher
	pc.queueCh <- messages
	return nil
}

func (pc *partitionConsumer) ConnectionClosed() {
	// Trigger reconnection in the consumer goroutine
	pc.eventsCh <- &connectionClosed{}
}

// Flow command gives additional permits to send messages to the consumer.
// A typical consumer implementation will use a queue to accumulate these messages
// before the application is ready to consume them. After the consumer is ready,
// the client needs to give permission to the broker to push messages.
func (pc *partitionConsumer) internalFlow(permits uint32) error {
	if permits == 0 {
		return fmt.Errorf("invalid number of permits requested: %d", permits)
	}

	cmdFlow := &pb.CommandFlow{
		ConsumerId:     proto.Uint64(pc.consumerID),
		MessagePermits: proto.Uint32(permits),
	}
	pc.client.rpcClient.RequestOnCnxNoWait(pc.conn, pb.BaseCommand_FLOW, cmdFlow)

	return nil
}

// dispatcher manages the internal message queue channel
// and manages the flow control
func (pc *partitionConsumer) dispatcher() {
	defer func() {
		pc.log.Info("exiting dispatch loop")
	}()
	var messages []*message
	for {
		var queueCh chan []*message
		var messageCh chan ConsumerMessage
		var nextMessage ConsumerMessage

		// are there more messages to send?
		if len(messages) > 0 {
			nextMessage = ConsumerMessage{
				Consumer: pc.parentConsumer,
				Message:  messages[0],
			}
			messageCh = pc.messageCh
		} else {
			// we are ready for more messages
			queueCh = pc.queueCh
		}

		select {
		case <-pc.closeCh:
			return

		case _, ok := <-pc.connectedCh:
			if !ok {
				return
			}
			pc.log.Debug("dispatcher received connection event")

			// drain messages
			messages = nil

			// drain the message queue on any new connection by sending a
			// special nil message to the channel so we know when to stop dropping messages
			go func() {
				pc.queueCh <- nil
			}()
			for m := range pc.queueCh {
				// the queue has been drained
				if m == nil {
					break
				}
			}

			// reset available permits
			pc.availablePermits = 0
			initialPermits := uint32(pc.queueSize)

			pc.log.Debugf("dispatcher requesting initial permits=%d", initialPermits)
			// send initial permits
			if err := pc.internalFlow(initialPermits); err != nil {
				pc.log.WithError(err).Error("unable to send initial permits to broker")
			}

		case msgs, ok := <-queueCh:
			if !ok {
				return
			}
			// we only read messages here after the consumer has processed all messages
			// in the previous batch
			messages = msgs

		// if the messageCh is nil or the messageCh is full this will not be selected
		case messageCh <- nextMessage:
			// allow this message to be garbage collected
			messages[0] = nil
			messages = messages[1:]

			// TODO implement a better flow controller
			// send more permits if needed
			pc.availablePermits++
			flowThreshold := int32(math.Max(float64(pc.queueSize/2), 1))
			if pc.availablePermits >= flowThreshold {
				availablePermits := pc.availablePermits
				requestedPermits := availablePermits
				pc.availablePermits = 0

				pc.log.Debugf("requesting more permits=%d available=%d", requestedPermits, availablePermits)
				if err := pc.internalFlow(uint32(requestedPermits)); err != nil {
					pc.log.WithError(err).Error("unable to send permits")
				}
			}
		}
	}
}

type ackRequest struct {
	msgID *messageID
}

type unsubscribeRequest struct {
	doneCh chan struct{}
	err    error
}

type closeRequest struct {
	doneCh chan struct{}
}

type redeliveryRequest struct {
	msgIds []messageID
}

func (pc *partitionConsumer) runEventsLoop() {
	defer func() {
		pc.log.Info("exiting events loop")
	}()
	for {
		select {
		case <-pc.closeCh:
			return
		case i := <-pc.eventsCh:
			switch v := i.(type) {
			case *ackRequest:
				pc.internalAck(v)
			case *redeliveryRequest:
				pc.internalRedeliver(v)
			case *unsubscribeRequest:
				pc.internalUnsubscribe(v)
			case *connectionClosed:
				pc.reconnectToBroker()
			case *closeRequest:
				pc.internalClose(v)
				return
			}
		}
	}
}

func (pc *partitionConsumer) internalClose(req *closeRequest) {
	defer close(req.doneCh)
	if pc.state != consumerReady {
		return
	}

	pc.state = consumerClosing
	pc.log.Infof("Closing consumer=%d", pc.consumerID)

	requestID := pc.client.rpcClient.NewConsumerID()
	cmdClose := &pb.CommandCloseConsumer{
		ConsumerId: proto.Uint64(pc.consumerID),
		RequestId:  proto.Uint64(requestID),
	}
	_, err := pc.client.rpcClient.RequestOnCnx(pc.conn, requestID, pb.BaseCommand_CLOSE_CONSUMER, cmdClose)
	if err != nil {
		pc.log.WithError(err).Warn("Failed to close consumer")
	} else {
		pc.log.Info("Closed consumer")
	}

	pc.state = consumerClosed
	pc.conn.DeleteConsumeHandler(pc.consumerID)
	pc.nackTracker.Close()
	close(pc.closeCh)
}

func (pc *partitionConsumer) reconnectToBroker() {
	backoff := internal.Backoff{}
	for {
		if pc.state != consumerReady {
			// Consumer is already closing
			return
		}

		d := backoff.Next()
		pc.log.Info("Reconnecting to broker in ", d)
		time.Sleep(d)

		err := pc.grabConn()
		if err == nil {
			// Successfully reconnected
			pc.log.Info("Reconnected consumer to broker")
			return
		}
	}
}

func (pc *partitionConsumer) grabConn() error {
	lr, err := pc.client.lookupService.Lookup(pc.topic)
	if err != nil {
		pc.log.WithError(err).Warn("Failed to lookup topic")
		return err
	}
	pc.log.Debugf("Lookup result: %+v", lr)

	subType := toProtoSubType(pc.options.subscriptionType)
	initialPosition := toProtoInitialPosition(pc.options.subscriptionInitPos)
	requestID := pc.client.rpcClient.NewRequestID()
	cmdSubscribe := &pb.CommandSubscribe{
		RequestId:       proto.Uint64(requestID),
		Topic:           proto.String(pc.topic),
		SubType:         subType.Enum(),
		Subscription:    proto.String(pc.options.subscription),
		ConsumerId:      proto.Uint64(pc.consumerID),
		ConsumerName:    proto.String(pc.name),
		InitialPosition: initialPosition.Enum(),
		Schema:          nil,
	}

	res, err := pc.client.rpcClient.Request(lr.LogicalAddr, lr.PhysicalAddr, requestID,
		pb.BaseCommand_SUBSCRIBE, cmdSubscribe)

	if err != nil {
		pc.log.WithError(err).Error("Failed to create consumer")
		return err
	}

	if res.Response.ConsumerStatsResponse != nil {
		pc.name = res.Response.ConsumerStatsResponse.GetConsumerName()
	}

	pc.conn = res.Cnx
	pc.log.Info("Connected consumer")
	pc.conn.AddConsumeHandler(pc.consumerID, pc)

	msgType := res.Response.GetType()

	switch msgType {
	case pb.BaseCommand_SUCCESS:
		// notify the dispatcher we have connection
		go func() {
			pc.connectedCh <- struct{}{}
		}()
		return nil
	case pb.BaseCommand_ERROR:
		errMsg := res.Response.GetError()
		return fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())
	default:
		return newUnexpectedErrMsg(msgType, requestID)
	}
}
