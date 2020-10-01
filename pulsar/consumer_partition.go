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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/gogo/protobuf/proto"

	log "github.com/sirupsen/logrus"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
)

var (
	messagesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsar_client_messages_received",
		Help: "Counter of messages received by the client",
	})

	bytesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsar_client_bytes_received",
		Help: "Counter of bytes received by the client",
	})

	prefetchedMessages = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulsar_client_consumer_prefetched_messages",
		Help: "Number of messages currently sitting in the consumer pre-fetch queue",
	})

	prefetchedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulsar_client_consumer_prefetched_bytes",
		Help: "Total number of bytes currently sitting in the consumer pre-fetch queue",
	})

	acksCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsar_client_consumer_acks",
		Help: "Counter of messages acked by client",
	})

	nacksCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsar_client_consumer_nacks",
		Help: "Counter of messages nacked by client",
	})

	dlqCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsar_client_consumer_dlq_messages",
		Help: "Counter of messages sent to Dead letter queue",
	})

	processingTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pulsar_client_consumer_processing_time_seconds",
		Help:    "Time it takes for application to process messages",
		Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})

	lastestMessageID = LatestMessageID()
)

type consumerState int

const (
	consumerInit consumerState = iota
	consumerReady
	consumerClosing
	consumerClosed
)

type subscriptionMode int

const (
	// Make the subscription to be backed by a durable cursor that will retain messages and persist the current
	// position
	durable subscriptionMode = iota

	// Lightweight subscription mode that doesn't have a durable cursor associated
	nonDurable
)

const (
	noMessageEntry = -1
)

type partitionConsumerOpts struct {
	topic                      string
	consumerName               string
	subscription               string
	subscriptionType           SubscriptionType
	subscriptionInitPos        SubscriptionInitialPosition
	partitionIdx               int
	receiverQueueSize          int
	nackRedeliveryDelay        time.Duration
	metadata                   map[string]string
	replicateSubscriptionState bool
	startMessageID             trackingMessageID
	startMessageIDInclusive    bool
	subscriptionMode           subscriptionMode
	readCompacted              bool
	disableForceTopicCreation  bool
	interceptors               ConsumerInterceptors
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
	partitionIdx int32

	// shared channel
	messageCh chan ConsumerMessage

	// the number of message slots available
	availablePermits int32

	// the size of the queue channel for buffering messages
	queueSize       int32
	queueCh         chan []*message
	startMessageID  trackingMessageID
	lastDequeuedMsg trackingMessageID

	eventsCh         chan interface{}
	connectedCh      chan struct{}
	connectClosedCh  chan struct{}
	closeCh          chan struct{}
	clearQueueCh     chan func(id trackingMessageID)

	nackTracker *negativeAcksTracker
	dlq         *dlqRouter

	log *log.Entry

	compressionProviders map[pb.CompressionType]compression.Provider
}

func newPartitionConsumer(parent Consumer, client *client, options *partitionConsumerOpts,
	messageCh chan ConsumerMessage, dlq *dlqRouter) (*partitionConsumer, error) {
	pc := &partitionConsumer{
		state:                consumerInit,
		parentConsumer:       parent,
		client:               client,
		options:              options,
		topic:                options.topic,
		name:                 options.consumerName,
		consumerID:           client.rpcClient.NewConsumerID(),
		partitionIdx:         int32(options.partitionIdx),
		eventsCh:             make(chan interface{}, 3),
		queueSize:            int32(options.receiverQueueSize),
		queueCh:              make(chan []*message, options.receiverQueueSize),
		startMessageID:       options.startMessageID,
		connectedCh:          make(chan struct{}),
		messageCh:            messageCh,
		connectClosedCh:      make(chan struct{}),
		closeCh:              make(chan struct{}),
		clearQueueCh:         make(chan func(id trackingMessageID)),
		compressionProviders: make(map[pb.CompressionType]compression.Provider),
		dlq:                  dlq,
		log:                  log.WithField("topic", options.topic),
	}
	pc.log = pc.log.WithField("name", pc.name).
		WithField("subscription", options.subscription).
		WithField("consumerID", pc.consumerID)
	pc.nackTracker = newNegativeAcksTracker(pc, options.nackRedeliveryDelay)

	err := pc.grabConn()
	if err != nil {
		log.WithError(err).Errorf("Failed to create consumer")
		return nil, err
	}
	pc.log.Info("Created consumer")
	pc.state = consumerReady

	if pc.options.startMessageIDInclusive && pc.startMessageID == lastestMessageID {
		msgID, err := pc.requestGetLastMessageID()
		if err != nil {
			return nil, err
		}
		if msgID.entryID != noMessageEntry {
			pc.startMessageID = msgID

			err = pc.requestSeek(msgID.messageID)
			if err != nil {
				return nil, err
			}
		}
	}

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

	if pc.state == consumerClosed || pc.state == consumerClosing {
		pc.log.Error("Failed to unsubscribe consumer, the consumer is closing or consumer has been closed")
		return
	}

	pc.state = consumerClosing
	requestID := pc.client.rpcClient.NewRequestID()
	cmdUnsubscribe := &pb.CommandUnsubscribe{
		RequestId:  proto.Uint64(requestID),
		ConsumerId: proto.Uint64(pc.consumerID),
	}
	_, err := pc.client.rpcClient.RequestOnCnx(pc.conn, requestID, pb.BaseCommand_UNSUBSCRIBE, cmdUnsubscribe)
	if err != nil {
		pc.log.WithError(err).Error("Failed to unsubscribe consumer")
		unsub.err = err
		// Set the state to ready for closing the consumer
		pc.state = consumerReady
		// Should'nt remove the consumer handler
		return
	}

	pc.conn.DeleteConsumeHandler(pc.consumerID)
	if pc.nackTracker != nil {
		pc.nackTracker.Close()
	}
	pc.log.Infof("The consumer[%d] successfully unsubscribed", pc.consumerID)
	pc.state = consumerClosed
}

func (pc *partitionConsumer) getLastMessageID() (trackingMessageID, error) {
	req := &getLastMsgIDRequest{doneCh: make(chan struct{})}
	pc.eventsCh <- req

	// wait for the request to complete
	<-req.doneCh
	return req.msgID, req.err
}

func (pc *partitionConsumer) internalGetLastMessageID(req *getLastMsgIDRequest) {
	defer close(req.doneCh)
	req.msgID, req.err = pc.requestGetLastMessageID()
}

func (pc *partitionConsumer) requestGetLastMessageID() (trackingMessageID, error) {
	requestID := pc.client.rpcClient.NewRequestID()
	cmdGetLastMessageID := &pb.CommandGetLastMessageId{
		RequestId:  proto.Uint64(requestID),
		ConsumerId: proto.Uint64(pc.consumerID),
	}
	res, err := pc.client.rpcClient.RequestOnCnx(pc.conn, requestID,
		pb.BaseCommand_GET_LAST_MESSAGE_ID, cmdGetLastMessageID)
	if err != nil {
		pc.log.WithError(err).Error("Failed to get last message id")
		return trackingMessageID{}, err
	}
	id := res.Response.GetLastMessageIdResponse.GetLastMessageId()
	return convertToMessageID(id), nil
}

func (pc *partitionConsumer) AckID(msgID trackingMessageID) {
	if !msgID.Undefined() && msgID.ack() {
		acksCounter.Inc()
		processingTime.Observe(float64(time.Now().UnixNano()-msgID.receivedTime.UnixNano()) / 1.0e9)
		req := &ackRequest{
			msgID: msgID,
		}
		pc.eventsCh <- req

		pc.options.interceptors.OnAcknowledge(pc.parentConsumer, msgID)
	}
}

func (pc *partitionConsumer) NackID(msgID trackingMessageID) {
	pc.nackTracker.Add(msgID.messageID)
	nacksCounter.Inc()
}

func (pc *partitionConsumer) Redeliver(msgIds []messageID) {
	pc.eventsCh <- &redeliveryRequest{msgIds}

	iMsgIds := make([]MessageID, len(msgIds))
	for i := range iMsgIds {
		iMsgIds[i] = &msgIds[i]
	}
	pc.options.interceptors.OnNegativeAcksSend(pc.parentConsumer, iMsgIds)
}

func (pc *partitionConsumer) internalRedeliver(req *redeliveryRequest) {
	msgIds := req.msgIds
	pc.log.Debug("Request redelivery after negative ack for messages", msgIds)

	msgIDDataList := make([]*pb.MessageIdData, len(msgIds))
	for i := 0; i < len(msgIds); i++ {
		msgIDDataList[i] = &pb.MessageIdData{
			LedgerId: proto.Uint64(uint64(msgIds[i].ledgerID)),
			EntryId:  proto.Uint64(uint64(msgIds[i].entryID)),
		}
	}

	pc.client.rpcClient.RequestOnCnxNoWait(pc.conn,
		pb.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES, &pb.CommandRedeliverUnacknowledgedMessages{
			ConsumerId: proto.Uint64(pc.consumerID),
			MessageIds: msgIDDataList,
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

func (pc *partitionConsumer) Seek(msgID trackingMessageID) error {
	req := &seekRequest{
		doneCh: make(chan struct{}),
		msgID:  msgID,
	}
	pc.eventsCh <- req

	// wait for the request to complete
	<-req.doneCh
	return req.err
}

func (pc *partitionConsumer) internalSeek(seek *seekRequest) {
	defer close(seek.doneCh)
	seek.err = pc.requestSeek(seek.msgID.messageID)
}

func (pc *partitionConsumer) requestSeek(msgID messageID) error {
	if pc.state == consumerClosing || pc.state == consumerClosed {
		pc.log.Error("Consumer was already closed")
		return nil
	}

	id := &pb.MessageIdData{}
	err := proto.Unmarshal(msgID.Serialize(), id)
	if err != nil {
		pc.log.WithError(err).Errorf("deserialize message id error: %s", err.Error())
		return err
	}

	requestID := pc.client.rpcClient.NewRequestID()
	cmdSeek := &pb.CommandSeek{
		ConsumerId: proto.Uint64(pc.consumerID),
		RequestId:  proto.Uint64(requestID),
		MessageId:  id,
	}

	_, err = pc.client.rpcClient.RequestOnCnx(pc.conn, requestID, pb.BaseCommand_SEEK, cmdSeek)
	if err != nil {
		pc.log.WithError(err).Error("Failed to reset to message id")
		return err
	}
	return nil
}

func (pc *partitionConsumer) SeekByTime(time time.Time) error {
	req := &seekByTimeRequest{
		doneCh:      make(chan struct{}),
		publishTime: time,
	}
	pc.eventsCh <- req

	// wait for the request to complete
	<-req.doneCh
	return req.err
}

func (pc *partitionConsumer) internalSeekByTime(seek *seekByTimeRequest) {
	defer close(seek.doneCh)

	if pc.state == consumerClosing || pc.state == consumerClosed {
		pc.log.Error("Consumer was already closed")
		return
	}

	requestID := pc.client.rpcClient.NewRequestID()
	cmdSeek := &pb.CommandSeek{
		ConsumerId:         proto.Uint64(pc.consumerID),
		RequestId:          proto.Uint64(requestID),
		MessagePublishTime: proto.Uint64(uint64(seek.publishTime.UnixNano() / int64(time.Millisecond))),
	}

	_, err := pc.client.rpcClient.RequestOnCnx(pc.conn, requestID, pb.BaseCommand_SEEK, cmdSeek)
	if err != nil {
		pc.log.WithError(err).Error("Failed to reset to message publish time")
		seek.err = err
	}
}

func (pc *partitionConsumer) internalAck(req *ackRequest) {
	msgID := req.msgID

	messageIDs := make([]*pb.MessageIdData, 1)
	messageIDs[0] = &pb.MessageIdData{
		LedgerId: proto.Uint64(uint64(msgID.ledgerID)),
		EntryId:  proto.Uint64(uint64(msgID.entryID)),
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
		pc.discardCorruptedMessage(pbMsgID, pb.CommandAck_ChecksumMismatch)
		return err
	}

	uncompressedHeadersAndPayload, err := pc.Decompress(msgMeta, headersAndPayload)
	if err != nil {
		pc.discardCorruptedMessage(pbMsgID, pb.CommandAck_DecompressionError)
		return err
	}

	// Reset the reader on the uncompressed buffer
	reader.ResetBuffer(uncompressedHeadersAndPayload)

	numMsgs := 1
	if msgMeta.NumMessagesInBatch != nil {
		numMsgs = int(msgMeta.GetNumMessagesInBatch())
	}
	messages := make([]*message, 0)
	var ackTracker *ackTracker
	// are there multiple messages in this batch?
	if numMsgs > 1 {
		ackTracker = newAckTracker(numMsgs)
	}

	messagesReceived.Add(float64(numMsgs))
	prefetchedMessages.Add(float64(numMsgs))

	for i := 0; i < numMsgs; i++ {
		smm, payload, err := reader.ReadMessage()
		if err != nil {
			pc.discardCorruptedMessage(pbMsgID, pb.CommandAck_BatchDeSerializeError)
			return err
		}

		bytesReceived.Add(float64(len(payload)))
		prefetchedBytes.Add(float64(len(payload)))

		msgID := newTrackingMessageID(
			int64(pbMsgID.GetLedgerId()),
			int64(pbMsgID.GetEntryId()),
			int32(i),
			pc.partitionIdx,
			ackTracker)

		if pc.messageShouldBeDiscarded(msgID) {
			pc.AckID(msgID)
			continue
		}

		// set the consumer so we know how to ack the message id
		msgID.consumer = pc
		var msg *message
		if smm != nil {
			msg = &message{
				publishTime:         timeFromUnixTimestampMillis(msgMeta.GetPublishTime()),
				eventTime:           timeFromUnixTimestampMillis(smm.GetEventTime()),
				key:                 smm.GetPartitionKey(),
				producerName:        msgMeta.GetProducerName(),
				properties:          internal.ConvertToStringMap(smm.GetProperties()),
				topic:               pc.topic,
				msgID:               msgID,
				payLoad:             payload,
				replicationClusters: msgMeta.GetReplicateTo(),
				replicatedFrom:      msgMeta.GetReplicatedFrom(),
				redeliveryCount:     response.GetRedeliveryCount(),
			}
		} else {
			msg = &message{
				publishTime:         timeFromUnixTimestampMillis(msgMeta.GetPublishTime()),
				eventTime:           timeFromUnixTimestampMillis(msgMeta.GetEventTime()),
				key:                 msgMeta.GetPartitionKey(),
				producerName:        msgMeta.GetProducerName(),
				properties:          internal.ConvertToStringMap(msgMeta.GetProperties()),
				topic:               pc.topic,
				msgID:               msgID,
				payLoad:             payload,
				replicationClusters: msgMeta.GetReplicateTo(),
				replicatedFrom:      msgMeta.GetReplicatedFrom(),
				redeliveryCount:     response.GetRedeliveryCount(),
			}
		}

		pc.options.interceptors.BeforeConsume(ConsumerMessage{
			Consumer: pc.parentConsumer,
			Message:  msg,
		})

		messages = append(messages, msg)
	}

	// send messages to the dispatcher
	pc.queueCh <- messages
	return nil
}

func (pc *partitionConsumer) messageShouldBeDiscarded(msgID trackingMessageID) bool {
	if pc.startMessageID.Undefined() {
		return false
	}

	if pc.options.startMessageIDInclusive {
		return pc.startMessageID.greater(msgID.messageID)
	}

	// Non inclusive
	return pc.startMessageID.greaterEqual(msgID.messageID)
}

func (pc *partitionConsumer) ConnectionClosed() {
	// Trigger reconnection in the consumer goroutine
	pc.log.Debug("connection closed and send to connectClosedCh")
	pc.connectClosedCh <- connectionClosed{}
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
		pc.log.Debug("exiting dispatch loop")
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

			if pc.dlq.shouldSendToDlq(&nextMessage) {
				// pass the message to the DLQ router
				dlqCounter.Inc()
				messageCh = pc.dlq.Chan()
			} else {
				// pass the message to application channel
				messageCh = pc.messageCh
			}

			prefetchedMessages.Dec()
			prefetchedBytes.Sub(float64(len(messages[0].payLoad)))
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

			messages = nil

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

		case clearQueueCb := <-pc.clearQueueCh:
			// drain the message queue on any new connection by sending a
			// special nil message to the channel so we know when to stop dropping messages
			var nextMessageInQueue trackingMessageID
			go func() {
				pc.queueCh <- nil
			}()
			for m := range pc.queueCh {
				// the queue has been drained
				if m == nil {
					break
				} else if nextMessageInQueue.Undefined() {
					nextMessageInQueue = m[0].msgID.(trackingMessageID)
				}
			}

			clearQueueCb(nextMessageInQueue)
		}
	}
}

type ackRequest struct {
	msgID trackingMessageID
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

type getLastMsgIDRequest struct {
	doneCh chan struct{}
	msgID  trackingMessageID
	err    error
}

type seekRequest struct {
	doneCh chan struct{}
	msgID  trackingMessageID
	err    error
}

type seekByTimeRequest struct {
	doneCh      chan struct{}
	publishTime time.Time
	err         error
}

func (pc *partitionConsumer) runEventsLoop() {
	defer func() {
		pc.log.Debug("exiting events loop")
	}()
	pc.log.Debug("get into runEventsLoop")

	go func() {
		for {
			select {
			case <-pc.closeCh:
				return
			case <-pc.connectClosedCh:
				pc.log.Debug("runEventsLoop will reconnect")
				pc.reconnectToBroker()
			}
		}
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
			case *getLastMsgIDRequest:
				pc.internalGetLastMessageID(v)
			case *seekRequest:
				pc.internalSeek(v)
			case *seekByTimeRequest:
				pc.internalSeekByTime(v)
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

	if pc.state == consumerClosed || pc.state == consumerClosing {
		pc.log.Error("The consumer is closing or has been closed")
		if pc.nackTracker != nil {
			pc.nackTracker.Close()
		}
		return
	}

	pc.state = consumerClosing
	pc.log.Infof("Closing consumer=%d", pc.consumerID)

	requestID := pc.client.rpcClient.NewRequestID()
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

	for _, provider := range pc.compressionProviders {
		provider.Close()
	}

	pc.state = consumerClosed
	pc.conn.DeleteConsumeHandler(pc.consumerID)
	if pc.nackTracker != nil {
		pc.nackTracker.Close()
	}
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
		Topic:                      proto.String(pc.topic),
		Subscription:               proto.String(pc.options.subscription),
		SubType:                    subType.Enum(),
		ConsumerId:                 proto.Uint64(pc.consumerID),
		RequestId:                  proto.Uint64(requestID),
		ConsumerName:               proto.String(pc.name),
		PriorityLevel:              nil,
		Durable:                    proto.Bool(pc.options.subscriptionMode == durable),
		Metadata:                   internal.ConvertFromStringMap(pc.options.metadata),
		ReadCompacted:              proto.Bool(pc.options.readCompacted),
		Schema:                     nil,
		InitialPosition:            initialPosition.Enum(),
		ReplicateSubscriptionState: proto.Bool(pc.options.replicateSubscriptionState),
	}

	pc.startMessageID = pc.clearReceiverQueue()
	if pc.options.subscriptionMode != durable {
		// For regular subscriptions the broker will determine the restarting point
		cmdSubscribe.StartMessageId = convertToMessageIDData(pc.startMessageID)
	}

	if len(pc.options.metadata) > 0 {
		cmdSubscribe.Metadata = toKeyValues(pc.options.metadata)
	}

	// force topic creation is enabled by default so
	// we only need to set the flag when disabling it
	if pc.options.disableForceTopicCreation {
		cmdSubscribe.ForceTopicCreation = proto.Bool(false)
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

func (pc *partitionConsumer) clearQueueAndGetNextMessage() trackingMessageID {
	if pc.state != consumerReady {
		return trackingMessageID{}
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	var msgID trackingMessageID

	pc.clearQueueCh <- func(id trackingMessageID) {
		msgID = id
		wg.Done()
	}

	wg.Wait()
	return msgID
}

/**
 * Clear the internal receiver queue and returns the message id of what was the 1st message in the queue that was
 * not seen by the application
 */
func (pc *partitionConsumer) clearReceiverQueue() trackingMessageID {
	nextMessageInQueue := pc.clearQueueAndGetNextMessage()

	if !nextMessageInQueue.Undefined() {
		return getPreviousMessage(nextMessageInQueue)
	} else if !pc.lastDequeuedMsg.Undefined() {
		// If the queue was empty we need to restart from the message just after the last one that has been dequeued
		// in the past
		return pc.lastDequeuedMsg
	} else {
		// No message was received or dequeued by this consumer. Next message would still be the startMessageId
		return pc.startMessageID
	}
}

func getPreviousMessage(mid trackingMessageID) trackingMessageID {
	if mid.batchIdx >= 0 {
		return trackingMessageID{
			messageID: messageID{
				ledgerID:     mid.ledgerID,
				entryID:      mid.entryID,
				batchIdx:     mid.batchIdx - 1,
				partitionIdx: mid.partitionIdx,
			},
			tracker:      mid.tracker,
			consumer:     mid.consumer,
			receivedTime: mid.receivedTime,
		}
	}

	// Get on previous message in previous entry
	return trackingMessageID{
		messageID: messageID{
			ledgerID:     mid.ledgerID,
			entryID:      mid.entryID - 1,
			batchIdx:     mid.batchIdx,
			partitionIdx: mid.partitionIdx,
		},
		tracker:      mid.tracker,
		consumer:     mid.consumer,
		receivedTime: mid.receivedTime,
	}
}

func (pc *partitionConsumer) Decompress(msgMeta *pb.MessageMetadata, payload internal.Buffer) (internal.Buffer, error) {
	provider, ok := pc.compressionProviders[msgMeta.GetCompression()]
	if !ok {
		var err error
		if provider, err = pc.initializeCompressionProvider(msgMeta.GetCompression()); err != nil {
			pc.log.WithError(err).Error("Failed to decompress message.")
			return nil, err
		}

		pc.compressionProviders[msgMeta.GetCompression()] = provider
	}

	uncompressed, err := provider.Decompress(nil, payload.ReadableSlice(), int(msgMeta.GetUncompressedSize()))
	if err != nil {
		return nil, err
	}

	return internal.NewBufferWrapper(uncompressed), nil
}

func (pc *partitionConsumer) initializeCompressionProvider(
	compressionType pb.CompressionType) (compression.Provider, error) {
	switch compressionType {
	case pb.CompressionType_NONE:
		return compression.NewNoopProvider(), nil
	case pb.CompressionType_ZLIB:
		return compression.NewZLibProvider(), nil
	case pb.CompressionType_LZ4:
		return compression.NewLz4Provider(), nil
	case pb.CompressionType_ZSTD:
		return compression.NewZStdProvider(compression.Default), nil
	}

	return nil, fmt.Errorf("unsupported compression type: %v", compressionType)
}

func (pc *partitionConsumer) discardCorruptedMessage(msgID *pb.MessageIdData,
	validationError pb.CommandAck_ValidationError) {
	pc.log.WithFields(log.Fields{
		"msgID":           msgID,
		"validationError": validationError,
	}).Error("Discarding corrupted message")

	pc.client.rpcClient.RequestOnCnxNoWait(pc.conn,
		pb.BaseCommand_ACK, &pb.CommandAck{
			ConsumerId:      proto.Uint64(pc.consumerID),
			MessageId:       []*pb.MessageIdData{msgID},
			AckType:         pb.CommandAck_Individual.Enum(),
			ValidationError: validationError.Enum(),
		})
}

func convertToMessageIDData(msgID trackingMessageID) *pb.MessageIdData {
	if msgID.Undefined() {
		return nil
	}

	return &pb.MessageIdData{
		LedgerId: proto.Uint64(uint64(msgID.ledgerID)),
		EntryId:  proto.Uint64(uint64(msgID.entryID)),
	}
}

func convertToMessageID(id *pb.MessageIdData) trackingMessageID {
	if id == nil {
		return trackingMessageID{}
	}

	msgID := trackingMessageID{
		messageID: messageID{
			ledgerID: int64(*id.LedgerId),
			entryID:  int64(*id.EntryId),
		},
	}
	if id.BatchIndex != nil {
		msgID.batchIdx = *id.BatchIndex
	}

	return msgID
}
