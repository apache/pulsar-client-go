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
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pkg/pb"
	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/util"
	"github.com/golang/protobuf/proto"

	log "github.com/sirupsen/logrus"
)

const maxRedeliverUnacknowledged = 1000

type consumerState int

const (
	consumerInit consumerState = iota
	consumerReady
	consumerClosing
	consumerClosed
)

var (
	subType  pb.CommandSubscribe_SubType
	position pb.CommandSubscribe_InitialPosition
)

type partitionConsumer struct {
	state  consumerState
	client *client
	topic  string
	log    *log.Entry
	cnx    internal.Connection

	options      *ConsumerOptions
	consumerName *string
	consumerID   uint64
	subQueue     chan ConsumerMessage

	omu               sync.Mutex // protects following
	redeliverMessages []*pb.MessageIdData

	unAckTracker      *UnackedMessageTracker
	receivedSinceFlow uint32

	eventsChan   chan interface{}
	partitionIdx int
	partitionNum int
}

func newPartitionConsumer(client *client, topic string, options *ConsumerOptions,
	partitionID, partitionNum int, ch chan<-ConsumerMessage) (*partitionConsumer, error) {
	c := &partitionConsumer{
		state:             consumerInit,
		client:            client,
		topic:             topic,
		options:           options,
		log:               log.WithField("topic", topic),
		consumerID:        client.rpcClient.NewConsumerID(),
		partitionIdx:      partitionID,
		partitionNum:      partitionNum,
		eventsChan:        make(chan interface{}, 1),
		subQueue:          make(chan ConsumerMessage, options.ReceiverQueueSize),
		receivedSinceFlow: 0,
	}

	c.setDefault(options)

	if options.MessageChannel == nil {
		options.MessageChannel = make(chan ConsumerMessage, options.ReceiverQueueSize)
	} else {
		c.subQueue = options.MessageChannel
	}

	if options.Name != "" {
		c.consumerName = &options.Name
	}

	switch options.Type {
	case Exclusive:
		subType = pb.CommandSubscribe_Exclusive
	case Failover:
		subType = pb.CommandSubscribe_Failover
	case Shared:
		subType = pb.CommandSubscribe_Shared
	case KeyShared:
		subType = pb.CommandSubscribe_Key_Shared
	}

	if options.Type == Shared || options.Type == KeyShared {
		if options.AckTimeout != 0 {
			c.unAckTracker = NewUnackedMessageTracker()
			c.unAckTracker.pcs = append(c.unAckTracker.pcs, c)
			c.unAckTracker.Start(int64(options.AckTimeout))
		}
	}

	switch options.SubscriptionInitPos {
	case Latest:
		position = pb.CommandSubscribe_Latest
	case Earliest:
		position = pb.CommandSubscribe_Earliest
	}

	err := c.grabCnx()
	if err != nil {
		log.WithError(err).Errorf("Failed to create consumer")
		return nil, err
	}
	c.log = c.log.WithField("consumerID", c.consumerID)
	c.log.Info("Created consumer")
	c.state = consumerReady

	go c.runEventsLoop()

	return c, nil
}

func (pc *partitionConsumer) setDefault(options *ConsumerOptions) {
	if options.ReceiverQueueSize <= 0 {
		options.ReceiverQueueSize = 1000
	}

	if options.AckTimeout == 0 {
		options.AckTimeout = time.Second * 30
	}

	position = pb.CommandSubscribe_Latest
	subType = pb.CommandSubscribe_Exclusive
}

func (pc *partitionConsumer) getMessageFromSubConsumer(ctx context.Context, ch chan<- ConsumerMessage) {
	err := pc.ReceiveAsync(ctx, ch)
	if err != nil {
		pc.log.Errorf("get message from sub queue error: %s", err.Error())
		return
	}
}

func (pc *partitionConsumer) grabCnx() error {
	lr, err := pc.client.lookupService.Lookup(pc.topic)
	if err != nil {
		pc.log.WithError(err).Warn("Failed to lookup topic")
		return err
	}

	pc.log.Debugf("Lookup result: %v, consumerID: %d", lr.LogicalAddr, pc.consumerID)
	requestID := pc.client.rpcClient.NewRequestID()
	res, err := pc.client.rpcClient.Request(lr.LogicalAddr, lr.PhysicalAddr, requestID,
		pb.BaseCommand_SUBSCRIBE, &pb.CommandSubscribe{
			RequestId:       proto.Uint64(requestID),
			Topic:           proto.String(pc.topic),
			SubType:         subType.Enum(),
			Subscription:    proto.String(pc.options.SubscriptionName),
			ConsumerId:      proto.Uint64(pc.consumerID),
			ConsumerName:    proto.String(pc.options.Name),
			InitialPosition: position.Enum(),
			Schema:          nil,
		})

	if err != nil {
		pc.log.WithError(err).Error("Failed to create consumer")
		return err
	}

	if res.Response.ConsumerStatsResponse != nil {
		pc.consumerName = res.Response.ConsumerStatsResponse.ConsumerName
	}

	pc.cnx = res.Cnx
	pc.log.WithField("cnx", res.Cnx).Debug("Connected consumer")
	pc.cnx.AddConsumeHandler(pc.consumerID, pc)

	msgType := res.Response.GetType()

	switch msgType {
	case pb.BaseCommand_SUCCESS:
		return pc.internalFlow(uint32(pc.options.ReceiverQueueSize/2))
	case pb.BaseCommand_ERROR:
		errMsg := res.Response.GetError()
		return fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())
	default:
		return util.NewUnexpectedErrMsg(msgType, requestID)
	}
}

func (pc *partitionConsumer) Topic() string {
	return pc.topic
}

func (pc *partitionConsumer) Subscription() string {
	return pc.options.SubscriptionName
}

func (pc *partitionConsumer) Unsubscribe() error {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	hu := &handleUnsubscribe{
		waitGroup: wg,
		err:       nil,
	}
	pc.eventsChan <- hu

	wg.Wait()
	return hu.err
}

func (pc *partitionConsumer) internalUnsubscribe(unsub *handleUnsubscribe) {
	requestID := pc.client.rpcClient.NewRequestID()
	_, err := pc.client.rpcClient.RequestOnCnx(pc.cnx, requestID,
		pb.BaseCommand_UNSUBSCRIBE, &pb.CommandUnsubscribe{
			RequestId:  proto.Uint64(requestID),
			ConsumerId: proto.Uint64(pc.consumerID),
		})
	if err != nil {
		pc.log.WithError(err).Error("Failed to unsubscribe consumer")
		unsub.err = err
	}

	pc.cnx.DeleteConsumeHandler(pc.consumerID)
	if pc.unAckTracker != nil {
		pc.unAckTracker.Stop()
	}

	unsub.waitGroup.Done()
}

func (pc *partitionConsumer) trackMessage(msgID MessageID) error {
	id := &pb.MessageIdData{}
	err := proto.Unmarshal(msgID.Serialize(), id)
	if err != nil {
		pc.log.WithError(err).Errorf("unserialize message id error:%s", err.Error())
		return err
	}
	if pc.unAckTracker != nil {
		pc.unAckTracker.Add(id)
	}
	return nil
}

func (pc *partitionConsumer) increaseAvailablePermits() error {
	atomic.AddUint32(&pc.receivedSinceFlow, 1)
	highWater := uint32(math.Max(float64(cap(pc.subQueue)/2), 1))
	pc.log.Debugf("receivedSinceFlow size is: %d, highWater size is: %d", pc.receivedSinceFlow, highWater)
	// send flow request after 1/2 of the queue has been consumed
	if pc.receivedSinceFlow >= highWater {
		pc.log.Debugf("send flow command to broker, permits size is: %d", pc.receivedSinceFlow)
		err := pc.internalFlow(highWater)
		if err != nil {
			pc.log.Errorf("Send flow cmd error:%s", err.Error())
			atomic.SwapUint32(&pc.receivedSinceFlow, 0)
			return err
		}
		atomic.SwapUint32(&pc.receivedSinceFlow, 0)
	}
	return nil
}

func (pc *partitionConsumer) messageProcessed(msgID MessageID) error {
	err := pc.trackMessage(msgID)
	if err != nil {
		return err
	}

	err = pc.increaseAvailablePermits()
	if err != nil {
		return err
	}

	return nil
}

func (pc *partitionConsumer) Receive(ctx context.Context) (message Message, err error) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	pc.ReceiveAsyncWithCallback(ctx, func(msg Message, e error) {
		message = msg
		err = e
		wg.Done()
	})
	wg.Wait()

	return message, err
}

func (pc *partitionConsumer) ReceiveAsync(ctx context.Context, msgs chan<- ConsumerMessage) error {
	// Send flow request after 1/2 of the queue
	// has been consumed
	highWater := uint32(cap(pc.subQueue)) / 2

	drain := func() {
		for {
			select {
			case tmpMsg := <-pc.subQueue:
				msgs <- tmpMsg
			default:
				return
			}
		}
	}

CONSUMER:
	for {
		// ensure that the message queue is empty
		drain()

		// request half the buffer's capacity
		if err := pc.internalFlow(highWater); err != nil {
			continue CONSUMER
		}

		for {
			select {
			case tmpMsg, ok := <-pc.subQueue:
				if ok {
					msgs <- tmpMsg

					err := pc.messageProcessed(tmpMsg.ID())
					if err != nil {
						continue CONSUMER
					}

					continue
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

}

func (pc *partitionConsumer) ReceiveAsyncWithCallback(ctx context.Context, callback func(msg Message, err error)) {
	var err error

	select {
	case tmpMsg, ok := <-pc.subQueue:
		if ok {
			err = pc.messageProcessed(tmpMsg.ID())
			callback(tmpMsg.Message, err)
			if err != nil {
				pc.log.Errorf("processed messages error:%s", err.Error())
				return
			}
		}
	case <-ctx.Done():
		pc.log.Errorf("context shouldn't done, please check error:%s", ctx.Err().Error())
		return
	}
}

func (pc *partitionConsumer) Ack(msg Message) error {
	return pc.AckID(msg.ID())
}

func (pc *partitionConsumer) AckID(msgID MessageID) error {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	ha := &handleAck{
		msgID:     msgID,
		waitGroup: wg,
		err:       nil,
	}
	pc.eventsChan <- ha
	wg.Wait()
	return ha.err
}

func (pc *partitionConsumer) internalAck(ack *handleAck) {
	id := &pb.MessageIdData{}
	messageIDs := make([]*pb.MessageIdData, 0)
	err := proto.Unmarshal(ack.msgID.Serialize(), id)
	if err != nil {
		pc.log.WithError(err).Errorf("unserialize message id error:%s", err.Error())
		ack.err = err
	}

	messageIDs = append(messageIDs, id)

	requestID := pc.client.rpcClient.NewRequestID()
	_, err = pc.client.rpcClient.RequestOnCnxNoWait(pc.cnx, requestID,
		pb.BaseCommand_ACK, &pb.CommandAck{
			ConsumerId: proto.Uint64(pc.consumerID),
			MessageId:  messageIDs,
			AckType:    pb.CommandAck_Individual.Enum(),
		})
	if err != nil {
		pc.log.WithError(err).Error("Failed to unsubscribe consumer")
		ack.err = err
	}

	if pc.unAckTracker != nil {
		pc.unAckTracker.Remove(id)
	}
	ack.waitGroup.Done()
}

func (pc *partitionConsumer) AckCumulative(msg Message) error {
	return pc.AckCumulativeID(msg.ID())
}

func (pc *partitionConsumer) AckCumulativeID(msgID MessageID) error {
	hac := &handleAckCumulative{
		msgID: msgID,
		err:   nil,
	}
	pc.eventsChan <- hac

	return hac.err
}

func (pc *partitionConsumer) internalAckCumulative(ackCumulative *handleAckCumulative) {
	id := &pb.MessageIdData{}
	messageIDs := make([]*pb.MessageIdData, 0)
	err := proto.Unmarshal(ackCumulative.msgID.Serialize(), id)
	if err != nil {
		pc.log.WithError(err).Errorf("unserialize message id error:%s", err.Error())
		ackCumulative.err = err
	}
	messageIDs = append(messageIDs, id)

	requestID := pc.client.rpcClient.NewRequestID()
	_, err = pc.client.rpcClient.RequestOnCnx(pc.cnx, requestID,
		pb.BaseCommand_ACK, &pb.CommandAck{
			ConsumerId: proto.Uint64(pc.consumerID),
			MessageId:  messageIDs,
			AckType:    pb.CommandAck_Cumulative.Enum(),
		})
	if err != nil {
		pc.log.WithError(err).Error("Failed to unsubscribe consumer")
		ackCumulative.err = err
	}

	if pc.unAckTracker != nil {
		pc.unAckTracker.Remove(id)
	}
}

func (pc *partitionConsumer) Close() error {
	if pc.state != consumerReady {
		return nil
	}
	if pc.unAckTracker != nil {
		pc.unAckTracker.Stop()
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	cc := &handlerClose{&wg, nil}
	pc.eventsChan <- cc

	wg.Wait()
	return cc.err
}

func (pc *partitionConsumer) Seek(msgID MessageID) error {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	hc := &handleSeek{
		msgID:     msgID,
		waitGroup: wg,
		err:       nil,
	}
	pc.eventsChan <- hc

	wg.Wait()
	return hc.err
}

func (pc *partitionConsumer) internalSeek(seek *handleSeek) {
	if pc.state == consumerClosing || pc.state == consumerClosed {
		pc.log.Error("Consumer was already closed")
		return
	}

	id := &pb.MessageIdData{}
	err := proto.Unmarshal(seek.msgID.Serialize(), id)
	if err != nil {
		pc.log.WithError(err).Errorf("unserialize message id error:%s", err.Error())
		seek.err = err
	}

	requestID := pc.client.rpcClient.NewRequestID()
	_, err = pc.client.rpcClient.RequestOnCnx(pc.cnx, requestID,
		pb.BaseCommand_SEEK, &pb.CommandSeek{
			ConsumerId: proto.Uint64(pc.consumerID),
			RequestId:  proto.Uint64(requestID),
			MessageId:  id,
		})
	if err != nil {
		pc.log.WithError(err).Error("Failed to unsubscribe consumer")
		seek.err = err
	}

	seek.waitGroup.Done()
}

func (pc *partitionConsumer) RedeliverUnackedMessages() error {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	hr := &handleRedeliver{
		waitGroup: wg,
		err:       nil,
	}
	pc.eventsChan <- hr
	wg.Wait()
	return hr.err
}

func (pc *partitionConsumer) internalRedeliver(redeliver *handleRedeliver) {
	pc.omu.Lock()
	defer pc.omu.Unlock()

	redeliverMessagesSize := len(pc.redeliverMessages)

	if redeliverMessagesSize == 0 {
		return
	}

	requestID := pc.client.rpcClient.NewRequestID()

	for i := 0; i < len(pc.redeliverMessages); i += maxRedeliverUnacknowledged {
		end := i + maxRedeliverUnacknowledged
		if end > redeliverMessagesSize {
			end = redeliverMessagesSize
		}
		_, err := pc.client.rpcClient.RequestOnCnxNoWait(pc.cnx, requestID,
			pb.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES, &pb.CommandRedeliverUnacknowledgedMessages{
				ConsumerId: proto.Uint64(pc.consumerID),
				MessageIds: pc.redeliverMessages[i:end],
			})
		if err != nil {
			pc.log.WithError(err).Error("Failed to unsubscribe consumer")
			redeliver.err = err
		}
	}

	// clear redeliverMessages slice
	pc.redeliverMessages = nil

	if pc.unAckTracker != nil {
		pc.unAckTracker.clear()
	}
	redeliver.waitGroup.Done()
}

func (pc *partitionConsumer) runEventsLoop() {
	for {
		select {
		case i := <-pc.eventsChan:
			switch v := i.(type) {
			case *handlerClose:
				pc.internalClose(v)
				return
			case *handleSeek:
				pc.internalSeek(v)
			case *handleUnsubscribe:
				pc.internalUnsubscribe(v)
			case *handleAckCumulative:
				pc.internalAckCumulative(v)
			case *handleAck:
				pc.internalAck(v)
			case *handleRedeliver:
				pc.internalRedeliver(v)
			case *handleConnectionClosed:
				pc.reconnectToBroker()
			}
		}
	}
}

func (pc *partitionConsumer) internalClose(req *handlerClose) {
	if pc.state != consumerReady {
		req.waitGroup.Done()
		return
	}

	pc.state = consumerClosing
	pc.log.Info("Closing consumer")

	requestID := pc.client.rpcClient.NewRequestID()
	_, err := pc.client.rpcClient.RequestOnCnxNoWait(pc.cnx, requestID, pb.BaseCommand_CLOSE_CONSUMER, &pb.CommandCloseConsumer{
		ConsumerId: proto.Uint64(pc.consumerID),
		RequestId:  proto.Uint64(requestID),
	})
	pc.cnx.DeleteConsumeHandler(pc.consumerID)

	if err != nil {
		req.err = err
	} else {
		pc.log.Info("Closed consumer")
		pc.state = consumerClosed
		close(pc.options.MessageChannel)
	}

	req.waitGroup.Done()
}

// Flow command gives additional permits to send messages to the consumer.
// A typical consumer implementation will use a queue to accuMulate these messages
// before the application is ready to consume them. After the consumer is ready,
// the client needs to give permission to the broker to push messages.
func (pc *partitionConsumer) internalFlow(permits uint32) error {
	if permits <= 0 {
		return fmt.Errorf("invalid number of permits requested: %d", permits)
	}

	requestID := pc.client.rpcClient.NewRequestID()
	pc.log.Debugf("send flow cmd to consumer: [%d], permits size: [%d]", pc.consumerID, permits)
	_, err := pc.client.rpcClient.RequestOnCnxNoWait(pc.cnx, requestID,
		pb.BaseCommand_FLOW, &pb.CommandFlow{
			ConsumerId:     proto.Uint64(pc.consumerID),
			MessagePermits: proto.Uint32(permits),
		})

	if err != nil {
		pc.log.WithError(err).Error("Failed to unsubscribe consumer")
		return err
	}
	return nil
}

func (pc *partitionConsumer) MessageReceived(response *pb.CommandMessage, headersAndPayload []byte) error {
	msgID := response.GetMessageId()

	id := newMessageID(int64(msgID.GetLedgerId()), int64(msgID.GetEntryId()),
		int(msgID.GetBatchIndex()), pc.partitionIdx)

	msgMeta, payloadList, err := internal.ParseMessage(headersAndPayload)
	if err != nil {
		return fmt.Errorf("parse message error:%s", err)
	}

	for _, payload := range payloadList {
		msg := &message{
			publishTime: timeFromUnixTimestampMillis(msgMeta.GetPublishTime()),
			eventTime:   timeFromUnixTimestampMillis(msgMeta.GetEventTime()),
			key:         msgMeta.GetPartitionKey(),
			properties:  internal.ConvertToStringMap(msgMeta.GetProperties()),
			topic:       pc.topic,
			msgID:       id,
			payLoad:     payload,
		}

		consumerMsg := ConsumerMessage{
			Message:  msg,
			Consumer: pc,
		}

		pc.log.Debugf("receive message form broker, payload is:%s", string(payload))
		select {
		case pc.subQueue <- consumerMsg:
			pc.log.Infof("sub queue size is: %d, consumerID: {%d}\n", len(pc.subQueue), pc.consumerID)
			return nil
		}
	}

	return nil
}

type handleAck struct {
	msgID     MessageID
	waitGroup *sync.WaitGroup
	err       error
}

type handleAckCumulative struct {
	msgID MessageID
	err   error
}

type handleUnsubscribe struct {
	waitGroup *sync.WaitGroup
	err       error
}

type handleSeek struct {
	msgID     MessageID
	waitGroup *sync.WaitGroup
	err       error
}

type handleRedeliver struct {
	waitGroup *sync.WaitGroup
	err       error
}

type handlerClose struct {
	waitGroup *sync.WaitGroup
	err       error
}

type handleConnectionClosed struct{}

func (pc *partitionConsumer) ConnectionClosed() {
	// Trigger reconnection in the consumer goroutine
	pc.eventsChan <- &handleConnectionClosed{}
}

func (pc *partitionConsumer) reconnectToBroker() {
	pc.log.Info("Reconnecting to broker")
	backoff := new(internal.Backoff)
	for {
		if pc.state != consumerReady {
			// Consumer is already closing
			return
		}

		err := pc.grabCnx()
		if err == nil {
			// Successfully reconnected
			pc.log.Info("Successfully reconnected")
			return
		}

		d := backoff.Next()
		pc.log.Info("Retrying reconnection after ", d)

		time.Sleep(d)
	}
}
