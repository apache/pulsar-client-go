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
	"sync"
	"time"

	uAtomic "go.uber.org/atomic"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/pkg/errors"
)

type zeroQueueConsumer struct {
	sync.Mutex
	topic                     string
	client                    *client
	options                   ConsumerOptions
	pc                        *partitionConsumer
	consumerName              string
	disableForceTopicCreation bool
	waitingOnReceive          uAtomic.Bool

	messageCh chan ConsumerMessage

	dlq       *dlqRouter
	rlq       *retryRouter
	closeOnce sync.Once
	closeCh   chan struct{}
	errorCh   chan error

	log     log.Logger
	metrics *internal.LeveledMetrics
}

func newZeroConsumer(client *client, options ConsumerOptions, topic string,
	messageCh chan ConsumerMessage, dlq *dlqRouter,
	rlq *retryRouter, disableForceTopicCreation bool) (*zeroQueueConsumer, error) {
	zc := &zeroQueueConsumer{
		topic:                     topic,
		client:                    client,
		options:                   options,
		disableForceTopicCreation: disableForceTopicCreation,
		messageCh:                 messageCh,
		closeCh:                   make(chan struct{}),
		errorCh:                   make(chan error),
		dlq:                       dlq,
		rlq:                       rlq,
		log:                       client.log.SubLogger(log.Fields{"topic": topic}),
		consumerName:              options.Name,
		metrics:                   client.metrics.GetLeveledMetrics(topic),
	}
	tn, err := internal.ParseTopicName(topic)
	if err != nil {
		return nil, err
	}
	opts := newPartitionConsumerOpts(zc.topic, zc.consumerName, tn.Partition, zc.options)
	opts.zeroQueueReconnectedPolicy = func(pc *partitionConsumer) {
		if zc.waitingOnReceive.Load() {
			pc.log.Info("zeroQueueConsumer reconnect, reset availablePermits")
			pc.availablePermits.inc()
		}
	}
	pc, err := newPartitionConsumer(zc, zc.client, opts, zc.messageCh, zc.dlq, zc.metrics)
	if err != nil {
		return nil, err
	}
	zc.pc = pc

	return zc, nil
}

func (z *zeroQueueConsumer) Subscription() string {
	return z.options.SubscriptionName
}

func (z *zeroQueueConsumer) Unsubscribe() error {
	return z.unsubscribe(false)
}

func (z *zeroQueueConsumer) UnsubscribeForce() error {
	return z.unsubscribe(true)
}

func (z *zeroQueueConsumer) unsubscribe(force bool) error {
	z.Lock()
	defer z.Unlock()

	if err := z.pc.unsubscribe(force); err != nil {
		return errors.Errorf("topic %s, subscription %s: %v", z.topic, z.Subscription(), err)
	}

	return nil
}

func (z *zeroQueueConsumer) GetLastMessageIDs() ([]TopicMessageID, error) {
	id, err := z.pc.getLastMessageID()
	if err != nil {
		return nil, err
	}
	tm := &topicMessageID{topic: z.pc.topic, track: id}
	return []TopicMessageID{tm}, nil
}

func (z *zeroQueueConsumer) Receive(ctx context.Context) (Message, error) {
	if state := z.pc.getConsumerState(); state == consumerClosed || state == consumerClosing {
		z.log.WithField("state", state).Error("Failed to ack by closing or closed consumer")
		return nil, errors.New("consumer state is closed")
	}
	z.Lock()
	defer z.Unlock()
	z.waitingOnReceive.Store(true)
	z.pc.availablePermits.inc()
	for {
		select {
		case <-z.closeCh:
			z.waitingOnReceive.Store(false)
			return nil, newError(ConsumerClosed, "consumer closed")
		case cm, ok := <-z.messageCh:
			if !ok {
				return nil, newError(ConsumerClosed, "consumer closed")
			}
			message, ok := cm.Message.(*message)
			if ok && message.getConn().ID() == z.pc._getConn().ID() {
				z.waitingOnReceive.Store(false)
				return cm.Message, nil
			} else {
				z.log.WithField("messageID", cm.Message.ID()).Warn("message from old connection discarded after reconnection")
			}
		case <-ctx.Done():
			z.waitingOnReceive.Store(false)
			return nil, ctx.Err()
		}
	}

}

func (z *zeroQueueConsumer) Chan() <-chan ConsumerMessage {
	panic("zeroQueueConsumer cannot support Chan method")
}

func (z *zeroQueueConsumer) Ack(m Message) error {
	return z.AckID(m.ID())
}

func (z *zeroQueueConsumer) checkMsgIDPartition(msgID MessageID) error {
	partition := msgID.PartitionIdx()
	if partition == 0 || partition == -1 {
		return nil
	}
	if partition != z.pc.partitionIdx {
		z.log.Errorf("invalid partition index %d expected a partition equal to %d",
			partition, z.pc.partitionIdx)
		return fmt.Errorf("invalid partition index %d expected a partition equal to %d",
			partition, z.pc.partitionIdx)
	}
	return nil
}

func (z *zeroQueueConsumer) messageID(msgID MessageID) *trackingMessageID {
	if err := z.checkMsgIDPartition(msgID); err != nil {
		return nil
	}
	mid := toTrackingMessageID(msgID)
	return mid
}

func (z *zeroQueueConsumer) AckID(msgID MessageID) error {
	if err := z.checkMsgIDPartition(msgID); err != nil {
		return err
	}

	if z.options.AckWithResponse {
		return z.pc.AckIDWithResponse(msgID)
	}

	return z.pc.AckID(msgID)
}

func (z *zeroQueueConsumer) AckIDList(msgIDs []MessageID) error {
	return z.pc.AckIDList(msgIDs)
}

func (z *zeroQueueConsumer) AckWithTxn(msg Message, txn Transaction) error {
	msgID := msg.ID()
	if err := z.checkMsgIDPartition(msgID); err != nil {
		return err
	}

	return z.pc.AckIDWithTxn(msgID, txn)
}

func (z *zeroQueueConsumer) AckCumulative(msg Message) error {
	return z.AckIDCumulative(msg.ID())
}

func (z *zeroQueueConsumer) AckIDCumulative(msgID MessageID) error {
	if err := z.checkMsgIDPartition(msgID); err != nil {
		return err
	}
	if z.options.AckWithResponse {
		return z.pc.AckIDWithResponseCumulative(msgID)
	}
	return z.pc.AckIDCumulative(msgID)
}

func (z *zeroQueueConsumer) ReconsumeLater(_ Message, _ time.Duration) {
	z.log.Warnf("zeroQueueConsumer not support ReconsumeLater yet.")
}

func (z *zeroQueueConsumer) ReconsumeLaterWithCustomProperties(_ Message, _ map[string]string, _ time.Duration) {
	z.log.Warnf("zeroQueueConsumer not support ReconsumeLaterWithCustomProperties yet.")
}

func (z *zeroQueueConsumer) Nack(msg Message) {
	if !checkMessageIDType(msg.ID()) {
		z.log.Warnf("invalid message id type %T", msg.ID())
		return
	}
	if z.options.EnableDefaultNackBackoffPolicy || z.options.NackBackoffPolicy != nil {
		mid := z.messageID(msg.ID())
		if mid == nil {
			return
		}

		if mid.consumer != nil {
			mid.NackByMsg(msg)
			return
		}
		z.pc.NackMsg(msg)
		return
	}

	z.NackID(msg.ID())
}

func (z *zeroQueueConsumer) NackID(msgID MessageID) {
	if err := z.checkMsgIDPartition(msgID); err != nil {
		return
	}
	z.pc.NackID(msgID)
}

func (z *zeroQueueConsumer) Close() {
	z.closeOnce.Do(func() {
		z.Lock()
		defer z.Unlock()

		z.pc.Close()
		close(z.closeCh)
		z.client.handlers.Del(z)
		z.dlq.close()
		z.rlq.close()
		z.metrics.ConsumersClosed.Inc()
		z.metrics.ConsumersPartitions.Sub(float64(1))
	})
}

func (z *zeroQueueConsumer) Seek(msgID MessageID) error {
	z.Lock()
	defer z.Unlock()

	if err := z.checkMsgIDPartition(msgID); err != nil {
		return err
	}

	if err := z.pc.Seek(msgID); err != nil {
		return err
	}

	// clear messageCh
	for len(z.messageCh) > 0 {
		<-z.messageCh
	}

	return nil
}

func (z *zeroQueueConsumer) SeekByTime(time time.Time) error {
	z.Lock()
	defer z.Unlock()
	var errs error

	if err := z.pc.SeekByTime(time); err != nil {
		msg := fmt.Sprintf("unable to SeekByTime for topic=%s subscription=%s", z.topic, z.Subscription())
		errs = errors.Wrap(newError(SeekFailed, err.Error()), msg)
	}

	// clear messageCh
	for len(z.messageCh) > 0 {
		<-z.messageCh
	}

	return errs
}

func (z *zeroQueueConsumer) Name() string {
	return z.consumerName
}
