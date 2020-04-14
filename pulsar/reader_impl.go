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
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	defaultReceiverQueueSize = 1000
)

type reader struct {
	pc                  *partitionConsumer
	messageCh           chan ConsumerMessage
	lastMessageInBroker *messageID

	log *log.Entry
}

func newReader(client *client, options ReaderOptions) (Reader, error) {
	if options.Topic == "" {
		return nil, newError(ResultInvalidConfiguration, "Topic is required")
	}

	if options.StartMessageID == nil {
		return nil, newError(ResultInvalidConfiguration, "StartMessageID is required")
	}

	subscriptionName := options.SubscriptionRolePrefix
	if subscriptionName == "" {
		subscriptionName = "reader"
	}
	subscriptionName += "-" + generateRandomName()

	receiverQueueSize := options.ReceiverQueueSize
	if receiverQueueSize == 0 {
		receiverQueueSize = defaultReceiverQueueSize
	}

	consumerOptions := &partitionConsumerOpts{
		topic:                      options.Topic,
		consumerName:               options.Name,
		subscription:               subscriptionName,
		subscriptionType:           Exclusive,
		receiverQueueSize:          receiverQueueSize,
		startMessageID:             options.StartMessageID.(*messageID),
		startMessageIDInclusive:    options.StartMessageIDInclusive,
		subscriptionMode:           nonDurable,
		readCompacted:              options.ReadCompacted,
		metadata:                   options.Properties,
		nackRedeliveryDelay:        defaultNackRedeliveryDelay,
		replicateSubscriptionState: false,
	}

	reader := &reader{
		messageCh: make(chan ConsumerMessage),
		log:       log.WithField("topic", options.Topic),
	}

	// Provide dummy dlq router with not dlq policy
	dlq, err := newDlqRouter(client, nil)
	if err != nil {
		return nil, err
	}
	pc, err := newPartitionConsumer(nil, client, consumerOptions, reader.messageCh, dlq)

	if err != nil {
		close(reader.messageCh)
		return nil, err
	}

	reader.pc = pc
	return reader, nil
}

func (r *reader) Topic() string {
	return r.pc.topic
}

func (r *reader) Next(ctx context.Context) (Message, error) {
	for {
		select {
		case cm, ok := <-r.messageCh:
			if !ok {
				return nil, ErrConsumerClosed
			}

			// Acknowledge message immediately because the reader is based on non-durable subscription. When it reconnects,
			// it will specify the subscription position anyway
			msgID := cm.Message.ID().(*messageID)
			r.pc.lastDequeuedMsg = msgID
			r.pc.AckID(msgID)
			return cm.Message, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (r *reader) HasNext() bool {
	if r.lastMessageInBroker != nil && r.hasMoreMessages() {
		return true
	}

	for {
		lastMsgID, err := r.pc.getLastMessageID()
		if err != nil {
			r.log.WithError(err).Error("Failed to get last message id from broker")
			continue
		} else {
			r.lastMessageInBroker = lastMsgID
			break
		}
	}

	return r.hasMoreMessages()
}

func (r *reader) hasMoreMessages() bool {
	if r.pc.lastDequeuedMsg != nil {
		return r.lastMessageInBroker.greater(r.pc.lastDequeuedMsg)
	}

	if r.pc.options.startMessageIDInclusive {
		return r.lastMessageInBroker.greaterEqual(r.pc.startMessageID)
	}

	// Non-inclusive
	return r.lastMessageInBroker.greater(r.pc.startMessageID)
}

func (r *reader) Close() {
	r.pc.Close()
}

func (r *reader) messageID(msgID MessageID) (*messageID, bool) {
	mid, ok := msgID.(*messageID)
	if !ok {
		r.log.Warnf("invalid message id type")
		return nil, false
	}

	partition := mid.partitionIdx
	// did we receive a valid partition index?
	if partition != 0 {
		r.log.Warnf("invalid partition index %d expected 0", partition)
		return nil, false
	}

	return mid, true
}

func (r *reader) Seek(msgID MessageID) error {
	mid, ok := r.messageID(msgID)
	if !ok {
		return nil
	}

	return r.pc.Seek(mid)
}

func (r *reader) SeekByTime(time time.Time) error {
	return r.pc.SeekByTime(time)
}
