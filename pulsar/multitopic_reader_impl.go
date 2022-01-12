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

	"time"

	"github.com/apache/pulsar-client-go/pulsar/log"
)

type multiTopicReader struct {
	consumer Consumer
	client   *client
	log      log.Logger
	topic    string
}

func newMultiTopicReader(client *client, options ReaderOptions) (Reader, error) {

	if options.Topic == "" {
		return nil, newError(InvalidConfiguration, "Topic is required")
	}

	if options.StartMessageID == nil {
		return nil, newError(InvalidConfiguration, "StartMessageID is required")
	}
	startMessageID, ok := toTrackingMessageID(options.StartMessageID)
	if !ok {
		// a custom type satisfying MessageID may not be a messageID or trackingMessageID
		// so re-create messageID using its data
		deserMsgID, err := deserializeMessageID(options.StartMessageID.Serialize())
		if err != nil {
			return nil, err
		}
		// de-serialized MessageID is a messageID
		startMessageID = trackingMessageID{
			messageID:    deserMsgID.(messageID),
			receivedTime: time.Now(),
		}
	}
	// only earliest/latest message id supported
	if !startMessageID.equal(latestMessageID) && !startMessageID.equal(earliestMessageID) {
		return nil, newError(OperationNotSupported, "The partitioned topic startMessageId is illegal")
	}

	subscriptionName := options.SubscriptionRolePrefix
	if subscriptionName == "" {
		subscriptionName = "multitopic-reader"
	}

	subscriptionName += "-" + generateRandomName()
	receiverQueueSize := options.ReceiverQueueSize
	if receiverQueueSize <= 0 {
		receiverQueueSize = defaultReceiverQueueSize
	}

	consumerOptions := ConsumerOptions{
		Topic:                      options.Topic,
		Name:                       options.Name,
		SubscriptionName:           subscriptionName,
		ReceiverQueueSize:          receiverQueueSize,
		startMessageID:             startMessageID,
		Type:                       Exclusive,
		subscriptionMode:           nonDurable,
		startMessageIDInclusive:    options.StartMessageIDInclusive,
		ReadCompacted:              options.ReadCompacted,
		Properties:                 options.Properties,
		NackRedeliveryDelay:        defaultNackRedeliveryDelay,
		ReplicateSubscriptionState: false,
		Decryption:                 options.Decryption,
	}

	multiTopicReader := &multiTopicReader{
		client: client,
		log:    client.log.SubLogger(log.Fields{"topic": options.Topic}),
		topic:  options.Topic,
	}

	consumer, err := newConsumer(client, consumerOptions)
	if err != nil {
		return nil, err
	}
	multiTopicReader.consumer = consumer
	return multiTopicReader, nil
}

func (r *multiTopicReader) Topic() string {
	return r.topic
}

func (r *multiTopicReader) Next(ctx context.Context) (Message, error) {
	msg, err := r.consumer.Receive(ctx)
	if err != nil {
		return nil, err
	}

	if mid, ok := toTrackingMessageID(msg.ID()); ok {
		err = r.consumer.lastDequeuedMsg(mid)
		if err != nil {
			return nil, err
		}
		// Acknowledge message immediately because the reader is based on non-durable subscription. When it reconnects,
		// it will specify the subscription position anyway
		r.consumer.AckID(mid)
		return msg, nil
	}
	return nil, newError(InvalidMessage, fmt.Sprintf("invalid message id type %T", msg.ID()))
}

func (r *multiTopicReader) HasNext() bool {

	for {
		// messages are available in queue
		if r.consumer.messagesInQueue() > 0 {
			return true
		}

		// check if messages are available on broker
		hasMessages, err := r.consumer.hasMessages()
		if err != nil {
			r.log.WithError(err).Error("Failed to get last message id from broker")
			continue
		}
		return hasMessages
	}
}

func (r *multiTopicReader) Close() {
	r.consumer.Close()
}

func (r *multiTopicReader) Seek(msgID MessageID) error {
	return r.consumer.Seek(msgID)
}

func (r *multiTopicReader) SeekByTime(time time.Time) error {
	return r.consumer.SeekByTime(time)
}
