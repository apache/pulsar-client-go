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
)

const (
	defaultReceiverQueueSize = 1000
)

type reader struct {
	pc        *partitionConsumer
	messageCh chan ConsumerMessage
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
	}

	pc, err := newPartitionConsumer(nil, client, consumerOptions, reader.messageCh)

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
			r.pc.AckID(cm.Message.ID().(*messageID))
			return cm.Message, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (r *reader) HasNext() bool {
	return false
}

func (r *reader) Close() {
	r.pc.Close()
}

//
//private boolean hasMoreMessages(MessageId lastMessageIdInBroker, MessageId lastDequeuedMessage) {
//if (lastMessageIdInBroker.compareTo(lastDequeuedMessage) > 0 &&
//((MessageIdImpl)lastMessageIdInBroker).getEntryId() != -1) {
//return true;
//} else {
//// Make sure batching message can be read completely.
//return lastMessageIdInBroker.compareTo(lastDequeuedMessage) == 0
//&& incomingMessages.size() > 0;
//}
//}

func (r *reader) hasMoreMessages(lastMessageInBroker, lastDequeuedMessage *messageID) bool {
	//if lastMessageInBroker.ledgerID > lastDequeuedMessage.ledgerID && lastMessageInBroker.entryID != -1 {
	return true
	//} else {
	//	// Make sure batching message can be read completely.
	//	return *lastMessageInBroker == *lastDequeuedMessage &&
	//
	//}
}
