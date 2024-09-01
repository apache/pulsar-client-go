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

	"github.com/pkg/errors"

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

const (
	defaultReceiverQueueSize = 1000
)

type reader struct {
	sync.Mutex
	client    *client
	messageCh chan ConsumerMessage
	log       log.Logger
	metrics   *internal.LeveledMetrics
	c         *consumer
}

func newReader(client *client, options ReaderOptions) (Reader, error) {
	if options.Topic == "" {
		return nil, newError(InvalidConfiguration, "Topic is required")
	}

	if options.StartMessageID == nil {
		return nil, newError(InvalidConfiguration, "StartMessageID is required")
	}

	var startMessageID *trackingMessageID
	if !checkMessageIDType(options.StartMessageID) {
		// a custom type satisfying MessageID may not be a messageID or trackingMessageID
		// so re-create messageID using its data
		deserMsgID, err := deserializeMessageID(options.StartMessageID.Serialize())
		if err != nil {
			return nil, err
		}
		// de-serialized MessageID is a messageID
		startMessageID = toTrackingMessageID(deserMsgID)
	} else {
		startMessageID = toTrackingMessageID(options.StartMessageID)
	}

	subscriptionName := options.SubscriptionName
	if subscriptionName == "" {
		subscriptionName = options.SubscriptionRolePrefix
		if subscriptionName == "" {
			subscriptionName = "reader"
		}
		subscriptionName += "-" + generateRandomName()
	}

	receiverQueueSize := options.ReceiverQueueSize
	if receiverQueueSize <= 0 {
		receiverQueueSize = defaultReceiverQueueSize
	}

	// decryption is enabled, use default message crypto if not provided
	if options.Decryption != nil && options.Decryption.MessageCrypto == nil {
		messageCrypto, err := crypto.NewDefaultMessageCrypto("decrypt",
			false,
			client.log.SubLogger(log.Fields{"topic": options.Topic}))
		if err != nil {
			return nil, err
		}
		options.Decryption.MessageCrypto = messageCrypto
	}

	if options.MaxPendingChunkedMessage == 0 {
		options.MaxPendingChunkedMessage = 100
	}

	if options.ExpireTimeOfIncompleteChunk == 0 {
		options.ExpireTimeOfIncompleteChunk = time.Minute
	}

	consumerOptions := &ConsumerOptions{
		Topic:                       options.Topic,
		Name:                        options.Name,
		SubscriptionName:            subscriptionName,
		Type:                        Exclusive,
		ReceiverQueueSize:           receiverQueueSize,
		SubscriptionMode:            NonDurable,
		ReadCompacted:               options.ReadCompacted,
		Properties:                  options.Properties,
		NackRedeliveryDelay:         defaultNackRedeliveryDelay,
		ReplicateSubscriptionState:  false,
		Decryption:                  options.Decryption,
		Schema:                      options.Schema,
		BackoffPolicy:               options.BackoffPolicy,
		MaxPendingChunkedMessage:    options.MaxPendingChunkedMessage,
		ExpireTimeOfIncompleteChunk: options.ExpireTimeOfIncompleteChunk,
		AutoAckIncompleteChunk:      options.AutoAckIncompleteChunk,
		startMessageID:              startMessageID,
		StartMessageIDInclusive:     options.StartMessageIDInclusive,
	}

	reader := &reader{
		client:    client,
		messageCh: make(chan ConsumerMessage),
		log:       client.log.SubLogger(log.Fields{"topic": options.Topic}),
		metrics:   client.metrics.GetLeveledMetrics(options.Topic),
	}

	// Provide dummy dlq router with not dlq policy
	dlq, err := newDlqRouter(client, nil, options.Topic, options.SubscriptionName, options.Name, client.log)
	if err != nil {
		return nil, err
	}
	// Provide dummy rlq router with not dlq policy
	rlq, err := newRetryRouter(client, nil, false, client.log)
	if err != nil {
		return nil, err
	}

	c, err := newInternalConsumer(client, *consumerOptions, options.Topic, reader.messageCh, dlq, rlq, false)
	if err != nil {
		close(reader.messageCh)
		return nil, err
	}
	cs, ok := c.(*consumer)
	if !ok {
		return nil, errors.New("invalid consumer type")
	}
	reader.c = cs

	reader.metrics.ReadersOpened.Inc()
	return reader, nil
}

func (r *reader) Topic() string {
	return r.c.topic
}

func (r *reader) Next(ctx context.Context) (Message, error) {
	for {
		select {
		case cm, ok := <-r.messageCh:
			if !ok {
				return nil, newError(ConsumerClosed, "consumer closed")
			}

			// Acknowledge message immediately because the reader is based on non-durable subscription. When it reconnects,
			// it will specify the subscription position anyway
			msgID := cm.Message.ID()
			err := r.c.setLastDequeuedMsg(msgID)
			if err != nil {
				return nil, err
			}
			err = r.c.AckID(msgID)
			if err != nil {
				return nil, err
			}
			return cm.Message, nil
		case <-r.c.closeCh:
			return nil, newError(ConsumerClosed, "consumer closed")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (r *reader) HasNext() bool {
	return r.c.hasNext()
}

func (r *reader) Close() {
	r.c.Close()
	r.client.handlers.Del(r)
	r.metrics.ReadersClosed.Inc()
}

func (r *reader) messageID(msgID MessageID) *trackingMessageID {
	mid := toTrackingMessageID(msgID)

	partition := int(mid.partitionIdx)
	// did we receive a valid partition index?
	if partition < 0 {
		r.log.Warnf("invalid partition index %d expected", partition)
		return nil
	}

	return mid
}

func (r *reader) Seek(msgID MessageID) error {
	r.Lock()
	defer r.Unlock()

	if !checkMessageIDType(msgID) {
		r.log.Warnf("invalid message id type %T", msgID)
		return fmt.Errorf("invalid message id type %T", msgID)
	}

	mid := r.messageID(msgID)
	if mid == nil {
		return nil
	}

	return r.c.Seek(mid)
}

func (r *reader) SeekByTime(time time.Time) error {
	r.Lock()
	defer r.Unlock()

	return r.c.SeekByTime(time)
}

func (r *reader) GetLastMessageID() (MessageID, error) {
	if len(r.c.consumers) > 1 {
		return nil, fmt.Errorf("GetLastMessageID is not supported for multi-topics reader")
	}
	return r.c.consumers[0].getLastMessageID()
}
