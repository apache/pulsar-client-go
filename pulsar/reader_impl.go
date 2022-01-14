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

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

const (
	defaultReceiverQueueSize = 1000
)

type reader struct {
	sync.Mutex
	topic               string
	client              *client
	options             ReaderOptions
	consumers           []*partitionConsumer
	messageCh           chan ConsumerMessage
	lastMessageInBroker trackingMessageID
	dlq                 *dlqRouter
	log                 log.Logger
	metrics             *internal.LeveledMetrics
	stopDiscovery       func()
	closeOnce           sync.Once
}

func newReader(client *client, options ReaderOptions) (Reader, error) {
	if options.Topic == "" {
		return nil, newError(InvalidConfiguration, "Topic is required")
	}

	if options.StartMessageID == nil {
		return nil, newError(InvalidConfiguration, "StartMessageID is required")
	}

	if options.ReceiverQueueSize <= 0 {
		options.ReceiverQueueSize = defaultReceiverQueueSize
	}

	// Provide dummy dlq router with not dlq policy
	dlq, err := newDlqRouter(client, nil, client.log)
	if err != nil {
		return nil, err
	}

	reader := &reader{
		topic:     options.Topic,
		client:    client,
		options:   options,
		messageCh: make(chan ConsumerMessage),
		dlq:       dlq,
		metrics:   client.metrics.GetLeveledMetrics(options.Topic),
		log:       client.log.SubLogger(log.Fields{"topic": options.Topic}),
	}

	if err := reader.internalTopicReadToPartitions(); err != nil {
		return nil, err
	}

	reader.stopDiscovery = reader.runBackgroundPartitionDiscovery(time.Second * 60)

	reader.metrics.ReadersOpened.Inc()
	return reader, nil
}

func (r *reader) Topic() string {
	return r.topic
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
			if mid, ok := toTrackingMessageID(msgID); ok {
				r.consumers[mid.partitionIdx].lastDequeuedMsg = mid
				r.consumers[mid.partitionIdx].AckID(mid)
				return cm.Message, nil
			}
			return nil, newError(InvalidMessage, fmt.Sprintf("invalid message id type %T", msgID))
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (r *reader) HasNext() bool {
	if !r.lastMessageInBroker.Undefined() && r.hasMoreMessages() {
		return true
	}

retryLoop:
	for {
	consumerLoop:
		for _, consumer := range r.consumers {
			lastMsgID, err := consumer.getLastMessageID()
			if err != nil {
				r.log.WithError(err).Error("Failed to get last message id from broker")
				continue retryLoop
			}
			if r.lastMessageInBroker.greater(lastMsgID.messageID) {
				continue consumerLoop
			}
			r.lastMessageInBroker = lastMsgID
		}
		break retryLoop
	}

	return r.hasMoreMessages()
}

func (r *reader) hasMoreMessages() bool {
	for _, c := range r.consumers {
		if r.consumerHasMoreMessages(c) {
			return true
		}
	}
	return false
}

func (r *reader) consumerHasMoreMessages(pc *partitionConsumer) bool {
	if !pc.lastDequeuedMsg.Undefined() {
		return r.lastMessageInBroker.isEntryIDValid() && r.lastMessageInBroker.greater(pc.lastDequeuedMsg.messageID)
	}

	if pc.options.startMessageIDInclusive {
		return r.lastMessageInBroker.isEntryIDValid() && r.lastMessageInBroker.greaterEqual(pc.startMessageID.messageID)
	}

	// Non-inclusive
	return r.lastMessageInBroker.isEntryIDValid() && r.lastMessageInBroker.greater(pc.startMessageID.messageID)
}

func (r *reader) Close() {
	r.closeOnce.Do(func() {
		r.stopDiscovery()

		r.Lock()
		defer r.Unlock()

		for _, consumer := range r.consumers {
			if consumer != nil {
				consumer.Close()
			}
		}
		r.dlq.close()
		r.client.handlers.Del(r)
		r.metrics.ReadersClosed.Inc()
	})
}

func (r *reader) messageID(msgID MessageID) (trackingMessageID, bool) {
	mid, ok := toTrackingMessageID(msgID)
	if !ok {
		r.log.Warnf("invalid message id type %T", msgID)
		return trackingMessageID{}, false
	}

	partition := int(mid.partitionIdx)
	// did we receive a valid partition index?
	if partition < 0 {
		r.log.Warnf("invalid partition index %d expected", partition)
		return trackingMessageID{}, false
	}

	return mid, true
}

func (r *reader) Seek(msgID MessageID) error {
	r.Lock()
	defer r.Unlock()

	mid, ok := r.messageID(msgID)
	if !ok {
		return nil
	}

	return r.consumers[mid.partitionIdx].Seek(mid)
}

func (r *reader) SeekByTime(time time.Time) error {
	r.Lock()
	defer r.Unlock()

	if len(r.consumers) > 1 {
		return newError(SeekFailed, "for partition topic, seek command should perform on the individual partitions")
	}
	return r.consumers[0].SeekByTime(time)
}

func (r *reader) runBackgroundPartitionDiscovery(period time.Duration) (cancel func()) {
	var wg sync.WaitGroup
	stopDiscoveryCh := make(chan struct{})
	ticker := time.NewTicker(period)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopDiscoveryCh:
				return
			case <-ticker.C:
				r.log.Debug("Auto discovering new partitions")
				r.internalTopicReadToPartitions()
			}
		}
	}()

	return func() {
		ticker.Stop()
		close(stopDiscoveryCh)
		wg.Wait()
	}
}
