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
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/apache/pulsar-client-go/pkg/pb"
	"github.com/apache/pulsar-client-go/pulsar/internal"
)

var ErrConsumerClosed = errors.New("consumer closed")

const defaultNackRedeliveryDelay = 1 * time.Minute

type consumer struct {
	options ConsumerOptions

	consumers []*partitionConsumer

	// channel used to deliver message to clients
	messageCh chan ConsumerMessage

	closeCh chan struct{}
	errorCh chan error

	log *log.Entry
}

func newConsumer(client *client, options ConsumerOptions) (Consumer, error) {
	if options.Topic == "" && options.Topics == nil && options.TopicsPattern == "" {
		return nil, newError(TopicNotFound, "topic is required")
	}

	if options.SubscriptionName == "" {
		return nil, newError(SubscriptionNotFound, "subscription name is required for consumer")
	}

	if options.ReceiverQueueSize == 0 {
		options.ReceiverQueueSize = 1000
	}

	// did the user pass in a message channel?
	messageCh := options.MessageChannel
	if options.MessageChannel == nil {
		messageCh = make(chan ConsumerMessage, 10)
	}

	// single topic consumer
	if options.Topic != "" || len(options.Topics) == 1 {
		topic := options.Topic
		if topic == "" {
			topic = options.Topics[0]
		}

		if _, err := internal.ParseTopicName(topic); err != nil {
			return nil, err
		}

		return topicSubscribe(client, options, topic, messageCh)
	}

	return nil, newError(ResultInvalidTopicName, "topic name is required for consumer")
}

func topicSubscribe(client *client, options ConsumerOptions, topic string,
	messageCh chan ConsumerMessage) (Consumer, error) {
	consumer := &consumer{
		messageCh: messageCh,
		errorCh:   make(chan error),
		log:       log.WithField("topic", topic),
	}

	partitions, err := client.TopicPartitions(topic)
	if err != nil {
		return nil, err
	}

	numPartitions := len(partitions)
	consumer.consumers = make([]*partitionConsumer, numPartitions)

	type ConsumerError struct {
		err       error
		partition int
		consumer  *partitionConsumer
	}

	consumerName := options.Name
	if consumerName == "" {
		consumerName = generateRandomName()
	}

	receiverQueueSize := options.ReceiverQueueSize
	var wg sync.WaitGroup
	ch := make(chan ConsumerError, numPartitions)
	for partitionIdx, partitionTopic := range partitions {
		wg.Add(1)
		go func(idx int, pt string) {
			defer wg.Done()

			var nackRedeliveryDelay time.Duration
			if options.NackRedeliveryDelay == nil {
				nackRedeliveryDelay = defaultNackRedeliveryDelay
			} else {
				nackRedeliveryDelay = *options.NackRedeliveryDelay
			}
			opts := &partitionConsumerOpts{
				topic:               pt,
				consumerName:        consumerName,
				subscription:        options.SubscriptionName,
				subscriptionType:    options.Type,
				subscriptionInitPos: options.SubscriptionInitialPosition,
				partitionIdx:        idx,
				receiverQueueSize:   receiverQueueSize,
				nackRedeliveryDelay: nackRedeliveryDelay,
			}
			cons, err := newPartitionConsumer(consumer, client, opts, messageCh)
			ch <- ConsumerError{
				err:       err,
				partition: idx,
				consumer:  cons,
			}
		}(partitionIdx, partitionTopic)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for ce := range ch {
		if ce.err != nil {
			err = ce.err
		} else {
			consumer.consumers[ce.partition] = ce.consumer
		}
	}

	if err != nil {
		// Since there were some failures,
		// cleanup all the partitions that succeeded in creating the consumer
		for _, c := range consumer.consumers {
			if c != nil {
				_ = c.Close()
			}
		}
		return nil, err
	}

	return consumer, nil
}

func (c *consumer) Subscription() string {
	return c.options.SubscriptionName
}

func (c *consumer) Unsubscribe() error {
	var errMsg string
	for _, consumer := range c.consumers {
		if err := consumer.Unsubscribe(); err != nil {
			errMsg += fmt.Sprintf("topic %s, subscription %s: %s", consumer.topic, c.Subscription(), err)
		}
	}
	if errMsg != "" {
		return fmt.Errorf(errMsg)
	}
	return nil
}

func (c *consumer) Receive(ctx context.Context) (message Message, err error) {
	for {
		select {
		case cm, ok := <-c.messageCh:
			if !ok {
				return nil, ErrConsumerClosed
			}
			return cm.Message, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Messages
func (c *consumer) Chan() <-chan ConsumerMessage {
	return c.messageCh
}

// Ack the consumption of a single message
func (c *consumer) Ack(msg Message) {
	c.AckID(msg.ID())
}

// Ack the consumption of a single message, identified by its MessageID
func (c *consumer) AckID(msgID MessageID)  {
	mid, ok := msgID.(*messageID)
	if !ok {
		c.log.Warnf("invalid message id type")
	}

	partition := mid.partitionIdx
	// did we receive a valid partition index?
	if partition < 0 || partition >= len(c.consumers) {
		c.log.Warnf("invalid partition index %d expected a partition between [0-%d]",
			partition, len(c.consumers))
	}
	c.consumers[partition].AckID(mid)
}

func (c *consumer) Nack(msg Message) {
	c.AckID(msg.ID())
}

func (c *consumer) NackID(msgID MessageID) {
	mid, ok := msgID.(*messageID)
	if !ok {
		c.log.Warnf("invalid message id type")
	}

	partition := mid.partitionIdx
	// did we receive a valid partition index?
	if partition < 0 || partition >= len(c.consumers) {
		c.log.Warnf("invalid partition index %d expected a partition between [0-%d]",
			partition, len(c.consumers))
	}

	c.consumers[partition].NackID(mid)
}

func (c *consumer) Close() error {
	var wg sync.WaitGroup
	for i := range c.consumers {
		wg.Add(1)
		go func(pc *partitionConsumer) {
			defer wg.Done()
			pc.Close()
		}(c.consumers[i])
	}
	wg.Wait()

	return nil
}

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

func generateRandomName() string {
	chars := "abcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, 5)
	for i := range bytes {
		bytes[i] = chars[random.Intn(len(chars))]
	}
	return string(bytes)
}

func toProtoSubType(st SubscriptionType) pb.CommandSubscribe_SubType {
	switch st {
	case Exclusive:
		return pb.CommandSubscribe_Exclusive
	case Shared:
		return pb.CommandSubscribe_Shared
	case Failover:
		return pb.CommandSubscribe_Failover
	case KeyShared:
		return pb.CommandSubscribe_Key_Shared
	}

	return pb.CommandSubscribe_Exclusive
}

func toProtoInitialPosition(p SubscriptionInitialPosition) pb.CommandSubscribe_InitialPosition {
	switch p {
	case SubscriptionPositionLatest:
		return pb.CommandSubscribe_Latest
	case SubscriptionPositionEarliest:
		return pb.CommandSubscribe_Earliest
	}

	return pb.CommandSubscribe_Latest
}
