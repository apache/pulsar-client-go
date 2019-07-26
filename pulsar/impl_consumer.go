//
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
//

package pulsar

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pkg/pb"
	"github.com/apache/pulsar-client-go/util"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type consumer struct {
	topicName       string
	consumers       []Consumer
	log             *log.Entry
	queue           chan ConsumerMessage
	unackMsgTracker *UnackedMessageTracker
}

func newConsumer(client *client, options *ConsumerOptions) (*consumer, error) {
	if options == nil {
		return nil, newError(ResultInvalidConfiguration, "consumer configuration undefined")
	}

	if options.Topic == "" && options.Topics == nil && options.TopicsPattern == "" {
		return nil, newError(TopicNotFound, "topic is required")
	}

	if options.SubscriptionName == "" {
		return nil, newError(SubscriptionNotFound, "subscription name is required for consumer")
	}

	if options.ReceiverQueueSize == 0 {
		options.ReceiverQueueSize = 1000
	}

	if options.TopicsPattern != "" {
		if options.Topics != nil {
			return nil, newError(ResultInvalidConfiguration, "Topic names list must be null when use topicsPattern")
		}
		// TODO: impl logic
	} else if options.Topics != nil && len(options.Topics) > 1 {
		// TODO: impl logic
	} else if options.Topics != nil && len(options.Topics) == 1 || options.Topic != "" {
		var singleTopicName string
		if options.Topic != "" {
			singleTopicName = options.Topic
		} else {
			singleTopicName = options.Topics[0]
		}
		return singleTopicSubscribe(client, options, singleTopicName)
	}

	return nil, newError(ResultInvalidTopicName, "topic name is required for consumer")
}

func singleTopicSubscribe(client *client, options *ConsumerOptions, topic string) (*consumer, error) {
	c := &consumer{
		topicName: topic,
		queue:     make(chan ConsumerMessage, options.ReceiverQueueSize),
	}

	partitions, err := client.TopicPartitions(topic)
	if err != nil {
		return nil, err
	}

	numPartitions := len(partitions)
	c.consumers = make([]Consumer, numPartitions)

	type ConsumerError struct {
		err       error
		partition int
		cons      Consumer
	}

	ch := make(chan ConsumerError, numPartitions)

	for partitionIdx, partitionTopic := range partitions {
		go func(partitionIdx int, partitionTopic string) {
			cons, err := newPartitionConsumer(client, partitionTopic, options, partitionIdx)
			ch <- ConsumerError{
				err:       err,
				partition: partitionIdx,
				cons:      cons,
			}
		}(partitionIdx, partitionTopic)
	}

	for i := 0; i < numPartitions; i++ {
		ce, ok := <-ch
		if ok {
			err = ce.err
			c.consumers[ce.partition] = ce.cons
		}
	}

	if err != nil {
		// Since there were some failures, cleanup all the partitions that succeeded in creating the consumers
		for _, consumer := range c.consumers {
			if !util.IsNil(consumer) {
				if err := consumer.Close(); err != nil {
					panic("close consumer error, please check.")
				}
			}
		}
		return nil, err
	}

	return c, nil
}

func (c *consumer) Topic() string {
	return c.topicName
}

func (c *consumer) Subscription() string {
	return c.consumers[0].Subscription()
}

func (c *consumer) Unsubscribe() error {
	var errMsg string
	for _, c := range c.consumers {
		if err := c.Unsubscribe(); err != nil {
			errMsg += fmt.Sprintf("topic %s, subscription %s: %s", c.Topic(), c.Subscription(), err)
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

func (c *consumer) Receive(ctx context.Context) (Message, error) {
	for _, pc := range c.consumers {
		go func(pc Consumer) {
			if err := pc.ReceiveAsync(ctx, c.queue); err != nil {
				return
			}
		}(pc)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-c.queue:
		if ok {
			return msg.Message, nil
		}
		return nil, errors.New("receive message error")
	}
}

func (c *consumer) ReceiveAsync(ctx context.Context, msgs chan<- ConsumerMessage) error {
	//TODO: impl logic
	return nil
}

//Ack the consumption of a single message
func (c *consumer) Ack(msg Message) error {
	return c.AckID(msg.ID())
}

// Ack the consumption of a single message, identified by its MessageID
func (c *consumer) AckID(msgID MessageID) error {
	id := &pb.MessageIdData{}
	err := proto.Unmarshal(msgID.Serialize(), id)
	if err != nil {
		c.log.WithError(err).Errorf("unserialize message id error:%s", err.Error())
		return err
	}

	partition := id.GetPartition()
	if partition < 0 {
		return c.consumers[0].AckID(msgID)
	}
	return c.consumers[partition].AckID(msgID)
}

func (c *consumer) AckCumulative(msg Message) error {
	return c.AckCumulativeID(msg.ID())
}

func (c *consumer) AckCumulativeID(msgID MessageID) error {
	id := &pb.MessageIdData{}
	err := proto.Unmarshal(msgID.Serialize(), id)
	if err != nil {
		c.log.WithError(err).Errorf("unserialize message id error:%s", err.Error())
		return err
	}

	partition := id.GetPartition()
	if partition < 0 {
		return errors.New("invalid partition index")
	}
	return c.consumers[partition].AckCumulativeID(msgID)
}

func (c *consumer) Close() error {
	for _, pc := range c.consumers {
		return pc.Close()
	}
	return nil
}

func (c *consumer) Seek(msgID MessageID) error {
	id := &pb.MessageIdData{}
	err := proto.Unmarshal(msgID.Serialize(), id)
	if err != nil {
		c.log.WithError(err).Errorf("unserialize message id error:%s", err.Error())
		return err
	}

	partition := id.GetPartition()

	if partition < 0 {
		return errors.New("invalid partition index")
	}
	return c.consumers[partition].Seek(msgID)
}

func (c *consumer) RedeliverUnackedMessages() error {
	var errMsg string
	for _, c := range c.consumers {
		if err := c.RedeliverUnackedMessages(); err != nil {
			errMsg += fmt.Sprintf("topic %s, subscription %s: %s", c.Topic(), c.Subscription(), err)
		}
	}

	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}
