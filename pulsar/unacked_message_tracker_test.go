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
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	ackTimeout  = 2000 * time.Millisecond
	recvTimeout = 1000 * time.Millisecond
)

type ackTimeoutMockedConsumer struct {
	msgCh  chan messageID
	closed bool
	sync.Mutex
}

func newAckTimeoutMockedConsumer() *ackTimeoutMockedConsumer {
	c := &ackTimeoutMockedConsumer{
		msgCh: make(chan messageID, 10),
	}
	go func() {
		// ensure we have enough time to verify msgs after timeout
		time.Sleep(ackTimeout + minAckTimeoutTickTime*2)
		c.Lock()
		defer c.Unlock()
		c.closed = true
		close(c.msgCh)
	}()
	return c
}

func (c *ackTimeoutMockedConsumer) Redeliver(msgIds []messageID) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return
	}
	for _, msgID := range msgIds {
		c.msgCh <- msgID
	}
}

func (c *ackTimeoutMockedConsumer) AckID(trackingMessageID) {}

func (c *ackTimeoutMockedConsumer) NackID(trackingMessageID) {}

func TestAckTimeoutWithBatchesTracker(t *testing.T) {
	c := newAckTimeoutMockedConsumer()
	tracker := newUnackedMessageTracker(ackTimeout)
	defer tracker.Close()

	f := func(ledgerID, entryID int64, batchIdx int32) trackingMessageID {
		return trackingMessageID{
			messageID: messageID{
				ledgerID: ledgerID,
				entryID:  entryID,
				batchIdx: batchIdx,
			},
			consumer: c,
		}
	}
	tracker.add(f(1, 1, 1))
	tracker.add(f(1, 1, 2))
	tracker.add(f(1, 1, 3))
	tracker.add(f(2, 2, 1))

	msgIDs := make([]messageID, 0, 5)
	for msgID := range c.msgCh {
		msgIDs = append(msgIDs, msgID)
	}
	msgIDs = sortMessageIds(msgIDs)

	assert.Equal(t, 2, len(msgIDs))
	assert.Equal(t, int64(1), msgIDs[0].ledgerID)
	assert.Equal(t, int64(1), msgIDs[0].entryID)
	assert.Equal(t, int64(2), msgIDs[1].ledgerID)
	assert.Equal(t, int64(2), msgIDs[1].entryID)
}

func TestMinAckTimeout(t *testing.T) {
	client, err := newClient(ClientOptions{URL: lookupURL})
	assert.Nil(t, err)
	defer client.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            newTopicName(),
		SubscriptionName: "sub-1",
		Type:             Exclusive,
		AckTimeout:       minAckTimeout - 1*time.Millisecond,
	})
	assert.NotNil(t, err)
	log.Error(err)
	assert.Nil(t, consumer)
}

func TestAckTimeoutExclusive(t *testing.T) {
	client, err := newClient(ClientOptions{URL: lookupURL})
	assert.Nil(t, err)
	defer client.Close()
	totalMsgs := 10

	topic := newTopicName()
	ctx := context.Background()

	// 1. create producer
	producer, err := client.CreateProducer(ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	defer producer.Close()

	// 2. create consumer
	c, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "sub-1",
		Type:             Exclusive,
		AckTimeout:       ackTimeout,
	})
	assert.Nil(t, err)
	defer c.Close()

	// 3. publish 5 messages
	for i := 0; i < totalMsgs/2; i++ {
		_, err = producer.Send(ctx, &ProducerMessage{Payload: []byte(fmt.Sprintf("MSG_%d", i))})
		assert.Nil(t, err)
	}

	// 4. received 5 messages, but not ack
	recv := receiveAll(c, false)
	assert.Equal(t, totalMsgs/2, recv)

	// validate consumer's unack tracker size
	assert.Equal(t, totalMsgs/2, unackTrackersSize(c))
	// wait ack timeout triggered
	time.Sleep(ackTimeout)

	// 5. publish more 5 messages
	for i := totalMsgs / 2; i < totalMsgs; i++ {
		_, err = producer.Send(ctx, &ProducerMessage{Payload: []byte(fmt.Sprintf("MSG_%d", i))})
		assert.Nil(t, err)
	}

	// 6. receive 10 messages, and ack
	recv = receiveAll(c, true)
	assert.Equal(t, totalMsgs, recv)
	assert.Equal(t, 0, unackTrackersSize(c))
}

func TestAckTimeoutFailover(t *testing.T) {
	topic := newTopicName()
	uri := adminURL + "/" + "admin/v2/persistent/public/default/" + topic + "/partitions"
	makeHTTPCall(t, http.MethodPut, uri, "3")

	client, err := newClient(ClientOptions{URL: lookupURL})
	assert.Nil(t, err)
	defer client.Close()

	totalMsgs := 10
	ctx := context.Background()

	// 1. create producer
	producer, err := client.CreateProducer(ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	defer producer.Close()

	// 2. create 2 consumers
	c1, err := client.Subscribe(ConsumerOptions{
		Name:             "C1",
		Topic:            topic,
		SubscriptionName: "sub-1",
		Type:             Failover,
		AckTimeout:       ackTimeout,
	})
	assert.Nil(t, err)
	defer c1.Close()

	c2, err := client.Subscribe(ConsumerOptions{
		Name:             "C2",
		Topic:            topic,
		SubscriptionName: "sub-1",
		Type:             Failover,
		AckTimeout:       ackTimeout,
	})
	assert.Nil(t, err)
	defer c2.Close()

	// 3. publish 10 messages
	for i := 0; i < totalMsgs; i++ {
		_, err = producer.Send(ctx, &ProducerMessage{Payload: []byte(fmt.Sprintf("MSG_%d", i))})
		assert.Nil(t, err)
	}

	// 4. received 10 messages totally
	recv1 := receiveAll(c1, false)
	acked1 := 0
	recv2 := receiveAll(c2, true)
	assert.Equal(t, totalMsgs, recv1+recv2)

	// wait ack timeout triggered
	time.Sleep(ackTimeout)

	// 5. check if messages redelivery
	recv1 = receiveAll(c1, true)
	acked1 = recv1
	recv2 += receiveAll(c2, false)
	assert.Equal(t, totalMsgs, acked1+recv2)
	assert.Equal(t, 0, unackTrackersSize(c1))
	assert.Equal(t, 0, unackTrackersSize(c2))
}

func TestAckTimeoutShared(t *testing.T) {
	topic := newTopicName()
	uri := adminURL + "/" + "admin/v2/persistent/public/default/" + topic + "/partitions"
	makeHTTPCall(t, http.MethodPut, uri, "3")

	client, err := newClient(ClientOptions{URL: lookupURL})
	assert.Nil(t, err)
	defer client.Close()

	totalMsgs := 10
	ctx := context.Background()

	// 1. create producer
	producer, err := client.CreateProducer(ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	defer producer.Close()

	// 2. create 2 consumers
	c1, err := client.Subscribe(ConsumerOptions{
		Name:             "C1",
		Topic:            topic,
		SubscriptionName: "sub-1",
		Type:             Shared,
		AckTimeout:       ackTimeout,
	})
	assert.Nil(t, err)
	defer c1.Close()

	c2, err := client.Subscribe(ConsumerOptions{
		Name:             "C2",
		Topic:            topic,
		SubscriptionName: "sub-1",
		Type:             Shared,
		AckTimeout:       ackTimeout,
	})
	assert.Nil(t, err)
	defer c2.Close()

	// 3. publish 10 messages
	for i := 0; i < totalMsgs; i++ {
		_, err = producer.Send(ctx, &ProducerMessage{Payload: []byte(fmt.Sprintf("MSG_%d", i))})
		assert.Nil(t, err)
	}

	// 4. receive 10 messages totally
	recv1 := receiveAll(c1, false) // not-ack
	acked1 := 0
	recv2 := receiveAll(c2, true) // ack
	acked2 := recv2
	assert.Equal(t, totalMsgs, recv1+recv2)

	time.Sleep(ackTimeout)

	// 5. check if messages redelivery again
	recv1 = receiveAll(c1, true) // not-ack --> ack
	acked1 = recv1
	recv2 += receiveAll(c2, false) // ack --> not-ack
	assert.Equal(t, totalMsgs, recv1+recv2)
	assert.Equal(t, totalMsgs, acked1+recv2)

	time.Sleep(ackTimeout)

	// 6. now check remaining messages redelivery
	acked1 += receiveAll(c1, true)
	acked2 += receiveAll(c2, true)
	assert.Equal(t, totalMsgs, acked1+acked2)
	assert.Equal(t, 0, unackTrackersSize(c1))
	assert.Equal(t, 0, unackTrackersSize(c2))
}

func TestAckTimeoutSharedMultiAndRegexTopics(t *testing.T) {
	t.Run("AckTimeoutMultiTopics", func(t *testing.T) {
		topic := newTopicName()
		topics := []string{topic + "1", topic + "2"}
		option := ConsumerOptions{
			Topics:           topics,
			SubscriptionName: "sub-1",
			Type:             Shared,
			AckTimeout:       ackTimeout,
		}
		runAckTimeoutMultiTopics(t, topics, option)
	})

	t.Run("AckTimeoutRegexTopics", func(t *testing.T) {
		topic := newTopicName()
		topics := []string{topic + "1", topic + "2"}
		option := ConsumerOptions{
			TopicsPattern:    topic + ".*",
			SubscriptionName: "sub-1",
			Type:             Shared,
			AckTimeout:       ackTimeout,
		}
		runAckTimeoutMultiTopics(t, topics, option)
	})
}

func runAckTimeoutMultiTopics(t *testing.T, topics []string, options ConsumerOptions) {
	topic1, topic2 := topics[0], topics[1]
	assert.Nil(t, createPartitionTopic(t, topic1, 3))
	assert.Nil(t, createPartitionTopic(t, topic2, 3))

	client, err := newClient(ClientOptions{URL: lookupURL})
	assert.Nil(t, err)
	defer client.Close()

	totalMsgs := 10
	ctx := context.Background()

	// 1. create 2 producers
	p1, err := client.CreateProducer(ProducerOptions{Topic: topic1})
	assert.Nil(t, err)
	defer p1.Close()
	p2, err := client.CreateProducer(ProducerOptions{Topic: topic2})
	assert.Nil(t, err)
	defer p2.Close()

	// 2. create 2 consumers
	options.Name = "C1"
	c1, err := client.Subscribe(options)
	assert.Nil(t, err)
	defer c1.Close()

	options.Name = "C2"
	c2, err := client.Subscribe(options)
	assert.Nil(t, err)
	defer c2.Close()

	// 3. publish 10 messages
	for i := 0; i < totalMsgs/2; i++ {
		_, err = p1.Send(ctx, &ProducerMessage{Payload: []byte(fmt.Sprintf("MSG_T1_%d", i))})
		assert.Nil(t, err)
		_, err = p2.Send(ctx, &ProducerMessage{Payload: []byte(fmt.Sprintf("MSG_T2_%d", i))})
		assert.Nil(t, err)
	}

	// 4. receive 10 messages totally
	recv1 := receiveAll(c1, false) // not-ack
	acked1 := 0
	recv2 := receiveAll(c2, true) // ack
	acked2 := recv2
	assert.Equal(t, totalMsgs, recv1+recv2)

	time.Sleep(ackTimeout)

	// 5. check if messages redelivery again
	recv1 = receiveAll(c1, true) // reset
	acked1 = recv1
	recv2 += receiveAll(c2, false)
	assert.Equal(t, totalMsgs, recv1+recv2)
	assert.Equal(t, totalMsgs, acked1+recv2)

	time.Sleep(ackTimeout)

	// 6. now check remaining messages redelivery
	acked1 += receiveAll(c1, true)
	acked2 += receiveAll(c2, true)
	assert.Equal(t, totalMsgs, acked1+acked2)
	assert.Equal(t, 0, unackTrackersSize(c1))
	assert.Equal(t, 0, unackTrackersSize(c2))
}

func receiveAll(c Consumer, ack bool) int {
	received := 0
	for {
		var ctx, cancel = context.WithTimeout(context.Background(), recvTimeout)
		defer cancel()
		msg, err := c.Receive(ctx)
		if err != nil {
			if err == context.DeadlineExceeded || msg == nil {
				return received
			}
			log.Errorf("unexpected receiveAll error: %v", err)
			return 0
		}
		if !ack {
			log.Warnf("%s consumed but not ack: <%s>: %s", c.Name(), msg.ID(), string(msg.Payload()))
		} else {
			log.Infof("%s consumed and ack: <%s>: %s", c.Name(), msg.ID(), string(msg.Payload()))
			c.Ack(msg)
		}
		received++
	}
}

func unackTrackersSize(ic Consumer) (tracked int) {
	switch c := ic.(type) {
	case *consumer:
		return len(c.unackTracker.msgID2Sector)
	case *multiTopicConsumer:
		return len(c.unackTracker.msgID2Sector)
	case *regexConsumer:
		return len(c.unackTracker.msgID2Sector)
	default:
		log.Errorf("unexpected Consumer type: %T", ic)
		return -1
	}
}

func createPartitionTopic(t *testing.T, topic string, partition int) error {
	if partition <= 1 {
		return createTopic(topic)
	}
	uri := adminURL + "/" + "admin/v2/persistent/public/default/" + topic + "/partitions"
	makeHTTPCall(t, http.MethodPut, uri, strconv.Itoa(partition))
	return nil
}
