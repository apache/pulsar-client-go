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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMultiTopicConsumerReceive(t *testing.T) {
	topic1 := newTopicName()
	topic2 := newTopicName()

	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		t.Fatal(err)
	}
	topics := []string{topic1, topic2}
	consumer, err := client.Subscribe(ConsumerOptions{
		Topics:           topics,
		SubscriptionName: "multi-topic-sub",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	// produce messages
	for i, topic := range topics {
		p, err := client.CreateProducer(ProducerOptions{
			Topic:           topic,
			DisableBatching: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = genMessages(p, 10, func(idx int) string {
			return fmt.Sprintf("topic-%d-hello-%d", i+1, idx)
		})
		p.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	receivedTopic1 := 0
	receivedTopic2 := 0
	// nolint
	for receivedTopic1+receivedTopic2 < 20 {
		select {
		case cm, ok := <-consumer.Chan():
			if ok {
				msg := string(cm.Payload())
				if strings.HasPrefix(msg, "topic-1") {
					receivedTopic1++
				} else if strings.HasPrefix(msg, "topic-2") {
					receivedTopic2++
				}
				consumer.Ack(cm.Message)
			} else {
				t.Fail()
			}
		}
	}
	assert.Equal(t, receivedTopic1, receivedTopic2)
}

func TestMultiTopicConsumerBatchReceive(t *testing.T) {
	topic1 := newTopicName()
	topic2 := newTopicName()

	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	assert.Nil(t, err)
	testTopics := []string{topic1, topic2}
	commonOpt := ConsumerOptions{
		Topics:                      testTopics,
		SubscriptionName:            "multi-topic-sub",
		Type:                        Exclusive,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	}

	// total 10 messages, 50 byte
	for _, topic := range testTopics {
		producer, err := client.CreateProducer(ProducerOptions{
			Topic: topic,
		})
		assert.Nil(t, err)
		for i := 0; i < 5; i++ {
			msg := &ProducerMessage{
				Payload: []byte("12345"),
			}
			_, err := producer.Send(context.Background(), msg)
			assert.Nil(t, err)
		}
	}

	//test for maxNumMessages
	commonOpt.BatchReceivePolicy = &BatchReceivePolicy{
		maxNumMessages: 6,
		maxNumBytes:    999,
		timeout:        10 * time.Second,
	}
	_consumer, err := client.Subscribe(commonOpt)
	assert.Nil(t, err)
	consumer1 := _consumer.(*multiTopicConsumer)
	messages, err := consumer1.BatchReceive(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 6, messages.Size())
	consumer1.Close()

	//test for maxNumBytes
	commonOpt.BatchReceivePolicy = &BatchReceivePolicy{
		maxNumMessages: 999,
		maxNumBytes:    14,
		timeout:        10 * time.Second,
	}
	_consumer, err = client.Subscribe(commonOpt)
	assert.Nil(t, err)
	consumer2 := _consumer.(*multiTopicConsumer)
	messages, err = consumer2.BatchReceive(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 2, messages.Size())
	consumer2.Close()

	//test for maxNumBytes and maxNumMessages
	commonOpt.BatchReceivePolicy = &BatchReceivePolicy{
		maxNumMessages: 3,
		maxNumBytes:    24,
		timeout:        10 * time.Second,
	}
	_consumer, err = client.Subscribe(commonOpt)
	assert.Nil(t, err)
	consumer3 := _consumer.(*multiTopicConsumer)
	messages, err = consumer3.BatchReceive(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 3, messages.Size())
	consumer3.Close()

	// test for timeout
	commonOpt.BatchReceivePolicy = &BatchReceivePolicy{
		maxNumMessages: 999,
		maxNumBytes:    999,
		timeout:        3 * time.Second,
	}
	_consumer, err = client.Subscribe(commonOpt)
	assert.Nil(t, err)
	consumer4 := _consumer.(*multiTopicConsumer)

	ch := make(chan struct{})
	go func() {
		messages, err = consumer4.BatchReceive(context.Background())
		ch <- struct{}{}
	}()
	timer := time.NewTimer(4 * time.Second)
	select {
	case <-timer.C:
		assert.Fail(t, "BatchReceivePolicy.timeout failed: should stop after 3 seconds")
	case <-ch:
		assert.Nil(t, err)
		assert.Equal(t, 10, messages.Size())
	}
	consumer4.Close()
}
