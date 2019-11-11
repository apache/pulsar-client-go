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
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	adminURL  = "http://localhost:8080"
	lookupURL = "pulsar://localhost:6650"
)

func TestProducerConsumer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := "my-topic"
	ctx := context.Background()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
		Type:             Exclusive,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		if err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Key:     "pulsar",
			Properties: map[string]string{
				"key-1": "pulsar-1",
			},
		}); err != nil {
			log.Fatal(err)
		}
	}

	// receive 10 messages
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		expectMsg := fmt.Sprintf("hello-%d", i)
		expectProperties := map[string]string{
			"key-1": "pulsar-1",
		}
		assert.Equal(t, []byte(expectMsg), msg.Payload())
		assert.Equal(t, "pulsar", msg.Key())
		assert.Equal(t, expectProperties, msg.Properties())

		// ack message
		if err := consumer.Ack(msg); err != nil {
			log.Fatal(err)
		}
	}
}

func TestConsumerConnectError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://invalid-hostname:6650",
	})

	assert.Nil(t, err)

	defer client.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "my-topic",
		SubscriptionName: "my-subscription",
	})

	// Expect error in creating consumer
	assert.Nil(t, consumer)
	assert.NotNil(t, err)

	assert.Equal(t, err.Error(), "connection error")
}

func TestBatchMessageReceive(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := "persistent://public/default/receive-batch"
	subName := "subscription-name"
	prefix := "msg-batch-"
	ctx := context.Background()

	// Enable batching on producer side
	batchSize, numOfMessages := 2, 100

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:               topicName,
		BatchingMaxMessages: uint(batchSize),
		DisableBatching:     false,
		BlockIfQueueFull:    true,
	})
	assert.Nil(t, err)
	assert.Equal(t, topicName, producer.Topic())
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: subName,
	})
	assert.Nil(t, err)
	assert.Equal(t, topicName, consumer.Topic())
	count := 0

	for i := 0; i < numOfMessages; i++ {
		messageContent := prefix + fmt.Sprintf("%d", i)
		msg := &ProducerMessage{
			Payload: []byte(messageContent),
		}
		err := producer.Send(ctx, msg)
		assert.Nil(t, err)
	}

	for i := 0; i < numOfMessages; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		err = consumer.Ack(msg)
		assert.Nil(t, err)
		count++
	}

	assert.Equal(t, count, numOfMessages)
}

func TestConsumerWithInvalidConf(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	if err != nil {
		t.Fatal(err)
		return
	}

	defer client.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic: "my-topic",
	})

	// Expect error in creating consumer
	assert.Nil(t, consumer)
	assert.NotNil(t, err)

	fmt.Println(err.Error())
	assert.Equal(t, err.(*Error).Result(), SubscriptionNotFound)

	consumer, err = client.Subscribe(ConsumerOptions{
		SubscriptionName: "my-subscription",
	})

	// Expect error in creating consumer
	assert.Nil(t, consumer)
	assert.NotNil(t, err)

	assert.Equal(t, err.(*Error).Result(), TopicNotFound)
}

func TestConsumerSubscriptionEarliestPosition(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := fmt.Sprintf("testSeek-%d", time.Now().Unix())
	subName := "test-subscription-initial-earliest-position"

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send message
	ctx := context.Background()
	err = producer.Send(ctx, &ProducerMessage{
		Payload: []byte("msg-1-content-1"),
	})
	assert.Nil(t, err)

	err = producer.Send(ctx, &ProducerMessage{
		Payload: []byte("msg-1-content-2"),
	})
	assert.Nil(t, err)

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:               topicName,
		SubscriptionName:    subName,
		SubscriptionInitPos: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)

	assert.Equal(t, "msg-1-content-1", string(msg.Payload()))
}

func TestConsumerKeyShared(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/test-topic-6"

	consumer1, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "sub-1",
		Type:             KeyShared,
	})
	assert.Nil(t, err)
	defer consumer1.Close()

	consumer2, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "sub-1",
		Type:             KeyShared,
	})
	assert.Nil(t, err)
	defer consumer2.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		err := producer.Send(ctx, &ProducerMessage{
			Key:     fmt.Sprintf("key-shared-%d", i%3),
			Payload: []byte(fmt.Sprintf("value-%d", i)),
		})
		assert.Nil(t, err)
	}

	receivedConsumer1 := 0
	receivedConsumer2 := 0
	for (receivedConsumer1 + receivedConsumer2) < 10 {
		select {
		case cm, ok := <-consumer1.Messages():
			if !ok {
				break
			}
			receivedConsumer1++
			if err := consumer1.Ack(cm.Message); err != nil {
				log.Fatal(err)
			}
		case cm, ok := <-consumer2.Messages():
			if !ok {
				break
			}
			receivedConsumer2++
			if err := consumer2.Ack(cm.Message); err != nil {
				log.Fatal(err)
			}
		}
	}

	fmt.Printf("TestConsumerKeyShared received messages consumer1: %d consumser2: %d\n",
		receivedConsumer1, receivedConsumer2)
	assert.Equal(t, 10, receivedConsumer1+receivedConsumer2)
}

func TestPartitionTopicsConsumerPubSub(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/testGetPartitions"
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/testGetPartitions/partitions"

	makeHTTPCall(t, http.MethodPut, testURL, "64")

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	assert.Nil(t, err)
	defer producer.Close()

	topics, err := client.TopicPartitions(topic)
	assert.Nil(t, err)
	assert.Equal(t, topic+"-partition-0", topics[0])
	assert.Equal(t, topic+"-partition-1", topics[1])
	assert.Equal(t, topic+"-partition-2", topics[2])

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:             topic,
		SubscriptionName:  "my-sub",
		Type:              Exclusive,
		ReceiverQueueSize: 10,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)
	}

	msgs := make([]string, 0)

	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		msgs = append(msgs, string(msg.Payload()))

		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		if err := consumer.Ack(msg); err != nil {
			assert.Nil(t, err)
		}
	}

	assert.Equal(t, len(msgs), 10)
}

func TestConsumerReceiveTimeout(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := "test-topic-with-no-messages"
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub1",
		Type:             Shared,
		AckTimeout:       5 * 1000,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, msg)
	assert.NotNil(t, err)
}

func TestConsumerShared(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/testMultiPartitionConsumerShared"
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/testMultiPartitionConsumerShared/partitions"

	makeHTTPCall(t, http.MethodPut, testURL, "3")

	sub := "sub-shared-1"
	consumer1, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
		Type:             Shared,
	})
	assert.Nil(t, err)
	defer consumer1.Close()

	consumer2, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
		Type:             Shared,
	})
	assert.Nil(t, err)
	defer consumer2.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages with unique payloads
	for i := 0; i < 10; i++ {
		if err := producer.Send(context.Background(), &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println("sending message:", fmt.Sprintf("hello-%d", i))
	}

	readMsgs := 0
	messages := make(map[string]struct{})
	for readMsgs < 10 {
		select {
		case cm, ok := <-consumer1.Messages():
			if !ok {
				break
			}
			readMsgs++
			payload := string(cm.Message.Payload())
			messages[payload] = struct{}{}
			fmt.Printf("consumer1 msg id is: %v, value is: %s\n", cm.Message.ID(), payload)
			if err := consumer1.Ack(cm.Message); err != nil {
				log.Fatal(err)
			}
		case cm, ok := <-consumer2.Messages():
			if !ok {
				break
			}
			readMsgs++
			payload := string(cm.Message.Payload())
			messages[payload] = struct{}{}
			fmt.Printf("consumer2 msg id is: %v, value is: %s\n", cm.Message.ID(), payload)
			if err := consumer2.Ack(cm.Message); err != nil {
				log.Fatal(err)
			}
		}
	}

	assert.Equal(t, 10, len(messages))
}

func TestConsumerEventTime(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := "test-event-time"
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
	})
	assert.Nil(t, err)
	defer consumer.Close()

	et := timeFromUnixTimestampMillis(uint64(5))
	err = producer.Send(ctx, &ProducerMessage{
		Payload:   []byte("test"),
		EventTime: &et,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	assert.Equal(t, et, msg.EventTime())
	assert.Equal(t, "test", string(msg.Payload()))
}

func TestConsumerFlow(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := "test-received-since-flow"
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:             topicName,
		SubscriptionName:  "sub-1",
		ReceiverQueueSize: 4,
	})
	assert.Nil(t, err)

	for msgNum := 0; msgNum < 100; msgNum++ {
		if err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d", msgNum)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for msgNum := 0; msgNum < 100; msgNum++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", msgNum), string(msg.Payload()))
	}
}
