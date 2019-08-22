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
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/util"
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

	// unsubscribe consumer
	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
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

	// check strategically
	for i := 0; i < 3; i++ {
		if count == numOfMessages {
			break
		}
		time.Sleep(time.Second)
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

	// Expect error in creating cosnumer
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

func TestConsumer_SubscriptionEarliestPos(t *testing.T) {
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

	//sent message
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
		SubscriptionInitPos: Earliest,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)

	assert.Equal(t, "msg-1-content-1", string(msg.Payload()))
}

func makeHTTPCall(t *testing.T, method string, urls string, body string) {
	client := http.Client{}

	req, err := http.NewRequest(method, urls, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
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
		Topic: topic,
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

	time.Sleep(time.Second * 1)

	go func() {
		for i := 0; i < 10; i++ {
			msg, err := consumer1.Receive(ctx)
			assert.Nil(t, err)
			if msg != nil {
				fmt.Printf("consumer1 key is: %s, value is: %s\n", msg.Key(), string(msg.Payload()))
				err = consumer1.Ack(msg)
				assert.Nil(t, err)
			}
		}
	}()

	go func() {
		for i := 0; i < 10; i++ {
			msg2, err := consumer2.Receive(ctx)
			assert.Nil(t, err)
			if msg2 != nil {
				fmt.Printf("consumer2 key is:%s, value is: %s\n", msg2.Key(), string(msg2.Payload()))
				err = consumer2.Ack(msg2)
				assert.Nil(t, err)
			}
		}
	}()

	time.Sleep(time.Second * 1)
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

func TestConsumer_ReceiveAsync(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := "persistent://public/default/receive-async"
	subName := "subscription-receive-async"
	ctx := context.Background()
	ch := make(chan ConsumerMessage, 10)

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: subName,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	//send 10 messages
	for i := 0; i < 10; i++ {
		err = producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)
	}

	//receive async 10 messages
	err = consumer.ReceiveAsync(ctx, ch)
	assert.Nil(t, err)

	payloadList := make([]string, 0, 10)

RECEIVE:
	for {
		select {
		case cMsg, ok := <-ch:
			if ok {
				fmt.Printf("receive message payload is:%s\n", string(cMsg.Payload()))
				assert.Equal(t, topicName, cMsg.Message.Topic())
				assert.Equal(t, topicName, cMsg.Consumer.Topic())
				payloadList = append(payloadList, string(cMsg.Message.Payload()))
				if len(payloadList) == 10 {
					break RECEIVE
				}
			}
			continue RECEIVE
		case <-ctx.Done():
			t.Error("context error.")
			return
		}
	}
}

func TestConsumerAckTimeout(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := "test-ack-timeout-topic-1"
	ctx := context.Background()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub1",
		Type:             Shared,
		AckTimeout:       5 * 1000,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create consumer1
	consumer1, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub2",
		Type:             Shared,
		AckTimeout:       5 * 1000,
	})
	assert.Nil(t, err)
	defer consumer1.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		if err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		}
	}

	// consumer receive 10 messages
	payloadList := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		payloadList = append(payloadList, string(msg.Payload()))

		// not ack message
	}
	assert.Equal(t, 10, len(payloadList))

	// consumer1 receive 10 messages
	for i := 0; i < 10; i++ {
		msg, err := consumer1.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		payloadList = append(payloadList, string(msg.Payload()))

		// ack half of the messages
		if i%2 == 0 {
			err = consumer1.Ack(msg)
			assert.Nil(t, err)
		}
	}

	// wait ack timeout
	time.Sleep(6 * time.Second)

	fmt.Println("start redeliver messages...")

	payloadList = make([]string, 0, 10)
	// consumer receive messages again
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		payloadList = append(payloadList, string(msg.Payload()))

		// ack message
		if err := consumer.Ack(msg); err != nil {
			log.Fatal(err)
		}
	}
	assert.Equal(t, 10, len(payloadList))

	payloadList = make([]string, 0, 5)
	// consumer1 receive messages again
	go func() {
		for i := 0; i < 10; i++ {
			msg, err := consumer1.Receive(context.Background())
			if err != nil {
				log.Fatal(err)
			}

			expectMsg := fmt.Sprintf("hello-%d", i)
			fmt.Printf("redeliver messages, payload is:%s\n", expectMsg)
			payloadList = append(payloadList, string(msg.Payload()))

			// ack message
			if err := consumer1.Ack(msg); err != nil {
				log.Fatal(err)
			}
		}
		assert.Equal(t, 5, len(payloadList))
	}()

	// sleep 2 seconds, wait gorutine receive messages.
	time.Sleep(time.Second * 2)
}

func TestConsumer_ReceiveAsyncWithCallback(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := "persistent://public/default/receive-async-with-callback"
	subName := "subscription-receive-async"
	ctx := context.Background()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: subName,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	//send 10 messages
	for i := 0; i < 10; i++ {
		err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)
	}

	for i := 0; i < 10; i++ {
		tmpMsg := fmt.Sprintf("hello-%d", i)
		consumer.ReceiveAsyncWithCallback(ctx, func(msg Message, err error) {
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("receive message payload is:%s\n", string(msg.Payload()))
			assert.Equal(t, tmpMsg, string(msg.Payload()))
		})
	}
}

func TestConsumer_Shared(t *testing.T) {
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

	// send 10 messages
	for i := 0; i < 10; i++ {
		if err := producer.Send(context.Background(), &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		}
	}

	msgList := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		msg, err := consumer1.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("consumer1 msg id is: %v, value is: %s\n", msg.ID(), string(msg.Payload()))
		msgList = append(msgList, string(msg.Payload()))
		if err := consumer1.Ack(msg); err != nil {
			log.Fatal(err)
		}
	}

	assert.Equal(t, 5, len(msgList))

	for i := 0; i < 5; i++ {
		msg, err := consumer2.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		if err := consumer2.Ack(msg); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("consumer2 msg id is: %v, value is: %s\n", msg.ID(), string(msg.Payload()))
		msgList = append(msgList, string(msg.Payload()))
	}

	assert.Equal(t, 10, len(msgList))
	res := util.RemoveDuplicateElement(msgList)
	assert.Equal(t, 10, len(res))
}

func TestConsumer_Seek(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := "persistent://public/default/testSeek"
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/testSeek"
	makeHTTPCall(t, http.MethodPut, testURL, "1")
	subName := "sub-testSeek"

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.Nil(t, err)
	assert.Equal(t, producer.Topic(), topicName)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: subName,
	})
	assert.Nil(t, err)
	assert.Equal(t, consumer.Topic(), topicName)
	assert.Equal(t, consumer.Subscription(), subName)
	defer consumer.Close()

	ctx := context.Background()

	// Send 10 messages synchronously
	t.Log("Publishing 10 messages synchronously")
	for msgNum := 0; msgNum < 10; msgNum++ {
		if err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d", msgNum)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	t.Log("Trying to receive 10 messages")
	idList := make([]MessageID, 0, 10)
	for msgNum := 0; msgNum < 10; msgNum++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		idList = append(idList, msg.ID())
		fmt.Println(string(msg.Payload()))
	}

	for index, id := range idList {
		if index == 4 {
			// seek to fourth message, expected receive fourth message.
			err = consumer.Seek(id)
			assert.Nil(t, err)
			break
		}
	}

	// Sleeping for 500ms to wait for consumer re-connect
	time.Sleep(500 * time.Millisecond)

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	t.Logf("again received message:%+v", msg.ID())
	assert.Equal(t, "msg-content-4", string(msg.Payload()))
}

func TestConsumer_EventTime(t *testing.T) {
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

func TestConsumer_Flow(t *testing.T) {
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
