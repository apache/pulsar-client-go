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
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	"github.com/apache/pulsar-client-go/pulsar/internal"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/pierrec/lz4"
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
		if _, err := producer.Send(ctx, &ProducerMessage{
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
		consumer.Ack(msg)
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
	})
	assert.Nil(t, err)
	assert.Equal(t, topicName, producer.Topic())
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: subName,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	count := 0
	for i := 0; i < numOfMessages; i++ {
		messageContent := prefix + fmt.Sprintf("%d", i)
		msg := &ProducerMessage{
			Payload: []byte(messageContent),
		}
		_, err := producer.Send(ctx, msg)
		assert.Nil(t, err)
	}

	for i := 0; i < numOfMessages; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		consumer.Ack(msg)
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
	_, err = producer.Send(ctx, &ProducerMessage{
		Payload: []byte("msg-1-content-1"),
	})
	assert.Nil(t, err)

	_, err = producer.Send(ctx, &ProducerMessage{
		Payload: []byte("msg-1-content-2"),
	})
	assert.Nil(t, err)

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            subName,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
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
	for i := 0; i < 100; i++ {
		_, err := producer.Send(ctx, &ProducerMessage{
			Key:     fmt.Sprintf("key-shared-%d", i%3),
			Payload: []byte(fmt.Sprintf("value-%d", i)),
		})
		assert.Nil(t, err)
	}

	receivedConsumer1 := 0
	receivedConsumer2 := 0
	for (receivedConsumer1 + receivedConsumer2) < 100 {
		select {
		case cm, ok := <-consumer1.Chan():
			if !ok {
				break
			}
			receivedConsumer1++
			consumer1.Ack(cm.Message)
		case cm, ok := <-consumer2.Chan():
			if !ok {
				break
			}
			receivedConsumer2++
			consumer2.Ack(cm.Message)
		}
	}

	assert.NotEqual(t, 0, receivedConsumer1)
	assert.NotEqual(t, 0, receivedConsumer2)

	fmt.Printf("TestConsumerKeyShared received messages consumer1: %d consumser2: %d\n",
		receivedConsumer1, receivedConsumer2)
	assert.Equal(t, 100, receivedConsumer1+receivedConsumer2)
}

func TestPartitionTopicsConsumerPubSub(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/testGetPartitions5"
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/testGetPartitions5/partitions"

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
		_, err := producer.Send(ctx, &ProducerMessage{
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

		consumer.Ack(msg)
	}

	assert.Equal(t, len(msgs), 10)
}

type TestActiveConsumerListener struct {
	nameToPartitions map[string]map[int32]struct{}
}

func (t *TestActiveConsumerListener) BecameActive(consumer Consumer, topicName string, partition int32) {
	fmt.Printf("%s become active on %s - %d\n", consumer.Name(), topicName, partition)
	partitionSet := t.nameToPartitions[consumer.Name()]
	if partitionSet == nil {
		partitionSet = map[int32]struct{}{}
	}
	partitionSet[partition] = struct{}{}
	t.nameToPartitions[consumer.Name()] = partitionSet
}

func (t *TestActiveConsumerListener) BecameInactive(consumer Consumer, topicName string, partition int32) {
	fmt.Printf("%s become inactive on %s - %d\n", consumer.Name(), topicName, partition)
	partitionSet := t.nameToPartitions[consumer.Name()]
	if _, ok := partitionSet[partition]; ok {
		delete(partitionSet, partition)
		if len(partitionSet) == 0 {
			delete(t.nameToPartitions, consumer.Name())
		}
	}
}

func allConsume(consumers []Consumer) {
	for i := 0; i < len(consumers); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		consumers[i].Receive(ctx)
	}
}

func TestPartitionTopic_ActiveConsumerChanged(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/testGetPartitions5"
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/testGetPartitions5/partitions"

	makeHTTPCall(t, http.MethodPut, testURL, "3")

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)
	}

	var consumers []Consumer
	listener := &TestActiveConsumerListener{
		nameToPartitions: map[string]map[int32]struct{}{},
	}
	for i := 0; i < 1; i++ {
		consumer, err := client.Subscribe(ConsumerOptions{
			Topic:            topic,
			Name:             fmt.Sprintf("consumer-%d", i),
			SubscriptionName: "my-sub",
			Type:             Failover,
			EventListener:    listener,
		})
		assert.Nil(t, err)
		defer consumer.Close()
		consumers = append(consumers, consumer)
	}
	nameToPartitions := listener.nameToPartitions

	allConsume(consumers)
	// first consumer will get 3 partitions
	assert.Equal(t, len(nameToPartitions), 1)
	assert.Equal(t, len(nameToPartitions[consumers[0].Name()]), 3)

	// 1 partition per consumer
	for i := 1; i < 3; i++ {
		consumer, err := client.Subscribe(ConsumerOptions{
			Topic:            topic,
			Name:             fmt.Sprintf("consumer-%d", i),
			SubscriptionName: "my-sub",
			Type:             Failover,
			EventListener:    listener,
		})
		assert.Nil(t, err)
		defer consumer.Close()
		consumers = append(consumers, consumer)
	}
	allConsume(consumers)
	assert.Equal(t, len(nameToPartitions), 3)
	for _, partitionSet := range nameToPartitions {
		assert.Equal(t, len(partitionSet), 1)
	}

	consumers[0].Close()
	// wait broker reschedule active consumers
	time.Sleep(time.Second * 3)
	allConsume(consumers)

	// close consumer won't get notify
	assert.Equal(t, len(nameToPartitions), 3)
	assert.Equal(t, len(nameToPartitions[consumers[1].Name()])+len(nameToPartitions[consumers[2].Name()]), 3)
}

func TestPartitionTopicsConsumerPubSubEncryption(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/testGetPartitions"
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/testGetPartitions/partitions"

	makeHTTPCall(t, http.MethodPut, testURL, "6")

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			Keys: []string{"client-rsa.pem"},
		},
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
		Decryption: &MessageDecryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			ConsumerCryptoFailureAction: crypto.ConsumerCryptoFailureActionFail,
		},
	})
	assert.Nil(t, err)
	defer consumer.Close()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_, err := producer.Send(ctx, &ProducerMessage{
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

		consumer.Ack(msg)
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
		if _, err := producer.Send(context.Background(), &ProducerMessage{
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
		case cm, ok := <-consumer1.Chan():
			if !ok {
				break
			}
			readMsgs++
			payload := string(cm.Message.Payload())
			messages[payload] = struct{}{}
			fmt.Printf("consumer1 msg id is: %v, value is: %s\n", cm.Message.ID(), payload)
			consumer1.Ack(cm.Message)
		case cm, ok := <-consumer2.Chan():
			if !ok {
				break
			}
			readMsgs++
			payload := string(cm.Message.Payload())
			messages[payload] = struct{}{}
			fmt.Printf("consumer2 msg id is: %v, value is: %s\n", cm.Message.ID(), payload)
			consumer2.Ack(cm.Message)
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
	_, err = producer.Send(ctx, &ProducerMessage{
		Payload:   []byte("test"),
		EventTime: et,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	assert.Equal(t, et, msg.EventTime())
	assert.Equal(t, "test", string(msg.Payload()))
}

func TestConsumerWithoutEventTime(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := "test-without-event-time"
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

	_, err = producer.Send(ctx, &ProducerMessage{
		Payload: []byte("test"),
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), msg.EventTime().UnixNano())
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
	defer consumer.Close()

	for msgNum := 0; msgNum < 100; msgNum++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
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

func TestConsumerAck(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := newTopicName()
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
		Type:             Shared,
	})
	assert.Nil(t, err)

	const N = 100

	for i := 0; i < N; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))

		if i < N/2 {
			// Only acks the first half of messages
			consumer.Ack(msg)
		}
	}

	consumer.Close()

	// Subscribe again
	consumer, err = client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
		Type:             Shared,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// We should only receive the 2nd half of messages
	for i := N / 2; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))

		consumer.Ack(msg)
	}
}

func TestConsumerNack(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := newTopicName()
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:               topicName,
		SubscriptionName:    "sub-1",
		Type:                Shared,
		NackRedeliveryDelay: 1 * time.Second,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	const N = 100

	for i := 0; i < N; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))

		if i%2 == 0 {
			// Only acks even messages
			consumer.Ack(msg)
		} else {
			// Fails to process odd messages
			consumer.Nack(msg)
		}
	}

	// Failed messages should be resent

	// We should only receive the odd messages
	for i := 1; i < N; i += 2 {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))

		consumer.Ack(msg)
	}
}

func TestConsumerCompression(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := newTopicName()
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		CompressionType: LZ4,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
	})
	assert.Nil(t, err)
	defer consumer.Close()

	const N = 100

	for i := 0; i < N; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func TestConsumerCompressionWithBatches(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := newTopicName()
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   topicName,
		CompressionType:         ZLib,
		BatchingMaxPublishDelay: 1 * time.Minute,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
	})
	assert.Nil(t, err)
	defer consumer.Close()

	const N = 100

	for i := 0; i < N; i++ {
		producer.SendAsync(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d", i)),
		}, nil)
	}

	producer.Flush()

	for i := 0; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func TestConsumerSeek(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topicName := newTopicName()
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// Use value bigger than 1000 to full-fill queue channel with size 1000 and message channel with size 10
	const N = 1100
	var seekID MessageID
	for i := 0; i < N; i++ {
		id, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)

		if i == N-50 {
			seekID = id
		}
	}

	// Don't consume all messages so some stay in queues
	for i := 0; i < N-20; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("hello-%d", i), string(msg.Payload()))
		consumer.Ack(msg)
	}

	err = consumer.Seek(seekID)
	assert.Nil(t, err)

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprintf("hello-%d", N-50), string(msg.Payload()))
}

func TestConsumerSeekByTime(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topicName := newTopicName()
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "my-sub",
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// Use value bigger than 1000 to full-fill queue channel with size 1000 and message channel with size 10
	const N = 1100
	resetTimeStr := "100s"
	retentionTimeInSecond, err := internal.ParseRelativeTimeInSeconds(resetTimeStr)
	assert.Nil(t, err)

	for i := 0; i < N; i++ {
		_, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)
	}

	// Don't consume all messages so some stay in queues
	for i := 0; i < N-20; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("hello-%d", i), string(msg.Payload()))
		consumer.Ack(msg)
	}

	currentTimestamp := time.Now()
	err = consumer.SeekByTime(currentTimestamp.Add(-retentionTimeInSecond))
	assert.Nil(t, err)

	for i := 0; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("hello-%d", i), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func TestConsumerMetadata(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topic := newTopicName()
	props := map[string]string{
		"key1": "value1",
	}
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
		Properties:       props,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	stats, err := topicStats(topic)
	if err != nil {
		t.Fatal(err)
	}
	subs := stats["subscriptions"].(map[string]interface{})
	cons := subs["my-sub"].(map[string]interface{})["consumers"].([]interface{})[0].(map[string]interface{})
	meta := cons["metadata"].(map[string]interface{})
	assert.Equal(t, len(props), len(meta))
	for k, v := range props {
		mv := meta[k].(string)
		assert.Equal(t, v, mv)
	}
}

// Test for issue #140
// Don't block on receive if the consumer has been closed
func TestConsumerReceiveErrAfterClose(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topicName := newTopicName()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "my-sub",
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer.Close()

	errorCh := make(chan error)
	go func() {
		_, err = consumer.Receive(context.Background())
		errorCh <- err
	}()
	select {
	case <-time.After(200 * time.Millisecond):
	case err = <-errorCh:
	}
	assert.Equal(t, ConsumerClosed, err.(*Error).result)
}

func TestDLQ(t *testing.T) {
	DLQWithProducerOptions(t, nil)
}

func TestDLQWithProducerOptions(t *testing.T) {
	DLQWithProducerOptions(t,
		&ProducerOptions{
			BatchingMaxPublishDelay: 100 * time.Millisecond,
			BatchingMaxSize:         64 * 1024,
			CompressionType:         ZLib,
		})
}

func DLQWithProducerOptions(t *testing.T, prodOpt *ProducerOptions) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	dlqTopic := newTopicName()
	// create consumer on the DLQ topic to verify the routing
	dlqConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:            dlqTopic,
		SubscriptionName: "dlq",
	})
	assert.Nil(t, err)
	defer dlqConsumer.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create consumer
	dlqPolicy := DLQPolicy{
		MaxDeliveries:   3,
		DeadLetterTopic: dlqTopic,
	}
	if prodOpt != nil {
		dlqPolicy.ProducerOptions = *prodOpt
	}
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:               topic,
		SubscriptionName:    "my-sub",
		NackRedeliveryDelay: 1 * time.Second,
		Type:                Shared,
		DLQ:                 &dlqPolicy,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		}
	}

	// receive 10 messages and only ack half-of-them
	for i := 0; i < 10; i++ {
		msg, _ := consumer.Receive(context.Background())

		if i%2 == 0 {
			// ack message
			consumer.Ack(msg)
		} else {
			consumer.Nack(msg)
		}
	}

	// Receive the unacked messages other 2 times, failing at processing
	for i := 0; i < 2; i++ {
		for i := 0; i < 5; i++ {
			msg, _ := consumer.Receive(context.Background())
			consumer.Nack(msg)
		}
	}

	// 5 Messages should now be routed to the DLQ
	for i := 0; i < 5; i++ {
		ctx, canc := context.WithTimeout(context.Background(), 5*time.Second)
		defer canc()
		msg, err := dlqConsumer.Receive(ctx)
		assert.NoError(t, err)
		expectedMsgIdx := 2*i + 1

		expectMsg := fmt.Sprintf("hello-%d", expectedMsgIdx)
		assert.Equal(t, []byte(expectMsg), msg.Payload())
	}

	// No more messages on the DLQ
	ctx, canc := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer canc()
	msg, err := dlqConsumer.Receive(ctx)
	assert.Error(t, err)
	assert.Nil(t, msg)

	// No more messages on regular consumer
	ctx, canc = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer canc()
	msg, err = consumer.Receive(ctx)
	assert.Error(t, err)
	assert.Nil(t, msg)
}

func TestDLQMultiTopics(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	dlqTopic := newTopicName()
	// create consumer on the DLQ topic to verify the routing
	dlqConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:            dlqTopic,
		SubscriptionName: "dlq",
	})
	assert.Nil(t, err)
	defer dlqConsumer.Close()

	topicPrefix := newTopicName()
	topics := make([]string, 10)

	for i := 0; i < 10; i++ {
		topics[i] = fmt.Sprintf("%s-%d", topicPrefix, i)
	}

	ctx := context.Background()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topics:              topics,
		SubscriptionName:    "my-sub",
		NackRedeliveryDelay: 1 * time.Second,
		Type:                Shared,
		DLQ: &DLQPolicy{
			MaxDeliveries:   3,
			DeadLetterTopic: dlqTopic,
			ProducerOptions: ProducerOptions{
				BatchingMaxPublishDelay: 100 * time.Millisecond,
			},
		},
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create one producer for each topic
	producers := make([]Producer, 10)
	for i, topic := range topics {
		producer, err := client.CreateProducer(ProducerOptions{
			Topic: topic,
		})
		assert.Nil(t, err)

		producers[i] = producer
	}

	// send 10 messages
	for i := 0; i < 10; i++ {
		if _, err := producers[i].Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		}
	}

	// receive 10 messages and only ack half-of-them
	for i := 0; i < 10; i++ {
		msg, _ := consumer.Receive(context.Background())

		if i%2 == 0 {
			// ack message
			consumer.Ack(msg)
		} else {
			consumer.Nack(msg)
		}
	}

	// Receive the unacked messages other 2 times, failing at processing
	for i := 0; i < 2; i++ {
		for i := 0; i < 5; i++ {
			msg, _ := consumer.Receive(context.Background())
			consumer.Nack(msg)
		}
	}

	// 5 Messages should now be routed to the DLQ
	for i := 0; i < 5; i++ {
		ctx, canc := context.WithTimeout(context.Background(), 5*time.Second)
		defer canc()
		_, err := dlqConsumer.Receive(ctx)
		assert.NoError(t, err)
	}

	// No more messages on the DLQ
	ctx, canc := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer canc()
	msg, err := dlqConsumer.Receive(ctx)
	assert.Error(t, err)
	assert.Nil(t, msg)

	// No more messages on regular consumer
	ctx, canc = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer canc()
	msg, err = consumer.Receive(ctx)
	assert.Error(t, err)
	assert.Nil(t, msg)
}

func TestRLQ(t *testing.T) {
	topic := newTopicName()
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/" + topic + "/partitions"
	makeHTTPCall(t, http.MethodPut, testURL, "3")

	subName := fmt.Sprintf("sub01-%d", time.Now().Unix())
	maxRedeliveries := 2
	N := 100
	ctx := context.Background()

	client, err := NewClient(ClientOptions{URL: lookupURL})
	assert.Nil(t, err)
	defer client.Close()

	// 1. Pre-produce N messages
	producer, err := client.CreateProducer(ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	defer producer.Close()

	for i := 0; i < N; i++ {
		_, err = producer.Send(ctx, &ProducerMessage{Payload: []byte(fmt.Sprintf("MESSAGE_%d", i))})
		assert.Nil(t, err)
	}

	// 2. Create consumer on the Retry Topic to reconsume N messages (maxRedeliveries+1) times
	rlqConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        Shared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
		DLQ: &DLQPolicy{
			MaxDeliveries: uint32(maxRedeliveries),
		},
		RetryEnable:         true,
		NackRedeliveryDelay: 1 * time.Second,
	})
	assert.Nil(t, err)
	defer rlqConsumer.Close()

	rlqReceived := 0
	for rlqReceived < N*(maxRedeliveries+1) {
		msg, err := rlqConsumer.Receive(ctx)
		assert.Nil(t, err)
		rlqConsumer.ReconsumeLater(msg, 1*time.Second)
		rlqReceived++
	}
	fmt.Println("retry consumed:", rlqReceived) // 300

	// No more messages on the Retry Topic
	rlqCtx, rlqCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer rlqCancel()
	msg, err := rlqConsumer.Receive(rlqCtx)
	assert.Error(t, err)
	assert.Nil(t, msg)

	// 3. Create consumer on the DLQ topic to verify the routing
	dlqConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "persistent://public/default/" + subName + "-DLQ",
		SubscriptionName:            subName,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer dlqConsumer.Close()

	dlqReceived := 0
	for dlqReceived < N {
		msg, err := dlqConsumer.Receive(ctx)
		assert.Nil(t, err)
		dlqConsumer.Ack(msg)
		dlqReceived++
	}
	fmt.Println("dlq received:", dlqReceived) // 100

	// No more messages on the DLQ Topic
	dlqCtx, dlqCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer dlqCancel()
	msg, err = dlqConsumer.Receive(dlqCtx)
	assert.Error(t, err)
	assert.Nil(t, msg)

	// 4. No more messages for same subscription
	checkConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        Shared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer checkConsumer.Close()

	checkCtx, checkCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer checkCancel()
	checkMsg, err := checkConsumer.Receive(checkCtx)
	assert.Error(t, err)
	assert.Nil(t, checkMsg)
}

func TestAckWithResponse(t *testing.T) {
	now := time.Now().Unix()
	topic01 := fmt.Sprintf("persistent://public/default/topic-%d-01", now)
	ctx := context.Background()

	client, err := NewClient(ClientOptions{URL: lookupURL})
	assert.Nil(t, err)
	defer client.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic01,
		SubscriptionName:            "my-sub",
		Type:                        Shared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
		AckWithResponse:             true,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	producer01, err := client.CreateProducer(ProducerOptions{Topic: topic01})
	assert.Nil(t, err)
	defer producer01.Close()
	for i := 0; i < 10; i++ {
		_, err = producer01.Send(ctx, &ProducerMessage{Payload: []byte(fmt.Sprintf("MSG_01_%d", i))})
		assert.Nil(t, err)
	}

	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		err = consumer.Ack(msg)
		assert.Nil(t, err)
	}
}

func TestRLQMultiTopics(t *testing.T) {
	now := time.Now().Unix()
	topic01 := fmt.Sprintf("persistent://public/default/topic-%d-1", now)
	topic02 := fmt.Sprintf("topic-%d-2", now)
	topics := []string{topic01, topic02}

	subName := fmt.Sprintf("sub01-%d", time.Now().Unix())
	maxRedeliveries := 2
	N := 100
	ctx := context.Background()

	client, err := NewClient(ClientOptions{URL: lookupURL})
	assert.Nil(t, err)
	defer client.Close()

	// subscribe multi topics with Retry Topics
	rlqConsumer, err := client.Subscribe(ConsumerOptions{
		Topics:                      topics,
		SubscriptionName:            subName,
		Type:                        Shared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
		DLQ:                         &DLQPolicy{MaxDeliveries: uint32(maxRedeliveries)},
		RetryEnable:                 true,
		NackRedeliveryDelay:         1 * time.Second,
	})
	assert.Nil(t, err)
	defer rlqConsumer.Close()

	// subscribe DLQ Topic
	dlqConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       subName + "-DLQ",
		SubscriptionName:            subName,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer dlqConsumer.Close()

	// create multi producers
	producer01, err := client.CreateProducer(ProducerOptions{Topic: topic01})
	assert.Nil(t, err)
	defer producer01.Close()

	producer02, err := client.CreateProducer(ProducerOptions{Topic: topic02})
	assert.Nil(t, err)
	defer producer02.Close()

	// 1. Pre-produce N messages for every topic
	for i := 0; i < N; i++ {
		_, err = producer01.Send(ctx, &ProducerMessage{Payload: []byte(fmt.Sprintf("MSG_01_%d", i))})
		assert.Nil(t, err)
		_, err = producer02.Send(ctx, &ProducerMessage{Payload: []byte(fmt.Sprintf("MSG_02_%d", i))})
		assert.Nil(t, err)
	}

	// 2. Create consumer on the Retry Topics to reconsume 2*N messages (maxRedeliveries+1) times
	rlqReceived := 0
	for rlqReceived < 2*N*(maxRedeliveries+1) {
		msg, err := rlqConsumer.Receive(ctx)
		assert.Nil(t, err)
		rlqConsumer.ReconsumeLater(msg, 1*time.Second)
		rlqReceived++
	}
	fmt.Println("retry consumed:", rlqReceived) // 600

	// No more messages on the Retry Topic
	rlqCtx, rlqCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer rlqCancel()
	msg, err := rlqConsumer.Receive(rlqCtx)
	assert.Error(t, err)
	assert.Nil(t, msg)

	// 3. Create consumer on the DLQ topic to verify the routing
	dlqReceived := 0
	for dlqReceived < 2*N {
		msg, err := dlqConsumer.Receive(ctx)
		assert.Nil(t, err)
		dlqConsumer.Ack(msg)
		dlqReceived++
	}
	fmt.Println("dlq received:", dlqReceived) // 200

	// No more messages on the DLQ Topic
	dlqCtx, dlqCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer dlqCancel()
	msg, err = dlqConsumer.Receive(dlqCtx)
	assert.Error(t, err)
	assert.Nil(t, msg)

	// 4. No more messages for same subscription
	checkConsumer, err := client.Subscribe(ConsumerOptions{
		Topics:                      []string{topic01, topic02},
		SubscriptionName:            subName,
		Type:                        Shared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer checkConsumer.Close()

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	checkMsg, err := checkConsumer.Receive(timeoutCtx)
	assert.Error(t, err)
	assert.Nil(t, checkMsg)
}

func TestRLQSpecifiedPartitionTopic(t *testing.T) {
	topic := newTopicName()
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/" + topic + "/partitions"
	makeHTTPCall(t, http.MethodPut, testURL, "1")

	normalTopic := "persistent://public/default/" + topic
	partitionTopic := normalTopic + "-partition-0"

	subName := fmt.Sprintf("sub01-%d", time.Now().Unix())
	maxRedeliveries := 2
	N := 100
	ctx := context.Background()

	client, err := NewClient(ClientOptions{URL: lookupURL})
	assert.Nil(t, err)
	defer client.Close()

	// subscribe topic with partition
	rlqConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       partitionTopic,
		SubscriptionName:            subName,
		Type:                        Shared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
		DLQ:                         &DLQPolicy{MaxDeliveries: uint32(maxRedeliveries)},
		RetryEnable:                 true,
		NackRedeliveryDelay:         1 * time.Second,
	})
	assert.Nil(t, err)
	defer rlqConsumer.Close()

	// subscribe DLQ Topic
	dlqConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       subName + "-DLQ",
		SubscriptionName:            subName,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer dlqConsumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{Topic: normalTopic})
	assert.Nil(t, err)
	defer producer.Close()

	// 1. Pre-produce N messages
	for i := 0; i < N; i++ {
		_, err = producer.Send(ctx, &ProducerMessage{Payload: []byte(fmt.Sprintf("MSG_01_%d", i))})
		assert.Nil(t, err)
	}

	// 2. Create consumer on the Retry Topics to reconsume N messages (maxRedeliveries+1) times
	rlqReceived := 0
	for rlqReceived < N*(maxRedeliveries+1) {
		msg, err := rlqConsumer.Receive(ctx)
		assert.Nil(t, err)
		rlqConsumer.ReconsumeLater(msg, 1*time.Second)
		rlqReceived++
	}
	fmt.Println("retry consumed:", rlqReceived) // 300

	// No more messages on the Retry Topic
	rlqCtx, rlqCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer rlqCancel()
	msg, err := rlqConsumer.Receive(rlqCtx)
	assert.Error(t, err)
	assert.Nil(t, msg)

	// 3. Create consumer on the DLQ topic to verify the routing
	dlqReceived := 0
	for dlqReceived < N {
		msg, err := dlqConsumer.Receive(ctx)
		assert.Nil(t, err)
		dlqConsumer.Ack(msg)
		dlqReceived++
	}
	fmt.Println("dlq received:", dlqReceived) // 100

	// No more messages on the DLQ Topic
	dlqCtx, dlqCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer dlqCancel()
	msg, err = dlqConsumer.Receive(dlqCtx)
	assert.Error(t, err)
	assert.Nil(t, msg)

	// 4. No more messages for same subscription
	checkConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       normalTopic,
		SubscriptionName:            subName,
		Type:                        Shared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer checkConsumer.Close()

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	checkMsg, err := checkConsumer.Receive(timeoutCtx)
	assert.Error(t, err)
	assert.Nil(t, checkMsg)
}

func TestGetDeliveryCount(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:               topic,
		SubscriptionName:    "my-sub",
		NackRedeliveryDelay: 1 * time.Second,
		Type:                Shared,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		}
	}

	// receive 10 messages and only ack half-of-them
	for i := 0; i < 10; i++ {
		msg, _ := consumer.Receive(context.Background())

		if i%2 == 0 {
			// ack message
			consumer.Ack(msg)
		} else {
			consumer.Nack(msg)
		}
	}

	// Receive the unacked messages other 2 times, failing at processing
	for i := 0; i < 2; i++ {
		var msg Message
		for i := 0; i < 5; i++ {
			msg, err = consumer.Receive(context.Background())
			assert.Nil(t, err)
			consumer.Nack(msg)
		}
		assert.Equal(t, uint32(i+1), msg.RedeliveryCount())
	}

	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), msg.RedeliveryCount())
}

func TestConsumerAddTopicPartitions(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/" + topic + "/partitions"
	makeHTTPCall(t, http.MethodPut, testURL, "3")

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		MessageRouter: func(msg *ProducerMessage, topicMetadata TopicMetadata) int {
			// The message key will contain the partition id where to route
			i, err := strconv.Atoi(msg.Key)
			assert.NoError(t, err)
			return i
		},
		PartitionsAutoDiscoveryInterval: 100 * time.Millisecond,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:               topic,
		SubscriptionName:    "my-sub",
		AutoDiscoveryPeriod: 100 * time.Millisecond,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// Increase number of partitions to 10
	makeHTTPCall(t, http.MethodPost, testURL, "10")

	// Wait for the producer/consumers to pick up the change
	time.Sleep(1 * time.Second)

	// Publish messages ensuring that they will go to all the partitions
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_, err := producer.Send(ctx, &ProducerMessage{
			Key:     fmt.Sprintf("%d", i),
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

		consumer.Ack(msg)
	}

	assert.Equal(t, len(msgs), 10)
}

func TestConsumerNegativeReceiverQueueSize(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:             topic,
		SubscriptionName:  "my-sub",
		ReceiverQueueSize: -1,
	})
	defer func() {
		if consumer != nil {
			consumer.Close()
		}
	}()

	assert.Nil(t, err)
}

func TestProducerName(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	producerName := "test-producer-name"

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		Name:  producerName,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
	})

	assert.Nil(t, err)
	defer consumer.Close()

	// publish 10 messages to topic
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)
	}

	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)

		assert.Equal(t, msg.ProducerName(), producerName)
		consumer.Ack(msg)
	}
}

type noopConsumerInterceptor struct{}

func (noopConsumerInterceptor) BeforeConsume(message ConsumerMessage) {}

func (noopConsumerInterceptor) OnAcknowledge(consumer Consumer, msgID MessageID) {}

func (noopConsumerInterceptor) OnNegativeAcksSend(consumer Consumer, msgIDs []MessageID) {}

// copyPropertyInterceptor copy all keys in message properties map and add a suffix
type copyPropertyInterceptor struct {
	suffix string
}

func (x copyPropertyInterceptor) BeforeConsume(message ConsumerMessage) {
	properties := message.Properties()
	copy := make(map[string]string, len(properties))
	for k, v := range properties {
		copy[k+x.suffix] = v
	}
	for ck, v := range copy {
		properties[ck] = v
	}
}

func (copyPropertyInterceptor) OnAcknowledge(consumer Consumer, msgID MessageID) {}

func (copyPropertyInterceptor) OnNegativeAcksSend(consumer Consumer, msgIDs []MessageID) {}

type metricConsumerInterceptor struct {
	ackn  int32
	nackn int32
}

func (x *metricConsumerInterceptor) BeforeConsume(message ConsumerMessage) {}

func (x *metricConsumerInterceptor) OnAcknowledge(consumer Consumer, msgID MessageID) {
	atomic.AddInt32(&x.ackn, 1)
}

func (x *metricConsumerInterceptor) OnNegativeAcksSend(consumer Consumer, msgIDs []MessageID) {
	atomic.AddInt32(&x.nackn, int32(len(msgIDs)))
}

func TestConsumerWithInterceptors(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	metric := &metricConsumerInterceptor{}

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:               topic,
		SubscriptionName:    "my-sub",
		Type:                Exclusive,
		NackRedeliveryDelay: time.Second, // for testing nack
		Interceptors: ConsumerInterceptors{
			noopConsumerInterceptor{},
			copyPropertyInterceptor{suffix: "-copy"},
			metric,
		},
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
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Key:     "pulsar",
			Properties: map[string]string{
				"key-1": "pulsar-1",
			},
		}); err != nil {
			log.Fatal(err)
		}
	}

	var nackIds []MessageID
	// receive 10 messages
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		expectMsg := fmt.Sprintf("hello-%d", i)
		expectProperties := map[string]string{
			"key-1":      "pulsar-1",
			"key-1-copy": "pulsar-1", // check properties copy by interceptor
		}
		assert.Equal(t, []byte(expectMsg), msg.Payload())
		assert.Equal(t, "pulsar", msg.Key())
		assert.Equal(t, expectProperties, msg.Properties())

		// ack message
		if i%2 == 0 {
			consumer.Ack(msg)
		} else {
			nackIds = append(nackIds, msg.ID())
		}
	}
	assert.Equal(t, int32(5), atomic.LoadInt32(&metric.ackn))

	for i := range nackIds {
		consumer.NackID(nackIds[i])
	}

	// receive 5 nack messages
	for i := 0; i < 5; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		expectMsg := fmt.Sprintf("hello-%d", i*2+1)
		expectProperties := map[string]string{
			"key-1":      "pulsar-1",
			"key-1-copy": "pulsar-1", // check properties copy by interceptor
		}
		assert.Equal(t, []byte(expectMsg), msg.Payload())
		assert.Equal(t, "pulsar", msg.Key())
		assert.Equal(t, expectProperties, msg.Properties())

		// ack message
		consumer.Ack(msg)
	}

	assert.Equal(t, int32(5), atomic.LoadInt32(&metric.nackn))
}

func TestConsumerName(t *testing.T) {
	assert := assert.New(t)

	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(err)
	defer client.Close()

	topic := newTopicName()

	// create consumer
	consumerName := "test-consumer-name"
	consumer, err := client.Subscribe(ConsumerOptions{
		Name:             consumerName,
		Topic:            topic,
		SubscriptionName: "my-sub",
	})

	assert.Nil(err)
	defer consumer.Close()

	assert.Equal(consumerName, consumer.Name())
}

func TestKeyBasedBatchProducerConsumerKeyShared(t *testing.T) {
	const MsgBatchCount = 100
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/test-key-based-batch-with-key-shared"

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
		Topic:               topic,
		DisableBatching:     false,
		BatcherBuilderType:  KeyBasedBatchBuilder,
		BatchingMaxMessages: 10,
	})
	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()
	keys := []string{"key1", "key2", "key3"}
	for i := 0; i < MsgBatchCount; i++ {
		for _, k := range keys {
			producer.SendAsync(ctx, &ProducerMessage{
				Key:     k,
				Payload: []byte(fmt.Sprintf("value-%d", i)),
			}, func(id MessageID, producerMessage *ProducerMessage, err error) {
				assert.Nil(t, err)
			},
			)
		}
	}

	receivedConsumer1 := 0
	receivedConsumer2 := 0
	consumer1Keys := make(map[string]int)
	consumer2Keys := make(map[string]int)
	for (receivedConsumer1 + receivedConsumer2) < 300 {
		select {
		case cm, ok := <-consumer1.Chan():
			if !ok {
				break
			}
			receivedConsumer1++
			cnt := 0
			if _, has := consumer1Keys[cm.Key()]; has {
				cnt = consumer1Keys[cm.Key()]
			}
			assert.Equal(
				t, fmt.Sprintf("value-%d", cnt),
				string(cm.Payload()),
			)
			consumer1Keys[cm.Key()] = cnt + 1
			consumer1.Ack(cm.Message)
		case cm, ok := <-consumer2.Chan():
			if !ok {
				break
			}
			receivedConsumer2++
			cnt := 0
			if _, has := consumer2Keys[cm.Key()]; has {
				cnt = consumer2Keys[cm.Key()]
			}
			assert.Equal(
				t, fmt.Sprintf("value-%d", cnt),
				string(cm.Payload()),
			)
			consumer2Keys[cm.Key()] = cnt + 1
			consumer2.Ack(cm.Message)
		}
	}

	assert.NotEqual(t, 0, receivedConsumer1)
	assert.NotEqual(t, 0, receivedConsumer2)
	assert.Equal(t, len(consumer1Keys)*MsgBatchCount, receivedConsumer1)
	assert.Equal(t, len(consumer2Keys)*MsgBatchCount, receivedConsumer2)

	fmt.Printf("TestKeyBasedBatchProducerConsumerKeyShared received messages consumer1: %d consumser2: %d\n",
		receivedConsumer1, receivedConsumer2)
	assert.Equal(t, 300, receivedConsumer1+receivedConsumer2)

	fmt.Printf("TestKeyBasedBatchProducerConsumerKeyShared received messages keys consumer1: %v consumser2: %v\n",
		consumer1Keys, consumer2Keys)
}

func TestOrderingOfKeyBasedBatchProducerConsumerKeyShared(t *testing.T) {
	const MsgBatchCount = 10
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/test-ordering-of-key-based-batch-with-key-shared"

	consumer1, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "sub-1",
		Type:             KeyShared,
	})
	assert.Nil(t, err)
	defer consumer1.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   topic,
		DisableBatching:         false,
		BatcherBuilderType:      KeyBasedBatchBuilder,
		BatchingMaxMessages:     30,
		BatchingMaxPublishDelay: time.Second * 5,
	})
	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()
	keys := []string{"key1", "key2", "key3"}
	for i := 0; i < MsgBatchCount; i++ {
		for _, k := range keys {
			producer.SendAsync(ctx, &ProducerMessage{
				Key:     k,
				Payload: []byte(fmt.Sprintf("value-%d", i)),
			}, func(id MessageID, producerMessage *ProducerMessage, err error) {
				assert.Nil(t, err)
			},
			)
		}
	}

	var receivedKey string
	var receivedMessageIndex int
	for i := 0; i < len(keys)*MsgBatchCount; i++ {
		cm, ok := <-consumer1.Chan()
		if !ok {
			break
		}
		if receivedKey != cm.Key() {
			receivedKey = cm.Key()
			receivedMessageIndex = 0
		}
		assert.Equal(
			t, fmt.Sprintf("value-%d", receivedMessageIndex%10),
			string(cm.Payload()),
		)
		consumer1.Ack(cm.Message)
		receivedMessageIndex++
	}

	// Test OrderingKey
	for i := 0; i < MsgBatchCount; i++ {
		for _, k := range keys {
			u := uuid.New()
			producer.SendAsync(ctx, &ProducerMessage{
				Key:         u.String(),
				OrderingKey: k,
				Payload:     []byte(fmt.Sprintf("value-%d", i)),
			}, func(id MessageID, producerMessage *ProducerMessage, err error) {
				assert.Nil(t, err)
			},
			)
		}
	}

	receivedKey = ""
	receivedMessageIndex = 0
	for i := 0; i < len(keys)*MsgBatchCount; i++ {
		cm, ok := <-consumer1.Chan()
		if !ok {
			break
		}
		if receivedKey != cm.OrderingKey() {
			receivedKey = cm.OrderingKey()
			receivedMessageIndex = 0
		}
		assert.Equal(
			t, fmt.Sprintf("value-%d", receivedMessageIndex%10),
			string(cm.Payload()),
		)
		consumer1.Ack(cm.Message)
		receivedMessageIndex++
	}

}

func TestConsumerKeySharedWithOrderingKey(t *testing.T) {
	client, err := NewClient(
		ClientOptions{
			URL: lookupURL,
		},
	)
	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/test-key-shared-with-ordering-key"

	consumer1, err := client.Subscribe(
		ConsumerOptions{
			Topic:            topic,
			SubscriptionName: "sub-1",
			Type:             KeyShared,
		},
	)
	assert.Nil(t, err)
	defer consumer1.Close()

	consumer2, err := client.Subscribe(
		ConsumerOptions{
			Topic:            topic,
			SubscriptionName: "sub-1",
			Type:             KeyShared,
		},
	)
	assert.Nil(t, err)
	defer consumer2.Close()

	// create producer
	producer, err := client.CreateProducer(
		ProducerOptions{
			Topic:           topic,
			DisableBatching: true,
		},
	)
	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		u := uuid.New()
		_, err := producer.Send(
			ctx, &ProducerMessage{
				Key:         u.String(),
				OrderingKey: fmt.Sprintf("key-shared-%d", i%3),
				Payload:     []byte(fmt.Sprintf("value-%d", i)),
			},
		)
		assert.Nil(t, err)
	}

	receivedConsumer1 := 0
	receivedConsumer2 := 0
	for (receivedConsumer1 + receivedConsumer2) < 100 {
		select {
		case cm, ok := <-consumer1.Chan():
			if !ok {
				break
			}
			receivedConsumer1++
			consumer1.Ack(cm.Message)
		case cm, ok := <-consumer2.Chan():
			if !ok {
				break
			}
			receivedConsumer2++
			consumer2.Ack(cm.Message)
		}
	}

	assert.NotEqual(t, 0, receivedConsumer1)
	assert.NotEqual(t, 0, receivedConsumer2)

	fmt.Printf(
		"TestConsumerKeySharedWithOrderingKey received messages consumer1: %d consumser2: %d\n",
		receivedConsumer1, receivedConsumer2,
	)
	assert.Equal(t, 100, receivedConsumer1+receivedConsumer2)
}

func TestProducerConsumerRSAEncryption(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := fmt.Sprintf("my-topic-enc-%v", time.Now().Nanosecond())

	cryptoConsumer, err := client.Subscribe(ConsumerOptions{
		Topic: topic,
		Decryption: &MessageDecryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			ConsumerCryptoFailureAction: crypto.ConsumerCryptoFailureActionFail,
		},
		SubscriptionName: "crypto-subscription",
		Schema:           NewStringSchema(nil),
	})

	assert.Nil(t, err)

	normalConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "normal-subscription",
		Schema:           NewStringSchema(nil),
	})

	assert.Nil(t, err)

	cryptoProducer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			Keys: []string{"client-rsa.pem"},
		},
		Schema: NewStringSchema(nil),
	})

	assert.Nil(t, err)

	msgFormat := "my-message-%v"

	totalMessages := 10

	ctx := context.Background()

	for i := 0; i < totalMessages; i++ {
		_, err := cryptoProducer.Send(ctx, &ProducerMessage{
			Value: fmt.Sprintf(msgFormat, i),
		})

		assert.Nil(t, err)
	}

	// try to consume with normal consumer
	normalConsumerCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	msg, err := normalConsumer.Receive(normalConsumerCtx)
	// msg should be null as the consumer will not be able to decrypt
	assert.NotNil(t, err)
	assert.Nil(t, msg)

	// try to consume the message by crypto consumer
	// consumer should be able to read all the messages
	var actualMessage *string
	for i := 0; i < totalMessages; i++ {
		msg, err := cryptoConsumer.Receive(ctx)
		fmt.Println(msg)
		assert.Nil(t, err)
		expectedMsg := fmt.Sprintf(msgFormat, i)
		err = msg.GetSchemaValue(&actualMessage)
		assert.Nil(t, err)
		assert.Equal(t, expectedMsg, *actualMessage)
		cryptoConsumer.Ack(msg)
	}
}

func TestProducerConsumerRSAEncryptionWithCompression(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := fmt.Sprintf("my-topic-enc-%v", time.Now().Nanosecond())

	cryptoConsumer, err := client.Subscribe(ConsumerOptions{
		Topic: topic,
		Decryption: &MessageDecryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
		},
		SubscriptionName: "crypto-subscription",
		Schema:           NewStringSchema(nil),
	})

	assert.Nil(t, err)

	normalConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "normal-subscription",
		Schema:           NewStringSchema(nil),
	})

	assert.Nil(t, err)

	cryptoProducer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			Keys: []string{"client-rsa.pem"},
		},
		Schema:          NewStringSchema(nil),
		CompressionType: LZ4,
	})

	assert.Nil(t, err)

	msgFormat := "my-message-%v"

	totalMessages := 10

	ctx := context.Background()

	for i := 0; i < totalMessages; i++ {
		_, err := cryptoProducer.Send(ctx, &ProducerMessage{
			Value: fmt.Sprintf(msgFormat, i),
		})

		assert.Nil(t, err)
	}

	// try to consume with normal consumer
	normalConsumerCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	msg, err := normalConsumer.Receive(normalConsumerCtx)
	// msg should be null as the consumer will not be able to decrypt
	assert.NotNil(t, err)
	assert.Nil(t, msg)

	// try to consume the message by crypto consumer
	// consumer should be able to read all the messages
	var actualMessage *string
	for i := 0; i < totalMessages; i++ {
		msg, err := cryptoConsumer.Receive(ctx)
		assert.Nil(t, err)
		expectedMsg := fmt.Sprintf(msgFormat, i)
		err = msg.GetSchemaValue(&actualMessage)
		assert.Nil(t, err)
		assert.Equal(t, expectedMsg, *actualMessage)
		cryptoConsumer.Ack(msg)
	}
}

func TestBatchProducerConsumerRSAEncryptionWithCompression(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := fmt.Sprintf("my-topic-enc-%v", time.Now().Nanosecond())

	cryptoConsumer, err := client.Subscribe(ConsumerOptions{
		Topic: topic,
		Decryption: &MessageDecryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
		},
		SubscriptionName: "crypto-subscription",
		Schema:           NewStringSchema(nil),
	})

	assert.Nil(t, err)

	normalConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "normal-subscription",
		Schema:           NewStringSchema(nil),
	})

	assert.Nil(t, err)
	batchSize := 2
	cryptoProducer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			Keys: []string{"client-rsa.pem"},
		},
		Schema:              NewStringSchema(nil),
		CompressionType:     LZ4,
		DisableBatching:     false,
		BatchingMaxMessages: uint(batchSize),
	})

	assert.Nil(t, err)

	msgFormat := "my-message-%v"

	totalMessages := 10

	ctx := context.Background()

	for i := 0; i < totalMessages; i++ {
		_, err := cryptoProducer.Send(ctx, &ProducerMessage{
			Value: fmt.Sprintf(msgFormat, i),
		})

		assert.Nil(t, err)
	}

	// try to consume with normal consumer
	normalConsumerCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	msg, err := normalConsumer.Receive(normalConsumerCtx)
	// msg should be null as the consumer will not be able to decrypt
	assert.NotNil(t, err)
	assert.Nil(t, msg)

	// try to consume the message by crypto consumer
	// consumer should be able to read all the messages
	var actualMessage *string
	for i := 0; i < totalMessages; i++ {
		msg, err := cryptoConsumer.Receive(ctx)
		assert.Nil(t, err)
		expectedMsg := fmt.Sprintf(msgFormat, i)
		err = msg.GetSchemaValue(&actualMessage)
		assert.Nil(t, err)
		assert.Equal(t, expectedMsg, *actualMessage)
		cryptoConsumer.Ack(msg)
	}
}

func TestProducerConsumerRedeliveryOfFailedEncryptedMessages(t *testing.T) {
	// create new client instance for each producer and consumer
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	clientCryptoConsumer, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.Nil(t, err)
	defer clientCryptoConsumer.Close()

	clientCryptoConsumerInvalidKeyReader, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.Nil(t, err)
	defer clientCryptoConsumerInvalidKeyReader.Close()

	clientcryptoConsumerNoKeyReader, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.Nil(t, err)
	defer clientcryptoConsumerNoKeyReader.Close()

	topic := fmt.Sprintf("my-topic-enc-%v", time.Now().Nanosecond())

	cryptoProducer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			Keys: []string{"client-rsa.pem"},
		},
		CompressionType: LZ4,
		Schema:          NewStringSchema(nil),
	})
	assert.Nil(t, err)

	sharedSubscription := "crypto-shared-subscription"

	cryptoConsumer, err := clientCryptoConsumer.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sharedSubscription,
		Decryption: &MessageDecryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
		},
		Schema:              NewStringSchema(nil),
		Type:                Shared,
		NackRedeliveryDelay: 1 * time.Second,
	})
	assert.Nil(t, err)

	cryptoConsumerInvalidKeyReader, err := clientCryptoConsumerInvalidKeyReader.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sharedSubscription,
		Decryption: &MessageDecryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa_invalid.pem"),
		},
		Schema:              NewStringSchema(nil),
		Type:                Shared,
		NackRedeliveryDelay: 1 * time.Second,
	})
	assert.Nil(t, err)

	cryptoConsumerNoKeyReader, err := clientcryptoConsumerNoKeyReader.Subscribe(ConsumerOptions{
		Topic:               topic,
		SubscriptionName:    sharedSubscription,
		Schema:              NewStringSchema(nil),
		Type:                Shared,
		NackRedeliveryDelay: 1 * time.Second,
	})
	assert.Nil(t, err)

	totalMessages := 5
	message := "my-message-%v"
	// since messages can be in random order
	// map can be used to check if all the messages are received
	messageMap := map[string]struct{}{}

	// producer messages
	for i := 0; i < totalMessages; i++ {
		mid, err := cryptoProducer.Send(context.Background(), &ProducerMessage{
			Value: fmt.Sprintf(message, i),
		})
		assert.Nil(t, err)
		fmt.Printf("Sent : %v\n", mid)
	}

	// Consuming from consumer 2 and 3
	// no message should be returned since they can't decrypt the message
	ctxWithTimeOut1, c1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer c1()

	ctxWithTimeOut2, c2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer c2()

	// try to consume messages
	msg, err := cryptoConsumerInvalidKeyReader.Receive(ctxWithTimeOut1)
	assert.NotNil(t, err)
	assert.Nil(t, msg)

	msg, err = cryptoConsumerNoKeyReader.Receive(ctxWithTimeOut2)
	assert.NotNil(t, err)
	assert.Nil(t, msg)

	cryptoConsumerInvalidKeyReader.Close()
	cryptoConsumerNoKeyReader.Close()

	// try to consume by consumer1
	// all the messages would by received by it
	var receivedMsg *string
	for i := 0; i < totalMessages; i++ {
		m, err := cryptoConsumer.Receive(context.Background())
		assert.Nil(t, err)
		err = m.GetSchemaValue(&receivedMsg)
		assert.Nil(t, err)
		messageMap[*receivedMsg] = struct{}{}
		cryptoConsumer.Ack(m)
		fmt.Printf("Received : %v\n", m.ID())
	}

	// check if all messages were received
	for i := 0; i < totalMessages; i++ {
		key := fmt.Sprintf(message, i)
		_, ok := messageMap[key]
		assert.True(t, ok)
	}
}

func TestRSAEncryptionFailure(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.Nil(t, err)
	client.Close()

	topic := fmt.Sprintf("my-topic-enc-%v", time.Now().Nanosecond())

	// 1. invalid key name
	// create producer with invalid key
	// producer creation succeeds but message sending should fail with an error
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa_invalid.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			Keys: []string{"client-rsa.pem"},
		},
		Schema:          NewStringSchema(nil),
		DisableBatching: true,
	})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	// sending of message should fail with an error, since invalid rsa keys are configured
	mid, err := producer.Send(context.Background(), &ProducerMessage{
		Value: "some-message",
	})

	assert.Nil(t, mid)
	assert.NotNil(t, err)
	producer.Close()

	// 2. Producer with valid key name
	producer, err = client.CreateProducer(ProducerOptions{
		Topic: topic,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			Keys: []string{"client-rsa.pem"},
		},
		Schema:          NewStringSchema(nil),
		DisableBatching: true,
	})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	subscriptionName := "enc-failure-subcription"
	totalMessages := 10

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
	})
	assert.Nil(t, err)

	messageFormat := "my-message-%v"
	for i := 0; i < totalMessages; i++ {
		_, err := producer.Send(context.Background(), &ProducerMessage{
			Value: fmt.Sprintf(messageFormat, i),
		})
		assert.Nil(t, err)
	}

	// 3. KeyReader is not set by the consumer
	// Receive should fail since KeyReader is not setup
	// because default behaviour of consumer is fail receiving message if error in decryption
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msg, err := consumer.Receive(ctx)
	assert.NotNil(t, err)
	assert.Nil(t, msg, "Receive should have failed with no keyreader")

	// 4. Set consumer config to consume even if decryption fails
	consumer.Close()
	consumer, err = client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
		Decryption: &MessageDecryptionInfo{
			ConsumerCryptoFailureAction: crypto.ConsumerCryptoFailureActionConsume,
		},
		Schema: NewStringSchema(nil),
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	for i := 0; i < totalMessages-1; i++ {
		expectedMessage := fmt.Sprintf(messageFormat, i)
		msg, err = consumer.Receive(ctx2)
		assert.Nil(t, err)
		assert.NotNil(t, msg)

		receivedMsg := string(msg.Payload())
		assert.NotEqual(t, expectedMessage, receivedMsg, fmt.Sprintf(`Received encrypted message [%v]
	should not match the expected message  [%v]`, expectedMessage, receivedMsg))
		// verify the message contains Encryption context
		assert.NotEmpty(t, msg.GetEncryptionContext(),
			"Encrypted message which is failed to decrypt must contain EncryptionContext")
		consumer.Ack(msg)
	}

	// 5. discard action on decryption failure
	consumer.Close()
	consumer, err = client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
		Decryption: &MessageDecryptionInfo{
			ConsumerCryptoFailureAction: crypto.ConsumerCryptoFailureActionDiscard,
		},
		Schema: NewStringSchema(nil),
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	ctx3, cancel3 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel3()

	msg, err = consumer.Receive(ctx3)
	assert.NotNil(t, err)
	assert.Nil(t, msg, "Message received even aftet ConsumerCryptoFailureAction.Discard is set.")
}

func TestConsumerCompressionWithRSAEncryption(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := newTopicName()
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		CompressionType: LZ4,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			Keys: []string{"enc-compress-app.key"},
		},
	})

	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
		Decryption: &MessageDecryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
		},
	})

	assert.Nil(t, err)
	defer consumer.Close()

	const N = 100

	for i := 0; i < N; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func TestBatchMessageReceiveWithCompressionAndRSAEcnryption(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := "persistent://public/default/receive-batch-comp-enc"
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
		CompressionType:     LZ4,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			Keys: []string{"batch-encryption-app.key"},
		},
	})
	assert.Nil(t, err)
	assert.Equal(t, topicName, producer.Topic())
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: subName,
		Decryption: &MessageDecryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
		},
	})

	assert.Nil(t, err)
	defer consumer.Close()

	count := 0
	for i := 0; i < numOfMessages; i++ {
		messageContent := prefix + fmt.Sprintf("%d", i)
		msg := &ProducerMessage{
			Payload: []byte(messageContent),
		}
		_, err := producer.Send(ctx, msg)
		assert.Nil(t, err)
	}

	for i := 0; i < numOfMessages; i++ {
		msg, err := consumer.Receive(ctx)
		fmt.Printf("received : %v\n", string(msg.Payload()))
		assert.Nil(t, err)
		consumer.Ack(msg)
		count++
	}

	assert.Equal(t, count, numOfMessages)
}

type EncKeyReader struct {
	publicKeyPath  string
	privateKeyPath string
	metaMap        map[string]string
}

func NewEncKeyReader(publicKeyPath, privateKeyPath string) *EncKeyReader {
	metaMap := map[string]string{
		"version": "1.0",
	}

	return &EncKeyReader{
		publicKeyPath:  publicKeyPath,
		privateKeyPath: privateKeyPath,
		metaMap:        metaMap,
	}
}

// GetPublicKey read public key from the given path
func (d *EncKeyReader) PublicKey(keyName string, keyMeta map[string]string) (*crypto.EncryptionKeyInfo, error) {
	return readKey(keyName, d.publicKeyPath, d.metaMap)
}

// GetPrivateKey read private key from the given path
func (d *EncKeyReader) PrivateKey(keyName string, keyMeta map[string]string) (*crypto.EncryptionKeyInfo, error) {
	return readKey(keyName, d.privateKeyPath, d.metaMap)
}

func readKey(keyName, path string, keyMeta map[string]string) (*crypto.EncryptionKeyInfo, error) {
	key, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return crypto.NewEncryptionKeyInfo(keyName, key, keyMeta), nil
}

func TestConsumerEncryptionWithoutKeyReader(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	encryptionKeyName := "client-rsa.pem"

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: NewEncKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			Keys: []string{encryptionKeyName},
		},
		CompressionType: LZ4,
		Schema:          NewStringSchema(nil),
	})

	assert.Nil(t, err)
	assert.NotNil(t, producer)

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-subscription-name",
		Decryption: &MessageDecryptionInfo{
			ConsumerCryptoFailureAction: crypto.ConsumerCryptoFailureActionConsume,
		},
		Schema: NewStringSchema(nil),
	})
	assert.Nil(t, err)

	message := "my-message"

	_, err = producer.Send(context.Background(), &ProducerMessage{
		Value: message,
	})
	assert.Nil(t, err)

	// consume encrypted message
	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	assert.NotNil(t, msg)

	// try to decrypt message
	encCtx := msg.GetEncryptionContext()
	assert.NotEmpty(t, encCtx)

	keys := encCtx.Keys
	assert.Equal(t, 1, len(keys))

	encryptionKey, ok := keys[encryptionKeyName]
	assert.True(t, ok)

	encDataKey := encryptionKey.KeyValue
	assert.NotNil(t, encDataKey)

	metadata := encryptionKey.Metadata
	assert.NotNil(t, metadata)

	version := metadata["version"]
	assert.Equal(t, "1.0", version)

	compressionType := encCtx.CompressionType
	uncompressedSize := uint32(encCtx.UncompressedSize)
	encParam := encCtx.Param
	encAlgo := encCtx.Algorithm
	batchSize := encCtx.BatchSize

	// try to decrypt using default MessageCrypto
	msgCrypto, err := crypto.NewDefaultMessageCrypto("testing", false, plog.DefaultNopLogger())
	assert.Nil(t, err)

	producerName := "test"
	sequenceID := uint64(123)
	publishTime := uint64(12333453454)

	messageMetaData := pb.MessageMetadata{
		EncryptionParam:  encParam,
		ProducerName:     &producerName,
		SequenceId:       &sequenceID,
		PublishTime:      &publishTime,
		UncompressedSize: &uncompressedSize,
		EncryptionAlgo:   &encAlgo,
	}

	if compressionType == LZ4 {
		messageMetaData.Compression = pb.CompressionType_LZ4.Enum()
	}

	messageMetaData.EncryptionKeys = []*pb.EncryptionKeys{{
		Key:   &encryptionKeyName,
		Value: encDataKey,
	}}

	decryptedPayload, err := msgCrypto.Decrypt(crypto.NewMessageMetadataSupplier(&messageMetaData),
		msg.Payload(),
		NewEncKeyReader("crypto/testdata/pub_key_rsa.pem",
			"crypto/testdata/pri_key_rsa.pem"))
	assert.Nil(t, err)
	assert.NotNil(t, decryptedPayload)

	// try to uncompress payload
	uncompressedPayload := make([]byte, uncompressedSize)
	s, err := lz4.UncompressBlock(decryptedPayload, uncompressedPayload)
	assert.Nil(t, err)
	assert.Equal(t, uncompressedSize, uint32(s))

	buffer := internal.NewBufferWrapper(uncompressedPayload)

	if batchSize > 0 {
		size := buffer.ReadUint32()
		var meta pb.SingleMessageMetadata
		if err := proto.Unmarshal(buffer.Read(size), &meta); err != nil {
			fmt.Println(err)
		}
		d := buffer.Read(uint32(meta.GetPayloadSize()))
		assert.Equal(t, message, string(d))
	}
}

// TestEncryptDecryptRedeliveryOnFailure test redelivery failed messages
func TestEncryptDecryptRedeliveryOnFailure(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.Nil(t, err)

	topic := newTopicName()
	subcription := "test-subscription-redelivery"

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subcription,
		Decryption: &MessageDecryptionInfo{
			KeyReader: NewEncKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_invalid_rsa.pem"),
		},
	})
	assert.Nil(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: NewEncKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			Keys: []string{"new-enc-key"},
		},
	})
	assert.Nil(t, err)

	producer.Send(context.Background(), &ProducerMessage{
		Payload: []byte("new-test-message"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()

	// message receive should fail due to decryption error
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, msg)
	assert.NotNil(t, err)

	consumer.Close()

	// create consumer with same subscription and proper rsa key pairs
	consumer, err = client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subcription,
		Decryption: &MessageDecryptionInfo{
			KeyReader: NewEncKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
		},
	})
	assert.Nil(t, err)

	// previous message should be redelivered
	msg, err = consumer.Receive(context.Background())
	assert.Nil(t, err)
	assert.NotNil(t, msg)
	consumer.Ack(msg)
}

// TestConsumerSeekByTimeOnPartitionedTopic test seek by time on partitioned topic.
// It is based on existing test case [TestConsumerSeekByTime] but for partitioned topic.
func TestConsumerSeekByTimeOnPartitionedTopic(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	// Create topic with 5 partitions
	topicAdminURL := "admin/v2/persistent/public/default/TestSeekByTimeOnPartitionedTopic/partitions"
	err = httpPut(topicAdminURL, 5)
	defer httpDelete(topicAdminURL)
	assert.Nil(t, err)

	topicName := "persistent://public/default/TestSeekByTimeOnPartitionedTopic"

	partitions, err := client.TopicPartitions(topicName)
	assert.Nil(t, err)
	assert.Equal(t, len(partitions), 5)
	for i := 0; i < 5; i++ {
		assert.Equal(t, partitions[i],
			fmt.Sprintf("%s-partition-%d", topicName, i))
	}

	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "my-sub",
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// Use value bigger than 1000 to full-fill queue channel with size 1000 and message channel with size 10
	const N = 1100
	resetTimeStr := "100s"
	retentionTimeInSecond, err := internal.ParseRelativeTimeInSeconds(resetTimeStr)
	assert.Nil(t, err)

	for i := 0; i < N; i++ {
		_, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)
	}

	// Don't consume all messages so some stay in queues
	for i := 0; i < N-20; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		consumer.Ack(msg)
	}

	currentTimestamp := time.Now()
	err = consumer.SeekByTime(currentTimestamp.Add(-retentionTimeInSecond))
	assert.Nil(t, err)

	// should be able to consume all messages once again
	for i := 0; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		consumer.Ack(msg)
	}
}

func TestAvailablePermitsLeak(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.Nil(t, err)
	client.Close()

	topic := fmt.Sprintf("my-topic-test-ap-leak-%v", time.Now().Nanosecond())

	// 1. Producer with valid key name
	p1, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			Keys: []string{"client-rsa.pem"},
		},
		Schema:          NewStringSchema(nil),
		DisableBatching: true,
	})
	assert.Nil(t, err)
	assert.NotNil(t, p1)

	subscriptionName := "enc-failure-subcription"
	totalMessages := 1000

	// 2. KeyReader is not set by the consumer
	// Receive should fail since KeyReader is not setup
	// because default behaviour of consumer is fail receiving message if error in decryption
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
	})
	assert.Nil(t, err)

	messageFormat := "my-message-%v"
	for i := 0; i < totalMessages; i++ {
		_, err := p1.Send(context.Background(), &ProducerMessage{
			Value: fmt.Sprintf(messageFormat, i),
		})
		assert.Nil(t, err)
	}

	// 2. Set another producer that send message without crypto.
	// The consumer can receive it correct.
	p2, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		Schema:          NewStringSchema(nil),
		DisableBatching: true,
	})
	assert.Nil(t, err)
	assert.NotNil(t, p2)

	_, err = p2.Send(context.Background(), &ProducerMessage{
		Value: fmt.Sprintf(messageFormat, totalMessages),
	})
	assert.Nil(t, err)

	// 3. Discard action on decryption failure. Create a availablePermits leak scenario
	consumer.Close()

	consumer, err = client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
		Decryption: &MessageDecryptionInfo{
			ConsumerCryptoFailureAction: crypto.ConsumerCryptoFailureActionDiscard,
		},
		Schema: NewStringSchema(nil),
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)

	// 4. If availablePermits does not leak, consumer can get the last message which is no crypto.
	// The ctx3 will not exceed deadline.
	ctx3, cancel3 := context.WithTimeout(context.Background(), 15*time.Second)
	_, err = consumer.Receive(ctx3)
	cancel3()
	assert.NotEqual(t, true, errors.Is(err, context.DeadlineExceeded),
		"This means the resource is exhausted. consumer.Receive() will block forever.")
}

func TestConsumerWithBackoffPolicy(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()

	backoff := newTestBackoffPolicy(1*time.Second, 4*time.Second)
	_consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
		Type:             Shared,
		BackoffPolicy:    backoff,
	})
	assert.Nil(t, err)
	defer _consumer.Close()

	partitionConsumerImp := _consumer.(*consumer).consumers[0]
	// 1 s
	startTime := time.Now()
	partitionConsumerImp.reconnectToBroker()
	assert.True(t, backoff.IsExpectedIntervalFrom(startTime))

	// 2 s
	startTime = time.Now()
	partitionConsumerImp.reconnectToBroker()
	assert.True(t, backoff.IsExpectedIntervalFrom(startTime))

	// 4 s
	startTime = time.Now()
	partitionConsumerImp.reconnectToBroker()
	assert.True(t, backoff.IsExpectedIntervalFrom(startTime))

	// 4 s
	startTime = time.Now()
	partitionConsumerImp.reconnectToBroker()
	assert.True(t, backoff.IsExpectedIntervalFrom(startTime))
}

func TestAckWithMessageID(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

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

	// send messages
	if _, err := producer.Send(context.Background(), &ProducerMessage{
		Payload: []byte("hello"),
	}); err != nil {
		log.Fatal(err)
	}

	message, err := consumer.Receive(context.Background())
	assert.Nil(t, err)

	id := message.ID()
	newID := NewMessageID(id.LedgerID(), id.EntryID(), id.BatchIdx(), id.PartitionIdx())
	err = consumer.AckID(newID)
	assert.Nil(t, err)
}
