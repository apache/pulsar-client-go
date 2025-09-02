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
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/apache/pulsar-client-go/pulsar/backoff"

	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	"github.com/apache/pulsar-client-go/pulsar/internal"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/google/uuid"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
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

	assert.ErrorContains(t, err, "connection error")
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

	t.Logf("TestConsumerKeyShared received messages consumer1: %d consumer2: %d\n",
		receivedConsumer1, receivedConsumer2)
	assert.Equal(t, 100, receivedConsumer1+receivedConsumer2)
}

// TestConsumerKeySharedWithDelayedMessages
// test using delayed messages and key-shared sub mode at the same time
func TestConsumerKeySharedWithDelayedMessages(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()
	topic := newTopicName()

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

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	assert.Nil(t, err)
	defer producer.Close()
	ctx := context.Background()
	startTime := time.Now()
	delayTime := 3 * time.Second
	for i := 0; i < 100; i++ {
		_, err := producer.Send(ctx, &ProducerMessage{
			Key:          fmt.Sprintf("key-shared-%d", i%3),
			Payload:      []byte(fmt.Sprintf("value-%d", i)),
			DeliverAfter: delayTime,
		})
		assert.Nil(t, err)
	}

	receivedConsumer1 := 0
	receivedConsumer2 := 0
	timeoutTimer := time.After(2 * delayTime)
	for (receivedConsumer1 + receivedConsumer2) < 100 {
		select {
		case <-timeoutTimer:
			break
		default:
		}

		select {
		case cm, ok := <-consumer1.Chan():
			if !ok {
				break
			}
			receivedConsumer1++
			_ = consumer1.Ack(cm.Message)
			assert.GreaterOrEqual(t, time.Since(startTime), delayTime,
				"TestConsumerKeySharedWithDelayedMessages should delay messages later than defined deliverAfter time",
			)
		case cm, ok := <-consumer2.Chan():
			if !ok {
				break
			}
			receivedConsumer2++
			_ = consumer2.Ack(cm.Message)
			assert.GreaterOrEqual(t, time.Since(startTime), delayTime,
				"TestConsumerKeySharedWithDelayedMessages should delay messages later than defined deliverAfter time",
			)
		}
	}

	assert.NotEqual(t, 0, receivedConsumer1)
	assert.NotEqual(t, 0, receivedConsumer2)
	assert.Equal(t, 100, receivedConsumer1+receivedConsumer2)
	t.Logf("TestConsumerKeySharedWithDelayedMessages received messages consumer1: %d consumer2: %d, timecost: %d\n",
		receivedConsumer1, receivedConsumer2, time.Since(startTime).Milliseconds(),
	)
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

		t.Logf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		consumer.Ack(msg)
	}

	assert.Equal(t, len(msgs), 10)
}

type TestActiveConsumerListener struct {
	t                *testing.T
	lock             sync.RWMutex
	nameToPartitions map[string]map[int32]struct{}
}

func (l *TestActiveConsumerListener) getConsumerCount() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return len(l.nameToPartitions)
}

func (l *TestActiveConsumerListener) getPartitionCount(consumerName string) int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return len(l.nameToPartitions[consumerName])
}

func (l *TestActiveConsumerListener) BecameActive(consumer Consumer, topicName string, partition int32) {
	l.t.Logf("%s become active on %s - %d\n", consumer.Name(), topicName, partition)
	l.lock.Lock()
	defer l.lock.Unlock()
	partitionSet := l.nameToPartitions[consumer.Name()]
	if partitionSet == nil {
		partitionSet = map[int32]struct{}{}
	}
	partitionSet[partition] = struct{}{}
	l.nameToPartitions[consumer.Name()] = partitionSet
}

func (l *TestActiveConsumerListener) BecameInactive(consumer Consumer, topicName string, partition int32) {
	l.t.Logf("%s become inactive on %s - %d\n", consumer.Name(), topicName, partition)
	l.lock.Lock()
	defer l.lock.Unlock()
	partitionSet := l.nameToPartitions[consumer.Name()]
	if _, ok := partitionSet[partition]; ok {
		delete(partitionSet, partition)
		if len(partitionSet) == 0 {
			delete(l.nameToPartitions, consumer.Name())
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

	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/" + randomName + "/partitions"

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
		t:                t,
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

	allConsume(consumers)
	// first consumer will get 3 partitions
	assert.Equal(t, 1, listener.getConsumerCount())
	assert.Equal(t, 3, listener.getPartitionCount(consumers[0].Name()))

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
	assert.Equal(t, 3, listener.getConsumerCount())
	for _, c := range consumers {
		assert.Equal(t, 1, listener.getPartitionCount(c.Name()))
	}

	consumers[0].Close()
	// wait broker reschedule active consumers
	time.Sleep(time.Second * 3)
	allConsume(consumers)

	// close consumer won't get notify
	assert.Equal(t, 3, listener.getConsumerCount())
	// residual consumers will cover all partitions
	assert.Equal(t, 3, listener.getPartitionCount(consumers[1].Name())+listener.getPartitionCount(consumers[2].Name()))
	for _, c := range consumers {
		c.Close()
	}
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

	// Verify encryption keys exist
	keyReader := crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem", "crypto/testdata/pri_key_rsa.pem")
	_, err = keyReader.PublicKey("client-rsa.pem", nil)
	assert.Nil(t, err, "Failed to load public key")
	_, err = keyReader.PrivateKey("client-rsa.pem", nil)
	assert.Nil(t, err, "Failed to load private key")

	// create producer with encryption
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: keyReader,
			Keys:      []string{"client-rsa.pem"},
		},
	})
	assert.Nil(t, err)
	defer producer.Close()

	topics, err := client.TopicPartitions(topic)
	assert.Nil(t, err)
	assert.Equal(t, topic+"-partition-0", topics[0])
	assert.Equal(t, topic+"-partition-1", topics[1])
	assert.Equal(t, topic+"-partition-2", topics[2])

	// create consumer with encryption
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:             topic,
		SubscriptionName:  "my-sub",
		Type:              Exclusive,
		ReceiverQueueSize: 10,
		Decryption: &MessageDecryptionInfo{
			KeyReader:                   keyReader,
			ConsumerCryptoFailureAction: crypto.ConsumerCryptoFailureActionFail,
		},
	})
	assert.Nil(t, err)
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Send messages with encryption
	for i := 0; i < 10; i++ {
		_, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)
	}

	msgs := make([]string, 0)

	// Receive messages with encryption
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		msgs = append(msgs, string(msg.Payload()))

		t.Logf("Received message msgId: %#v -- content: '%s'\n",
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
			t.Logf("consumer1 msg id is: %v, value is: %s\n", cm.Message.ID(), payload)
			consumer1.Ack(cm.Message)
		case cm, ok := <-consumer2.Chan():
			if !ok {
				break
			}
			readMsgs++
			payload := string(cm.Message.Payload())
			messages[payload] = struct{}{}
			t.Logf("consumer2 msg id is: %v, value is: %s\n", cm.Message.ID(), payload)
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

func TestConsumerNoBatchCumulativeAck(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := newTopicName()
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
		// disable batching
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
		Type:             Exclusive,
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

		if i == N/2-1 {
			// cumulative acks the first half of messages
			assert.Nil(t, consumer.AckCumulative(msg))
		}
	}

	consumer.Close()

	// Subscribe again
	consumer, err = client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
		Type:             Exclusive,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// We should only receive the 2nd half of messages
	for i := N / 2; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))

		assert.Nil(t, consumer.Ack(msg))
	}
}

func TestConsumerBatchCumulativeAck(t *testing.T) {
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

	c1, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
		Type:             Exclusive,
	})
	assert.Nil(t, err)

	// c2 is used to test if previous batch can be acked
	// when cumulative ack the next batch message id
	c2, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-2",
		Type:             Exclusive,
	})
	assert.Nil(t, err)

	const N = 100

	// send a batch
	wg := sync.WaitGroup{}
	for i := 0; i < N; i++ {
		wg.Add(1)
		producer.SendAsync(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d", i))},
			func(_ MessageID, _ *ProducerMessage, e error) {
				assert.NoError(t, e)
				wg.Done()
			})
	}
	wg.Wait()

	err = producer.FlushWithCtx(context.Background())
	assert.NoError(t, err)

	// send another batch
	wg = sync.WaitGroup{}
	for i := N; i < 2*N; i++ {
		wg.Add(1)
		producer.SendAsync(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d", i))},
			func(_ MessageID, _ *ProducerMessage, e error) {
				assert.NoError(t, e)
				wg.Done()
			})
	}
	wg.Wait()

	for i := 0; i < 2*N; i++ {
		msg, err := c1.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))

		if i == N-1 {
			// cumulative ack the first half of messages
			err := c1.AckCumulative(msg)
			assert.Nil(t, err)
		} else if i == N {
			// the N+1 msg is in the second batch
			// cumulative ack it to test if the first batch can be acked
			err := c2.AckCumulative(msg)
			assert.Nil(t, err)
		}
	}

	c1.Close()
	c2.Close()

	// Subscribe again
	c1, err = client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
		Type:             Exclusive,
	})
	assert.Nil(t, err)
	defer c1.Close()

	// Subscribe again
	c2, err = client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-2",
		Type:             Exclusive,
	})
	assert.Nil(t, err)
	defer c2.Close()

	// We should only receive the 2nd half of messages
	for i := N; i < 2*N; i++ {
		msg, err := c1.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))

		c1.Ack(msg)
	}

	// We should only receive the 2nd half of messages
	for i := N; i < 2*N; i++ {
		msg, err := c2.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))

		c2.Ack(msg)
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
		NackPrecisionBit:    8,
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
	receivedOdd := 0
	expectedOdd := (N + 1) / 2 // Expected number of odd message IDs

	for receivedOdd < expectedOdd {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)

		// Extract message ID from the payload (e.g., "msg-content-15")
		var id int
		_, err = fmt.Sscanf(string(msg.Payload()), "msg-content-%d", &id)
		assert.Nil(t, err)

		// Count only odd message IDs
		if id%2 == 1 {
			assert.True(t, id%2 == 1) // Optional check, included for clarity
			receivedOdd++
		}

		// Acknowledge message to mark it as processed
		consumer.Ack(msg)
	}

	// Verify that the correct number of odd messages were received
	assert.Equal(t, expectedOdd, receivedOdd)
}

func TestNegativeAckPrecisionBitCnt(t *testing.T) {
	const delay = 1 * time.Second

	for precision := 1; precision <= 8; precision++ {
		topicName := fmt.Sprintf("testNegativeAckPrecisionBitCnt-%d-%d", precision, time.Now().UnixNano())
		ctx := context.Background()
		client, err := NewClient(ClientOptions{URL: lookupURL})
		assert.Nil(t, err)
		defer client.Close()

		consumer, err := client.Subscribe(ConsumerOptions{
			Topic:               topicName,
			SubscriptionName:    "sub-1",
			Type:                Shared,
			NackRedeliveryDelay: delay,
			NackPrecisionBit:    int64(precision),
		})
		assert.Nil(t, err)
		defer consumer.Close()

		producer, err := client.CreateProducer(ProducerOptions{
			Topic: topicName,
		})
		assert.Nil(t, err)
		defer producer.Close()

		// Send single message
		content := "test-0"
		_, err = producer.Send(ctx, &ProducerMessage{
			Payload: []byte(content),
		})
		assert.Nil(t, err)

		// Receive and send negative ack
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, content, string(msg.Payload()))
		consumer.Nack(msg)

		// Calculate expected redelivery window
		expectedRedelivery := time.Now().Add(delay)
		deviation := time.Duration(int64(1)<<precision) * time.Millisecond

		// Wait for redelivery
		redelivered, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, content, string(redelivered.Payload()))

		now := time.Now()
		// Assert that redelivery happens >= expected - deviation
		assert.GreaterOrEqual(t, now.UnixMilli(), expectedRedelivery.UnixMilli()-deviation.Milliseconds())

		consumer.Ack(redelivered)
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

	// enable batching
	p1, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		CompressionType: LZ4,
	})
	assert.Nil(t, err)
	defer p1.Close()

	// disable batching
	p2, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		CompressionType: LZ4,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer p2.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
	})
	assert.Nil(t, err)
	defer consumer.Close()

	const N = 100

	for i := 0; i < N; i++ {
		if _, err := p1.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d-batching-enabled", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < N; i++ {
		if _, err := p2.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d-batching-disabled", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d-batching-enabled", i), string(msg.Payload()))
		consumer.Ack(msg)
	}

	for i := 0; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d-batching-disabled", i), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func TestConsumerMultiCompressions(t *testing.T) {
	type testProvider struct {
		name            string
		compressionType CompressionType
	}

	providers := []testProvider{
		{"zlib", ZLib},
		{"lz4", LZ4},
		{"zstd", ZSTD},
		{"snappy", SNAPPY},
	}

	for _, provider := range providers {
		p := provider
		t.Run(p.name, func(t *testing.T) {
			client, err := NewClient(ClientOptions{
				URL: lookupURL,
			})

			assert.Nil(t, err)
			defer client.Close()

			batchTopic, nonBatchTopic := newTopicName(), newTopicName()
			ctx := context.Background()

			// enable batching
			batchProducer, err := client.CreateProducer(ProducerOptions{
				Topic:           batchTopic,
				CompressionType: p.compressionType,
				DisableBatching: false,
			})
			assert.Nil(t, err)
			defer batchProducer.Close()

			batchConsumer, err := client.Subscribe(ConsumerOptions{
				Topic:            batchTopic,
				SubscriptionName: "sub-1",
			})
			assert.Nil(t, err)
			defer batchConsumer.Close()

			const N = 100
			for i := 0; i < N; i++ {
				batchProducer.SendAsync(ctx, &ProducerMessage{
					Payload: []byte(fmt.Sprintf("msg-content-%d-batching-enabled", i)),
				}, func(_ MessageID, _ *ProducerMessage, err error) {
					assert.Nil(t, err)
				})
			}

			for i := 0; i < N; i++ {
				msg, err := batchConsumer.Receive(ctx)
				assert.Nil(t, err)
				assert.Equal(t, fmt.Sprintf("msg-content-%d-batching-enabled", i), string(msg.Payload()))
				batchConsumer.Ack(msg)
			}

			// disable batching
			nonBatchProducer, err := client.CreateProducer(ProducerOptions{
				Topic:           nonBatchTopic,
				CompressionType: p.compressionType,
				DisableBatching: true,
			})
			assert.Nil(t, err)
			defer nonBatchProducer.Close()

			nonBatchConsumer, err := client.Subscribe(ConsumerOptions{
				Topic:            nonBatchTopic,
				SubscriptionName: "sub-1",
			})
			assert.Nil(t, err)
			defer nonBatchConsumer.Close()

			for i := 0; i < N; i++ {
				if _, err := nonBatchProducer.Send(ctx, &ProducerMessage{
					Payload: []byte(fmt.Sprintf("msg-content-%d-batching-disabled", i)),
				}); err != nil {
					t.Fatal(err)
				}
			}

			for i := 0; i < N; i++ {
				msg, err := nonBatchConsumer.Receive(ctx)
				assert.Nil(t, err)
				assert.Equal(t, fmt.Sprintf("msg-content-%d-batching-disabled", i), string(msg.Payload()))
				nonBatchConsumer.Ack(msg)
			}
		})
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

	producer.FlushWithCtx(context.Background())

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
		Topic:                   topicName,
		SubscriptionName:        "sub-1",
		StartMessageIDInclusive: true,
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
	sub, consumerName := "my-sub", "my-consumer"

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:               topic,
		SubscriptionName:    sub,
		NackRedeliveryDelay: 1 * time.Second,
		Type:                Shared,
		DLQ:                 &dlqPolicy,
		Name:                consumerName,
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
	eventTimeList := make([]time.Time, 10)
	msgIDList := make([]string, 10)
	msgKeyList := make([]string, 10)
	for i := 0; i < 10; i++ {
		eventTime := time.Now()
		eventTimeList[i] = eventTime
		msgKeyList[i] = fmt.Sprintf("key-%d", i)
		msgID, err := producer.Send(ctx, &ProducerMessage{
			Payload:     []byte(fmt.Sprintf("hello-%d", i)),
			Key:         fmt.Sprintf("key-%d", i),
			OrderingKey: fmt.Sprintf("key-%d", i),
			EventTime:   eventTime,
			Properties: map[string]string{
				"key": fmt.Sprintf("key-%d", i),
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		msgIDList[i] = msgID.String()
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

		// check dlq produceName
		regex := regexp.MustCompile(fmt.Sprintf("%s-%s-%s-[a-z]{5}-DLQ", topic, sub, consumerName))
		assert.True(t, regex.MatchString(msg.ProducerName()))

		// check original messageId
		assert.NotEmpty(t, msg.Properties()[SysPropertyOriginMessageID])
		assert.Equal(t, msgIDList[expectedMsgIdx], msg.Properties()[SysPropertyOriginMessageID])
		assert.NotEmpty(t, msg.Properties()[PropertyOriginMessageID])
		assert.Equal(t, msgIDList[expectedMsgIdx], msg.Properties()[PropertyOriginMessageID])

		// check original topic
		assert.Contains(t, msg.Properties()[SysPropertyRealTopic], topic)

		// check original key
		assert.NotEmpty(t, msg.Key())
		assert.Equal(t, msgKeyList[expectedMsgIdx], msg.Key())
		assert.NotEmpty(t, msg.OrderingKey())
		assert.Equal(t, msgKeyList[expectedMsgIdx], msg.OrderingKey())
		assert.NotEmpty(t, msg.Properties()["key"])
		assert.Equal(t, msg.Key(), msg.Properties()["key"])

		//	check original event time
		//	Broker will ignore event time microsecond(us) level precision,
		//	so that we need to check eventTime precision in millisecond level
		assert.NotEqual(t, 0, msg.EventTime())
		assert.True(t, eventTimeList[expectedMsgIdx].Sub(msg.EventTime()).Abs() < 2*time.Millisecond)
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
func TestDeadLetterTopicWithInitialSubscription(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/" + newTopicName()
	dlqSub, sub, consumerName := "init-sub", "my-sub", "my-consumer"
	dlqTopic := fmt.Sprintf("%s-%s-DLQ", topic, sub)
	ctx := context.Background()

	// create consumer
	maxRedeliveryCount, sendMessages := 1, 100

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:               topic,
		SubscriptionName:    sub,
		NackRedeliveryDelay: 1 * time.Second,
		Type:                Shared,
		DLQ: &DLQPolicy{
			MaxDeliveries:           uint32(maxRedeliveryCount),
			DeadLetterTopic:         dlqTopic,
			InitialSubscriptionName: dlqSub,
		},
		Name:              consumerName,
		ReceiverQueueSize: sendMessages,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send messages
	for i := 0; i < sendMessages; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		}
	}

	// nack all messages
	for i := 0; i < sendMessages*(maxRedeliveryCount+1); i++ {
		ctx, canc := context.WithTimeout(context.Background(), 3*time.Second)
		defer canc()
		msg, _ := consumer.Receive(ctx)
		if msg == nil {
			break
		}
		consumer.Nack(msg)
	}

	// create dlq consumer
	dlqConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:            dlqTopic,
		SubscriptionName: dlqSub,
	})
	assert.Nil(t, err)
	defer dlqConsumer.Close()

	for i := 0; i < sendMessages; i++ {
		ctx, canc := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer canc()
		msg, err := dlqConsumer.Receive(ctx)
		assert.Nil(t, err)
		assert.NotNil(t, msg)
		err = dlqConsumer.Ack(msg)
		assert.Nil(t, err)
	}

	// No more messages on the DLQ
	ctx, canc := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer canc()
	msg, err := dlqConsumer.Receive(ctx)
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
	consumerName := "my-consumer"
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

	eventTimeList := make([]time.Time, N)
	msgIDList := make([]string, N)
	msgKeyList := make([]string, N)
	for i := 0; i < N; i++ {
		eventTime := time.Now()
		eventTimeList[i] = eventTime
		msgKeyList[i] = fmt.Sprintf("key-%d", i)
		msgID, err := producer.Send(ctx, &ProducerMessage{
			Payload:     []byte(fmt.Sprintf("MESSAGE_%d", i)),
			Key:         fmt.Sprintf("key-%d", i),
			OrderingKey: fmt.Sprintf("key-%d", i),
			EventTime:   eventTime,
			Properties: map[string]string{
				"key": fmt.Sprintf("key-%d", i),
			},
		})
		assert.Nil(t, err)
		msgIDList[i] = msgID.String()
	}

	// 2. Create consumer on the Retry Topic to reconsume N messages (maxRedeliveries+1) times
	rlqConsumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Name:                        consumerName,
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
		Topic:                       "persistent://public/default/" + topic + "-" + subName + "-DLQ",
		SubscriptionName:            subName,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer dlqConsumer.Close()

	dlqReceived := 0
	for dlqReceived < N {
		msg, err := dlqConsumer.Receive(ctx)
		//	check original messageId
		//	we create a topic with three partitions,
		//	so that messages maybe not be received as the same order as we produced
		assert.NotEmpty(t, msg.Properties()[SysPropertyOriginMessageID])
		assert.Contains(t, msgIDList, msg.Properties()[SysPropertyOriginMessageID])
		assert.NotEmpty(t, msg.Properties()[PropertyOriginMessageID])
		assert.Contains(t, msgIDList, msg.Properties()[PropertyOriginMessageID])

		// check original topic
		assert.Contains(t, msg.Properties()[SysPropertyRealTopic], topic)

		// check original key
		assert.NotEmpty(t, msg.Key())
		assert.Contains(t, msgKeyList, msg.Key())
		assert.NotEmpty(t, msg.OrderingKey())
		assert.Contains(t, msgKeyList, msg.OrderingKey())
		assert.NotEmpty(t, msg.Properties()["key"])
		assert.Equal(t, msg.Key(), msg.Properties()["key"])

		// check original event time
		assert.NotEqual(t, 0, msg.EventTime())
		//	check original event time
		//	Broker will ignore event time microsecond(us) level precision,
		//	so that we need to check eventTime precision in millisecond level
		assert.LessOrEqual(t, eventTimeList[0].Add(-2*time.Millisecond), msg.EventTime())
		assert.LessOrEqual(t, msg.EventTime(), eventTimeList[N-1].Add(2*time.Millisecond))

		// check dlq produceName
		regex := regexp.MustCompile(fmt.Sprintf("%s-%s-%s-[a-z]{5}-DLQ", topic, subName, consumerName))
		assert.True(t, regex.MatchString(msg.ProducerName()))

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

func TestRLQWithCustomProperties(t *testing.T) {
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

		if msg.RedeliveryCount() > 0 {
			msgProps := msg.Properties()

			value, ok := msgProps["custom-key-1"]
			assert.True(t, ok)
			if ok {
				assert.Equal(t, value, "custom-value-1")
			}

			rlqConsumer.ReconsumeLater(msg, 1*time.Second)
		} else {
			customProps := map[string]string{
				"custom-key-1": "custom-val-1",
			}
			rlqConsumer.ReconsumeLaterWithCustomProperties(msg, customProps, 1*time.Second)
		}

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
		Topic:                       "persistent://public/default/" + topic + "-" + subName + "-DLQ",
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

// Test function to test Retry Logic with Custom Properties and Event Time
func TestRLQWithCustomPropertiesEventTime(t *testing.T) {
	topic := newTopicName()
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/" + topic + "/partitions"
	makeHTTPCall(t, http.MethodPut, testURL, "3")

	subName := fmt.Sprintf("sub01-%d", time.Now().Unix())
	maxRedeliveries := 2
	ctx := context.Background()

	client, err := NewClient(ClientOptions{URL: lookupURL})
	assert.Nil(t, err)
	defer client.Close()

	// 1. Create producer and send a message with custom event time
	producer, err := client.CreateProducer(ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	defer producer.Close()

	expectedEventTime := timeFromUnixTimestampMillis(uint64(1565161612000)) // Custom event time
	_, err = producer.Send(ctx, &ProducerMessage{
		Payload:   []byte("MESSAGE_WITH_EVENT_TIME"),
		EventTime: expectedEventTime,
	})
	assert.Nil(t, err)

	// 2. Create consumer on the Retry Topic
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

	// 3. Receive the original message and verify event time
	msg, err := rlqConsumer.Receive(ctx)
	assert.Nil(t, err)
	assert.Equal(t, expectedEventTime.Unix(), msg.EventTime().Unix(),
		"Original message should have the expected event time")

	// 4. ReconsumeLater with custom properties and verify event time is preserved
	customProps := map[string]string{
		"custom-key-1": "custom-value-1",
	}
	rlqConsumer.ReconsumeLaterWithCustomProperties(msg, customProps, 1*time.Second)

	// 5. Receive the reconsumed message and verify event time is preserved
	retryMsg, err := rlqConsumer.Receive(ctx)
	assert.Nil(t, err)
	assert.Equal(t, expectedEventTime.Unix(), retryMsg.EventTime().Unix(),
		"Reconsumed message should preserve the original event time")

	// 6. Verify custom properties are also preserved
	msgProps := retryMsg.Properties()
	value, ok := msgProps["custom-key-1"]
	assert.True(t, ok, "Custom property should be present")
	assert.Equal(t, "custom-value-1", value, "Custom property value should match")

	// 7. Clean up - ack the message to avoid further redeliveries
	rlqConsumer.Ack(retryMsg)
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

func TestCumulativeAckWithResponse(t *testing.T) {
	now := time.Now().Unix()
	topic01 := fmt.Sprintf("persistent://public/default/topic-%d-01", now)
	ctx := context.Background()

	client, err := NewClient(ClientOptions{URL: lookupURL})
	assert.Nil(t, err)
	defer client.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic01,
		SubscriptionName:            "my-sub",
		Type:                        Exclusive,
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

	var msg Message
	for i := 0; i < 10; i++ {
		msg, err = consumer.Receive(ctx)
		assert.Nil(t, err)
	}

	err = consumer.AckCumulative(msg)
	assert.Nil(t, err)
}

func TestRLQMultiTopics(t *testing.T) {
	now := time.Now().Unix()
	topic01 := fmt.Sprintf("persistent://public/default/topic-%d-1", now)
	topic02 := fmt.Sprintf("topic-%d-2", now)
	topics := []string{topic01, topic02}

	subName := fmt.Sprintf("sub01-%d", time.Now().Unix())
	consumerName := "my-consumer"
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
		Name:                        consumerName,
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
		Topic:                       topics[0] + "-" + subName + "-DLQ",
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
	// check dlq produceName
	regex := regexp.MustCompile(fmt.Sprintf("%s-%s-%s-[a-z]{5}-DLQ", "", subName, consumerName))
	for dlqReceived < 2*N {
		msg, err := dlqConsumer.Receive(ctx)
		assert.Nil(t, err)
		assert.True(t, regex.MatchString(msg.ProducerName()))
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
		Topic:                       normalTopic + "-" + subName + "-DLQ",
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
		MessageRouter: func(msg *ProducerMessage, _ TopicMetadata) int {
			// The message key will contain the partition id where to route
			i, err := strconv.Atoi(msg.Key)
			assert.NoError(t, err)
			return i
		},
		PartitionsAutoDiscoveryInterval: 100 * time.Millisecond,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// Increase number of partitions to 10
	makeHTTPCall(t, http.MethodPost, testURL, "10")

	// Wait for the producer/consumers to pick up the change
	time.Sleep(1 * time.Second)

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:               topic,
		SubscriptionName:    "my-sub",
		AutoDiscoveryPeriod: 100 * time.Millisecond,
	})
	assert.Nil(t, err)
	defer consumer.Close()

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

		t.Logf("Received message msgId: %#v -- content: '%s'\n",
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

func (noopConsumerInterceptor) BeforeConsume(_ ConsumerMessage) {}

func (noopConsumerInterceptor) OnAcknowledge(_ Consumer, _ MessageID) {}

func (noopConsumerInterceptor) OnNegativeAcksSend(_ Consumer, _ []MessageID) {}

// copyPropertyInterceptor copy all keys in message properties map and add a suffix
type copyPropertyInterceptor struct {
	suffix string
}

func (x copyPropertyInterceptor) BeforeConsume(message ConsumerMessage) {
	properties := message.Properties()
	cp := make(map[string]string, len(properties))
	for k, v := range properties {
		cp[k+x.suffix] = v
	}
	for ck, v := range cp {
		properties[ck] = v
	}
}

func (copyPropertyInterceptor) OnAcknowledge(_ Consumer, _ MessageID) {}

func (copyPropertyInterceptor) OnNegativeAcksSend(_ Consumer, _ []MessageID) {}

type metricConsumerInterceptor struct {
	ackn  int32
	nackn int32
}

func (x *metricConsumerInterceptor) BeforeConsume(_ ConsumerMessage) {}

func (x *metricConsumerInterceptor) OnAcknowledge(_ Consumer, _ MessageID) {
	atomic.AddInt32(&x.ackn, 1)
}

func (x *metricConsumerInterceptor) OnNegativeAcksSend(_ Consumer, msgIDs []MessageID) {
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

	var nackIDs []MessageID
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
			nackIDs = append(nackIDs, msg.ID())
		}
	}
	assert.Equal(t, int32(5), atomic.LoadInt32(&metric.ackn))

	for i := range nackIDs {
		consumer.NackID(nackIDs[i])
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
			}, func(_ MessageID, _ *ProducerMessage, err error) {
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

	t.Logf("TestKeyBasedBatchProducerConsumerKeyShared received messages consumer1: %d consumer2: %d\n",
		receivedConsumer1, receivedConsumer2)
	assert.Equal(t, 300, receivedConsumer1+receivedConsumer2)

	t.Logf("TestKeyBasedBatchProducerConsumerKeyShared received messages keys consumer1: %v consumer2: %v\n",
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
			}, func(_ MessageID, _ *ProducerMessage, err error) {
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
			}, func(_ MessageID, _ *ProducerMessage, err error) {
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

	t.Logf(
		"TestConsumerKeySharedWithOrderingKey received messages consumer1: %d consumer2: %d\n",
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
		t.Logf("Sent : %v\n", mid)
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
		t.Logf("Received : %v\n", m.ID())
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
		t.Logf("received : %v\n", string(msg.Payload()))
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
func (d *EncKeyReader) PublicKey(keyName string, _ map[string]string) (*crypto.EncryptionKeyInfo, error) {
	return readKey(keyName, d.publicKeyPath, d.metaMap)
}

// GetPrivateKey read private key from the given path
func (d *EncKeyReader) PrivateKey(keyName string, _ map[string]string) (*crypto.EncryptionKeyInfo, error) {
	return readKey(keyName, d.privateKeyPath, d.metaMap)
}

func readKey(keyName, path string, keyMeta map[string]string) (*crypto.EncryptionKeyInfo, error) {
	key, err := os.ReadFile(path)
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

	bo := newTestBackoffPolicy(1*time.Second, 4*time.Second)
	_consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
		Type:             Shared,
		BackOffPolicyFunc: func() backoff.Policy {
			return bo
		},
	})
	assert.Nil(t, err)
	defer _consumer.Close()

	partitionConsumerImp := _consumer.(*consumer).consumers[0]
	// 1 s
	startTime := time.Now()
	partitionConsumerImp.reconnectToBroker(nil)
	assert.True(t, bo.IsExpectedIntervalFrom(startTime))

	// 2 s
	startTime = time.Now()
	partitionConsumerImp.reconnectToBroker(nil)
	assert.True(t, bo.IsExpectedIntervalFrom(startTime))

	// 4 s
	startTime = time.Now()
	partitionConsumerImp.reconnectToBroker(nil)
	assert.True(t, bo.IsExpectedIntervalFrom(startTime))

	// 4 s
	startTime = time.Now()
	partitionConsumerImp.reconnectToBroker(nil)
	assert.True(t, bo.IsExpectedIntervalFrom(startTime))
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

func TestBatchIndexAck(t *testing.T) {
	type config struct {
		ackWithResponse    bool
		cumulative         bool
		ackGroupingOptions *AckGroupingOptions
	}
	configs := make([]config, 0)
	for _, option := range []*AckGroupingOptions{
		nil, // MaxSize: 1000, MaxTime: 10ms
		{MaxSize: 0, MaxTime: 0},
		{MaxSize: 1000, MaxTime: 0},
	} {
		configs = append(configs, config{true, true, option})
		configs = append(configs, config{true, false, option})
		configs = append(configs, config{false, true, option})
		configs = append(configs, config{false, false, option})
	}

	for _, params := range configs {
		option := params.ackGroupingOptions
		if option == nil {
			option = &AckGroupingOptions{1000, 10 * time.Millisecond}
		}

		t.Run(fmt.Sprintf("TestBatchIndexAck_WithResponse_%v_Cumulative_%v_AckGroupingOption_%v_%v",
			params.ackWithResponse, params.cumulative, option.MaxSize, option.MaxTime.Milliseconds()),
			func(t *testing.T) {
				runBatchIndexAckTest(t, params.ackWithResponse, params.cumulative, params.ackGroupingOptions)
			})
	}
}

func runBatchIndexAckTest(t *testing.T, ackWithResponse bool, cumulative bool, option *AckGroupingOptions) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)

	topic := newTopicName()
	createConsumer := func() Consumer {
		consumer, err := client.Subscribe(ConsumerOptions{
			Topic:                          topic,
			SubscriptionName:               "my-sub",
			AckWithResponse:                ackWithResponse,
			EnableBatchIndexAcknowledgment: true,
			AckGroupingOptions:             option,
		})
		assert.Nil(t, err)
		return consumer
	}

	consumer := createConsumer()

	duration, err := time.ParseDuration("1h")
	assert.Nil(t, err)

	const BatchingMaxSize int = 2 * 5
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   topic,
		DisableBatching:         false,
		BatchingMaxMessages:     uint(BatchingMaxSize),
		BatchingMaxSize:         uint(1024 * 1024 * 10),
		BatchingMaxPublishDelay: duration,
	})
	assert.Nil(t, err)
	for i := 0; i < BatchingMaxSize; i++ {
		producer.SendAsync(context.Background(), &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		}, func(id MessageID, _ *ProducerMessage, err error) {
			assert.Nil(t, err)
			log.Printf("Sent to %v:%d:%d", id, id.BatchIdx(), id.BatchSize())
		})
	}
	assert.Nil(t, producer.FlushWithCtx(context.Background()))

	msgIDs := make([]MessageID, BatchingMaxSize)
	for i := 0; i < BatchingMaxSize; i++ {
		message, err := consumer.Receive(context.Background())
		assert.Nil(t, err)
		msgIDs[i] = message.ID()
		log.Printf("Received %v from %v:%d:%d", string(message.Payload()), message.ID(),
			message.ID().BatchIdx(), message.ID().BatchSize())
	}

	// Acknowledge half of the messages
	if cumulative {
		msgID := msgIDs[BatchingMaxSize/2-1]
		err := consumer.AckIDCumulative(msgID)
		assert.Nil(t, err)
		log.Printf("Acknowledge %v:%d cumulatively\n", msgID, msgID.BatchIdx())
	} else {
		for i := 0; i < BatchingMaxSize; i++ {
			msgID := msgIDs[i]
			if i%2 == 0 {
				consumer.AckID(msgID)
				log.Printf("Acknowledge %v:%d\n", msgID, msgID.BatchIdx())
			}
		}
	}
	consumer.Close()
	consumer = createConsumer()

	for i := 0; i < BatchingMaxSize/2; i++ {
		message, err := consumer.Receive(context.Background())
		assert.Nil(t, err)
		log.Printf("Received %v from %v:%d:%d", string(message.Payload()), message.ID(),
			message.ID().BatchIdx(), message.ID().BatchSize())
		index := i*2 + 1
		if cumulative {
			index = i + BatchingMaxSize/2
		}
		assert.Equal(t, []byte(fmt.Sprintf("msg-%d", index)), message.Payload())
		assert.Equal(t, msgIDs[index].BatchIdx(), message.ID().BatchIdx())
		// We should not acknowledge message.ID() here because message.ID() shares a different
		// tracker with msgIds
		if !cumulative {
			msgID := msgIDs[index]
			consumer.AckID(msgID)
			log.Printf("Acknowledge %v:%d\n", msgID, msgID.BatchIdx())
		}
	}
	if cumulative {
		msgID := msgIDs[BatchingMaxSize-1]
		err := consumer.AckIDCumulative(msgID)
		assert.Nil(t, err)
		log.Printf("Acknowledge %v:%d cumulatively\n", msgID, msgID.BatchIdx())
	}
	consumer.Close()
	consumer = createConsumer()
	_, err = producer.Send(context.Background(), &ProducerMessage{Payload: []byte("end-marker")})
	assert.Nil(t, err)
	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, "end-marker", string(msg.Payload()))

	client.Close()
}

func TestConsumerWithAutoScaledQueueReceive(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

	// create consumer
	c, err := client.Subscribe(ConsumerOptions{
		Topic:                             topic,
		SubscriptionName:                  "my-sub",
		Type:                              Exclusive,
		ReceiverQueueSize:                 3,
		EnableAutoScaledReceiverQueueSize: true,
	})
	assert.Nil(t, err)
	pc := c.(*consumer).consumers[0]
	assert.Equal(t, int32(1), pc.currentQueueSize.Load())
	defer c.Close()

	// create p
	p, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer p.Close()

	// send message, it will update scaleReceiverQueueHint from false to true
	_, err = p.Send(context.Background(), &ProducerMessage{
		Payload: []byte("hello"),
	})
	assert.NoError(t, err)

	// this will trigger receiver queue size expanding to 2 because we have prefetched 1 message >= currentSize 1.
	_, err = c.Receive(context.Background())
	assert.Nil(t, err)

	// currentQueueSize should be doubled in size
	retryAssert(t, 5, 200, func() {}, func(t assert.TestingT) bool {
		return assert.Equal(t, 2, int(pc.currentQueueSize.Load()))
	})

	for i := 0; i < 5; i++ {
		_, err = p.Send(context.Background(), &ProducerMessage{
			Payload: []byte("hello"),
		})
		assert.NoError(t, err)

		// waiting for prefetched message passing from queueCh to messageCh
		retryAssert(t, 5, 200, func() {}, func(t assert.TestingT) bool {
			return assert.Equal(t, 1, len(pc.messageCh))
		})

		_, err = p.Send(context.Background(), &ProducerMessage{
			Payload: []byte("hello"),
		})
		assert.NoError(t, err)

		// wait all the messages has been prefetched
		_, err = c.Receive(context.Background())
		assert.Nil(t, err)
		_, err = c.Receive(context.Background())
		assert.Nil(t, err)
		// this will not trigger receiver queue size expanding because we have prefetched 2 message < currentSize 4.
		assert.Equal(t, int32(2), pc.currentQueueSize.Load())
	}

	for i := 0; i < 5; i++ {
		p.SendAsync(
			context.Background(),
			&ProducerMessage{Payload: []byte("hello")},
			func(_ MessageID, _ *ProducerMessage, _ error) {
			},
		)
	}

	retryAssert(t, 3, 300, func() {}, func(t assert.TestingT) bool {
		return assert.Equal(t, 3, int(pc.currentQueueSize.Load()))
	})
}

func TestConsumerNonDurable(t *testing.T) {
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
		SubscriptionMode: NonDurable,
	})
	assert.Nil(t, err)

	i := 1
	if _, err := producer.Send(ctx, &ProducerMessage{
		Payload: []byte(fmt.Sprintf("msg-content-%d", i)),
	}); err != nil {
		t.Fatal(err)
	}

	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))
	consumer.Ack(msg)

	consumer.Close()

	i++

	// send a message. Pulsar should delete it as there is no active subscription
	if _, err := producer.Send(ctx, &ProducerMessage{
		Payload: []byte(fmt.Sprintf("msg-content-%d", i)),
	}); err != nil {
		t.Fatal(err)
	}

	i++

	// Subscribe again
	consumer, err = client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
		Type:             Shared,
		SubscriptionMode: NonDurable,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	if _, err := producer.Send(ctx, &ProducerMessage{
		Payload: []byte(fmt.Sprintf("msg-content-%d", i)),
	}); err != nil {
		t.Fatal(err)
	}

	msg, err = consumer.Receive(ctx)
	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))
	consumer.Ack(msg)
}

func TestConsumerBatchIndexAckDisabled(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
	})
	assert.Nil(t, err)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	assert.Nil(t, err)

	for i := 0; i < 5; i++ {
		producer.SendAsync(context.Background(), &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		}, nil)
	}
	for i := 0; i < 5; i++ {
		message, err := consumer.Receive(context.Background())
		assert.Nil(t, err)
		consumer.Ack(message)
	}
	consumer.Close()
	consumer, err = client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
	})
	assert.Nil(t, err)
	producer.Send(context.Background(), &ProducerMessage{Payload: []byte("done")})
	message, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, []byte("done"), message.Payload())
}

func TestConsumerMemoryLimit(t *testing.T) {
	// Create client 1 without memory limit
	cli1, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer cli1.Close()

	// Create client 1 with memory limit
	cli2, err := NewClient(ClientOptions{
		URL:              lookupURL,
		MemoryLimitBytes: 10 * 1024,
	})

	assert.Nil(t, err)
	defer cli2.Close()

	topic := newTopicName()

	// Use client 1 to create producer p1
	p1, err := cli1.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer p1.Close()

	// Use mem-limited client 2 to create consumer c1
	c1, err := cli2.Subscribe(ConsumerOptions{
		Topic:                             topic,
		SubscriptionName:                  "my-sub-1",
		Type:                              Exclusive,
		EnableAutoScaledReceiverQueueSize: true,
	})
	assert.Nil(t, err)
	defer c1.Close()
	pc1 := c1.(*consumer).consumers[0]

	// Fill up the messageCh of c1
	for i := 0; i < 10; i++ {
		p1.SendAsync(
			context.Background(),
			&ProducerMessage{Payload: createTestMessagePayload(1)},
			func(_ MessageID, _ *ProducerMessage, _ error) {
			},
		)
	}

	retryAssert(t, 5, 200, func() {}, func(t assert.TestingT) bool {
		return assert.Equal(t, 10, len(pc1.messageCh))
	})

	// Get current receiver queue size of c1
	prevQueueSize := pc1.currentQueueSize.Load()

	// Make the client 1 exceed the memory limit
	_, err = p1.Send(context.Background(), &ProducerMessage{
		Payload: createTestMessagePayload(10*1024 + 1),
	})
	assert.NoError(t, err)

	// c1 should shrink it's receiver queue size
	retryAssert(t, 5, 200, func() {}, func(t assert.TestingT) bool {
		return assert.Equal(t, prevQueueSize/2, pc1.currentQueueSize.Load())
	})

	// Use mem-limited client 2 to create consumer c2
	c2, err := cli2.Subscribe(ConsumerOptions{
		Topic:                             topic,
		SubscriptionName:                  "my-sub-2",
		Type:                              Exclusive,
		SubscriptionInitialPosition:       SubscriptionPositionEarliest,
		EnableAutoScaledReceiverQueueSize: true,
	})
	assert.Nil(t, err)
	defer c2.Close()
	pc2 := c2.(*consumer).consumers[0]

	// Try to induce c2 receiver queue size expansion
	for i := 0; i < 10; i++ {
		p1.SendAsync(
			context.Background(),
			&ProducerMessage{Payload: createTestMessagePayload(1)},
			func(_ MessageID, _ *ProducerMessage, _ error) {
			},
		)
	}

	retryAssert(t, 5, 200, func() {}, func(t assert.TestingT) bool {
		return assert.Equal(t, 10, len(pc1.messageCh))
	})

	// c2 receiver queue size should not expansion because client 1 has exceeded the memory limit
	assert.Equal(t, 1, int(pc2.currentQueueSize.Load()))

	// Use mem-limited client 2 to create producer p2
	p2, err := cli2.CreateProducer(ProducerOptions{
		Topic:                   topic,
		DisableBatching:         false,
		DisableBlockIfQueueFull: true,
	})
	assert.Nil(t, err)
	defer p2.Close()

	_, err = p2.Send(context.Background(), &ProducerMessage{
		Payload: createTestMessagePayload(1),
	})
	// Producer can't send message
	assert.Equal(t, true, errors.Is(err, ErrMemoryBufferIsFull))
}

func TestMultiConsumerMemoryLimit(t *testing.T) {
	// Create client 1 without memory limit
	cli1, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer cli1.Close()

	// Create client 1 with memory limit
	cli2, err := NewClient(ClientOptions{
		URL:              lookupURL,
		MemoryLimitBytes: 10 * 1024,
	})

	assert.Nil(t, err)
	defer cli2.Close()

	topic := newTopicName()

	// Use client 1 to create producer p1
	p1, err := cli1.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer p1.Close()

	// Use mem-limited client 2 to create consumer c1
	c1, err := cli2.Subscribe(ConsumerOptions{
		Topic:                             topic,
		SubscriptionName:                  "my-sub-1",
		Type:                              Exclusive,
		EnableAutoScaledReceiverQueueSize: true,
	})
	assert.Nil(t, err)
	defer c1.Close()
	pc1 := c1.(*consumer).consumers[0]

	// Use mem-limited client 2 to create consumer c1
	c2, err := cli2.Subscribe(ConsumerOptions{
		Topic:                             topic,
		SubscriptionName:                  "my-sub-2",
		Type:                              Exclusive,
		EnableAutoScaledReceiverQueueSize: true,
	})
	assert.Nil(t, err)
	defer c2.Close()
	pc2 := c2.(*consumer).consumers[0]

	// Fill up the messageCh of c1 nad c2
	for i := 0; i < 10; i++ {
		p1.SendAsync(
			context.Background(),
			&ProducerMessage{Payload: createTestMessagePayload(1)},
			func(_ MessageID, _ *ProducerMessage, _ error) {
			},
		)
	}

	retryAssert(t, 5, 200, func() {}, func(t assert.TestingT) bool {
		return assert.Equal(t, 10, len(pc1.messageCh))
	})

	retryAssert(t, 5, 200, func() {}, func(t assert.TestingT) bool {
		return assert.Equal(t, 10, len(pc2.messageCh))
	})

	// Get current receiver queue size of c1 and c2
	pc1PrevQueueSize := pc1.currentQueueSize.Load()
	pc2PrevQueueSize := pc2.currentQueueSize.Load()

	// Make the client 1 exceed the memory limit
	_, err = p1.Send(context.Background(), &ProducerMessage{
		Payload: createTestMessagePayload(10*1024 + 1),
	})
	assert.NoError(t, err)

	// c1 should shrink it's receiver queue size
	retryAssert(t, 5, 200, func() {}, func(t assert.TestingT) bool {
		return assert.Equal(t, pc1PrevQueueSize/2, pc1.currentQueueSize.Load())
	})

	// c2 should shrink it's receiver queue size too
	retryAssert(t, 5, 200, func() {}, func(t assert.TestingT) bool {
		return assert.Equal(t, pc2PrevQueueSize/2, pc2.currentQueueSize.Load())
	})
}

func TestConsumerAckCumulativeOnSharedSubShouldFailed(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
		Type:             Shared,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	assert.Nil(t, err)
	defer producer.Close()

	_, err = producer.Send(context.Background(), &ProducerMessage{
		Payload: []byte("hello"),
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)

	err = consumer.AckIDCumulative(msg.ID())
	assert.NotNil(t, err)
	assert.ErrorIs(t, err, ErrInvalidAck)
}

func TestConsumerUnSubscribe(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := "my-topic"
	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
		Type:             Exclusive,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	err = consumer.Unsubscribe()
	assert.Nil(t, err)

	err = consumer.Unsubscribe()
	assert.Error(t, err)

}
func TestConsumerForceUnSubscribe(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := "my-topic"
	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
		Type:             Exclusive,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	err = consumer.UnsubscribeForce()
	assert.Nil(t, err)

	err = consumer.UnsubscribeForce()
	assert.Error(t, err)

}

func TestConsumerGetLastMessageIDs(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	partition := 1
	topic := "my-topic"
	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
		Type:             Shared,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()
	// send messages
	totalMessage := 10
	for i := 0; i < totalMessage; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			assert.Nil(t, err)
		}
	}

	// create admin
	admin, err := pulsaradmin.NewClient(&config.Config{})
	assert.Nil(t, err)

	messageIDs, err := consumer.GetLastMessageIDs()
	assert.Nil(t, err)
	assert.Equal(t, partition, len(messageIDs))

	id := messageIDs[0]
	topicName, err := utils.GetTopicName(id.Topic())
	assert.Nil(t, err)
	messages, err := admin.Subscriptions().GetMessagesByID(*topicName, id.LedgerID(), id.EntryID())
	assert.Nil(t, err)
	assert.Equal(t, 1, len(messages))

}

func TestPartitionConsumerGetLastMessageIDs(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	partition := 3
	err = createPartitionedTopic(topic, partition)
	assert.Nil(t, err)

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
		Type:             Shared,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	ctx := context.Background()
	totalMessage := 30
	// send messages
	for i := 0; i < totalMessage; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			assert.Nil(t, err)
		}
	}

	// create admin
	admin, err := pulsaradmin.NewClient(&config.Config{})
	assert.Nil(t, err)

	topicMessageIDs, err := consumer.GetLastMessageIDs()
	assert.Nil(t, err)
	assert.Equal(t, partition, len(topicMessageIDs))
	for _, id := range topicMessageIDs {
		assert.Equal(t, int(id.EntryID()), totalMessage/partition-1)

		topicName, err := utils.GetTopicName(id.Topic())
		assert.Nil(t, err)
		messages, err := admin.Subscriptions().GetMessagesByID(*topicName, id.LedgerID(), id.EntryID())
		assert.Nil(t, err)
		assert.Equal(t, 1, len(messages))

	}

}

func TestLookupConsumer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
		LookupProperties: map[string]string{
			"broker.id": "1",
		},
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
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
		assert.Equal(t, []byte(expectMsg), msg.Payload())
		// ack message
		consumer.Ack(msg)
	}
}

func TestAckIDList(t *testing.T) {
	for _, params := range []bool{true, false} {
		t.Run(fmt.Sprintf("TestAckIDList_%v", params), func(t *testing.T) {
			runAckIDListTest(t, params)
		})
	}
}

func getAckCount(registry *prometheus.Registry) (int, error) {
	metrics, err := registry.Gather()
	if err != nil {
		return 0, err
	}

	var ackCount float64
	for _, metric := range metrics {
		if metric.GetName() == "pulsar_client_consumer_acks" {
			for _, m := range metric.GetMetric() {
				ackCount += m.GetCounter().GetValue()
			}
		}
	}
	return int(ackCount), nil
}

func runAckIDListTest(t *testing.T, enableBatchIndexAck bool) {
	// Create a custom metrics registry
	registry := prometheus.NewRegistry()
	client, err := NewClient(ClientOptions{
		URL:               lookupURL,
		MetricsRegisterer: registry,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := fmt.Sprintf("test-ack-id-list-%v", time.Now().Nanosecond())

	consumer := createSharedConsumer(t, client, topic, enableBatchIndexAck)
	sendMessages(t, client, topic, 0, 5, true)  // entry 0: [0, 1, 2, 3, 4]
	sendMessages(t, client, topic, 5, 3, false) // entry 2: [5], 3: [6], 4: [7]
	sendMessages(t, client, topic, 8, 2, true)  // entry 5: [8, 9]

	msgs := receiveMessages(t, consumer, 10)
	originalMsgIDs := make([]MessageID, 0)
	for i := 0; i < 10; i++ {
		originalMsgIDs = append(originalMsgIDs, msgs[i].ID())
		assert.Equal(t, fmt.Sprintf("msg-%d", i), string(msgs[i].Payload()))
	}

	ackedIndexes := []int{0, 2, 3, 6, 8, 9}
	unackedIndexes := []int{1, 4, 5, 7}
	if !enableBatchIndexAck {
		// [0, 4] is the first batch range but only partial of it is acked
		unackedIndexes = []int{0, 1, 2, 3, 4, 5, 7}
	}
	msgIDs := make([]MessageID, len(ackedIndexes))
	for i := 0; i < len(ackedIndexes); i++ {
		msgIDs[i] = msgs[ackedIndexes[i]].ID()
	}
	assert.Nil(t, consumer.AckIDList(msgIDs))
	ackCnt, err := getAckCount(registry)
	expectedAckCnt := len(msgIDs)
	assert.NoError(t, err)
	assert.Equal(t, expectedAckCnt, ackCnt)
	consumer.Close()

	consumer = createSharedConsumer(t, client, topic, enableBatchIndexAck)
	msgs = receiveMessages(t, consumer, len(unackedIndexes))
	for i := 0; i < len(unackedIndexes); i++ {
		assert.Equal(t, fmt.Sprintf("msg-%d", unackedIndexes[i]), string(msgs[i].Payload()))
	}

	if !enableBatchIndexAck {
		msgIDs = make([]MessageID, 0)
		for i := 0; i < 5; i++ {
			msgIDs = append(msgIDs, originalMsgIDs[i])
		}
		assert.Nil(t, consumer.AckIDList(msgIDs))
		expectedAckCnt = expectedAckCnt + len(msgIDs)
		ackCnt, err = getAckCount(registry)
		assert.NoError(t, err)
		assert.Equal(t, expectedAckCnt, ackCnt)
		consumer.Close()

		consumer = createSharedConsumer(t, client, topic, enableBatchIndexAck)
		msgs = receiveMessages(t, consumer, 2)
		assert.Equal(t, "msg-5", string(msgs[0].Payload()))
		assert.Equal(t, "msg-7", string(msgs[1].Payload()))
		consumer.Close()
	}
	consumer.Close()
	err = consumer.AckIDList(msgIDs)
	assert.NotNil(t, err)
	if ackError := err.(AckError); ackError != nil {
		assert.Equal(t, len(msgIDs), len(ackError))
		for _, id := range msgIDs {
			assert.Contains(t, ackError, id)
			assert.Equal(t, "consumer state is closed", ackError[id].Error())
		}
	} else {
		assert.Fail(t, "AckIDList should return AckError")
	}

	ackCnt, err = getAckCount(registry)
	assert.NoError(t, err)
	assert.Equal(t, expectedAckCnt, ackCnt) // The Ack Counter shouldn't be increased.
}

func createSharedConsumer(t *testing.T, client Client, topic string, enableBatchIndexAck bool) Consumer {
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                          topic,
		SubscriptionName:               "my-sub",
		SubscriptionInitialPosition:    SubscriptionPositionEarliest,
		Type:                           Shared,
		EnableBatchIndexAcknowledgment: enableBatchIndexAck,
		AckWithResponse:                true,
	})
	assert.Nil(t, err)
	return consumer
}

func sendMessages(t *testing.T, client Client, topic string, startIndex int, numMessages int, batching bool) {
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   topic,
		DisableBatching:         !batching,
		BatchingMaxMessages:     uint(numMessages),
		BatchingMaxSize:         1024 * 1024 * 10,
		BatchingMaxPublishDelay: 1 * time.Hour,
	})
	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()
	for i := 0; i < numMessages; i++ {
		msg := &ProducerMessage{Payload: []byte(fmt.Sprintf("msg-%d", startIndex+i))}
		if batching {
			producer.SendAsync(ctx, msg, func(_ MessageID, _ *ProducerMessage, err error) {
				if err != nil {
					t.Logf("Failed to send message: %v", err)
				}
			})
		} else {
			if _, err := producer.Send(ctx, msg); err != nil {
				assert.Fail(t, "Failed to send message: %v", err)
			}
		}
	}
	assert.Nil(t, producer.FlushWithCtx(ctx))
}

func receiveMessages(t *testing.T, consumer Consumer, numMessages int) []Message {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msgs := make([]Message, 0)
	for i := 0; i < numMessages; i++ {
		if msg, err := consumer.Receive(ctx); err == nil {
			msgs = append(msgs, msg)
		} else {
			t.Logf("Failed to receive message: %v", err)
			break
		}
	}
	assert.Equal(t, numMessages, len(msgs))
	return msgs
}

func TestAckResponseNotBlocked(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:              lookupURL,
		OperationTimeout: 5 * time.Second,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := fmt.Sprintf("test-ack-response-not-blocked-%v", time.Now().Nanosecond())
	assert.Nil(t, createPartitionedTopic(topic, 10))

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	assert.Nil(t, err)

	ctx := context.Background()
	numMessages := 1000
	for i := 0; i < numMessages; i++ {
		producer.SendAsync(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("value-%d", i)),
		}, func(_ MessageID, _ *ProducerMessage, err error) {
			if err != nil {
				t.Fatal(err)
			}
		})
		if i%100 == 99 {
			assert.Nil(t, producer.FlushWithCtx(ctx))
		}
	}
	producer.FlushWithCtx(ctx)
	producer.Close()

	// Set a small receiver queue size to trigger ack response blocking if the internal `queueCh`
	// is a channel with the same size
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                          topic,
		SubscriptionName:               "my-sub",
		SubscriptionInitialPosition:    SubscriptionPositionEarliest,
		Type:                           KeyShared,
		EnableBatchIndexAcknowledgment: true,
		AckWithResponse:                true,
		ReceiverQueueSize:              5,
	})
	assert.Nil(t, err)
	msgIDs := make([]MessageID, 0)
	for i := 0; i < numMessages; i++ {
		if msg, err := consumer.Receive(context.Background()); err != nil {
			t.Fatal(err)
		} else {
			msgIDs = append(msgIDs, msg.ID())
			if len(msgIDs) >= 10 {
				if err := consumer.AckIDList(msgIDs); err != nil {
					t.Fatal("Failed to acked messages: ", msgIDs, " ", err)
				} else {
					t.Log("Acked messages: ", msgIDs)
				}
				msgIDs = msgIDs[:0]
			}
		}
	}
}

func TestConsumerKeepReconnectingAndThenCallClose(t *testing.T) {
	req := testcontainers.ContainerRequest{
		Image:        getPulsarTestImage(),
		ExposedPorts: []string{"6650/tcp", "8080/tcp"},
		WaitingFor:   wait.ForExposedPort(),
		Cmd:          []string{"bin/pulsar", "standalone", "-nfw"},
	}
	c, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "Failed to start the pulsar container")
	endpoint, err := c.PortEndpoint(context.Background(), "6650", "pulsar")
	require.NoError(t, err, "Failed to get the pulsar endpoint")

	client, err := NewClient(ClientOptions{
		URL:               endpoint,
		ConnectionTimeout: 5 * time.Second,
		OperationTimeout:  5 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	var testConsumer Consumer
	require.Eventually(t, func() bool {
		testConsumer, err = client.Subscribe(ConsumerOptions{
			Topic:            newTopicName(),
			Schema:           NewBytesSchema(nil),
			SubscriptionName: "test-sub",
		})
		return err == nil
	}, 30*time.Second, 1*time.Second)
	_ = c.Terminate(context.Background())
	require.Eventually(t, func() bool {
		testConsumer.Close()
		return true
	}, 30*time.Second, 1*time.Second)
}

func TestClientVersion(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	cfg := &config.Config{}
	admin, err := admin.New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	topicName, err := utils.GetTopicName(topic)
	assert.Nil(t, err)
	topicState, err := admin.Topics().GetStats(*topicName)
	assert.Nil(t, err)
	publisher := topicState.Publishers[0]
	assert.True(t, strings.HasPrefix(publisher.ClientVersion, "Pulsar-Go-version"))
	assert.NotContains(t, publisher.ClientVersion, " ")

	topic = newTopicName()
	client, err = NewClient(ClientOptions{
		URL:         lookupURL,
		Description: "test-client",
	})
	assert.Nil(t, err)
	producer, err = client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	topicName, err = utils.GetTopicName(topic)
	assert.Nil(t, err)
	topicState, err = admin.Topics().GetStats(*topicName)
	assert.Nil(t, err)
	publisher = topicState.Publishers[0]
	assert.True(t, strings.HasPrefix(publisher.ClientVersion, "Pulsar-Go-version"))
	assert.True(t, strings.HasSuffix(publisher.ClientVersion, "-test-client"))
	assert.NotContains(t, publisher.ClientVersion, " ")
}

func TestSelectConnectionForSameConsumer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                     serviceURL,
		MaxConnectionsPerBroker: 10,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()

	_consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
		Type:             Shared,
	})
	assert.NoError(t, err)
	defer _consumer.Close()

	partitionConsumerImpl := _consumer.(*consumer).consumers[0]
	conn := partitionConsumerImpl._getConn()

	for i := 0; i < 5; i++ {
		assert.NoError(t, partitionConsumerImpl.grabConn(""))
		assert.Equal(t, conn.ID(), partitionConsumerImpl._getConn().ID(),
			"The consumer uses a different connection when reconnecting")
	}
}
