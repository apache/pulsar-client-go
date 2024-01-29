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
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"

	log "github.com/sirupsen/logrus"

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
)

func TestInvalidURL(t *testing.T) {
	client, err := NewClient(ClientOptions{})

	if client != nil || err == nil {
		t.Fatal("Should have failed to create client")
	}
}

func TestProducerConnectError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://invalid-hostname:6650",
	})

	assert.Nil(t, err)

	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	// Expect error in creating producer
	assert.Nil(t, producer)
	assert.NotNil(t, err)

	assert.Equal(t, err.Error(), "connection error")
}

func TestProducerNoTopic(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{})

	// Expect error in creating producer
	assert.Nil(t, producer)
	assert.NotNil(t, err)

	assert.Equal(t, InvalidTopicName, err.(*Error).Result())
}

func TestSimpleProducer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	for i := 0; i < 10; i++ {
		ID, err := producer.Send(context.Background(), &ProducerMessage{
			Payload: []byte("hello"),
		})

		assert.NoError(t, err)
		assert.NotNil(t, ID)
	}

	_, err = producer.Send(context.Background(), nil)
	assert.NotNil(t, err)

	_, err = producer.Send(context.Background(), &ProducerMessage{
		Payload: []byte("hello"),
		Value:   []byte("hello"),
	})
	assert.NotNil(t, err)
}

func TestProducerAsyncSend(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   newTopicName(),
		BatchingMaxPublishDelay: 1 * time.Second,
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	wg := sync.WaitGroup{}
	wg.Add(10)
	errors := internal.NewBlockingQueue(10)

	for i := 0; i < 10; i++ {
		producer.SendAsync(context.Background(), &ProducerMessage{
			Payload: []byte("hello"),
		}, func(id MessageID, message *ProducerMessage, e error) {
			if e != nil {
				log.WithError(e).Error("Failed to publish")
				errors.Put(e)
			} else {
				log.Info("Published message ", id)
			}
			wg.Done()
		})

		assert.NoError(t, err)
	}

	err = producer.Flush()
	assert.Nil(t, err)

	wg.Wait()

	assert.Equal(t, 0, errors.Size())

	wg.Add(1)
	producer.SendAsync(context.Background(), nil, func(id MessageID, m *ProducerMessage, e error) {
		assert.NotNil(t, e)
		assert.Nil(t, id)
		wg.Done()
	})
	wg.Wait()

	wg.Add(1)
	producer.SendAsync(context.Background(), &ProducerMessage{Payload: []byte("hello"), Value: []byte("hello")},
		func(id MessageID, m *ProducerMessage, e error) {
			assert.NotNil(t, e)
			assert.Nil(t, id)
			wg.Done()
		})
	wg.Wait()
}

func TestProducerFlushDisableBatching(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           newTopicName(),
		DisableBatching: true,
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	wg := sync.WaitGroup{}
	wg.Add(10)
	errors := internal.NewBlockingQueue(10)

	for i := 0; i < 10; i++ {
		producer.SendAsync(context.Background(), &ProducerMessage{
			Payload: []byte("hello"),
		}, func(id MessageID, message *ProducerMessage, e error) {
			if e != nil {
				log.WithError(e).Error("Failed to publish")
				errors.Put(e)
			} else {
				log.Info("Published message ", id)
			}
			wg.Done()
		})

		assert.NoError(t, err)
	}

	err = producer.Flush()
	assert.Nil(t, err)

	wg.Wait()

	assert.Equal(t, 0, errors.Size())
}

func TestProducerCompression(t *testing.T) {
	type testProvider struct {
		name            string
		compressionType CompressionType
	}

	providers := []testProvider{
		{"zlib", ZLib},
		{"lz4", LZ4},
		{"zstd", ZSTD},
	}

	for _, provider := range providers {
		p := provider
		t.Run(p.name, func(t *testing.T) {
			client, err := NewClient(ClientOptions{
				URL: serviceURL,
			})
			assert.NoError(t, err)
			defer client.Close()

			producer, err := client.CreateProducer(ProducerOptions{
				Topic:           newTopicName(),
				CompressionType: p.compressionType,
			})

			assert.NoError(t, err)
			assert.NotNil(t, producer)
			defer producer.Close()

			for i := 0; i < 10; i++ {
				ID, err := producer.Send(context.Background(), &ProducerMessage{
					Payload: []byte("hello"),
				})

				assert.NoError(t, err)
				assert.NotNil(t, ID)
			}
		})
	}
}

func TestProducerLastSequenceID(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	assert.Equal(t, int64(-1), producer.LastSequenceID())

	for i := 0; i < 10; i++ {
		ID, err := producer.Send(context.Background(), &ProducerMessage{
			Payload: []byte("hello"),
		})

		assert.NoError(t, err)
		assert.NotNil(t, ID)
		assert.Equal(t, int64(i), producer.LastSequenceID())
	}
}

func TestEventTime(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := "test-event-time"
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "subName",
	})
	assert.Nil(t, err)
	defer consumer.Close()

	eventTime := timeFromUnixTimestampMillis(uint64(1565161612))
	ID, err := producer.Send(context.Background(), &ProducerMessage{
		Payload:   []byte("test-event-time"),
		EventTime: eventTime,
	})
	assert.Nil(t, err)
	assert.NotNil(t, ID)

	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	actualEventTime := msg.EventTime()
	assert.Equal(t, eventTime.Unix(), actualEventTime.Unix())
}

func TestFlushInProducer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := "test-flush-in-producer"
	subName := "subscription-name"
	numOfMessages := 10
	ctx := context.Background()

	// set batch message number numOfMessages, and max delay 10s
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   topicName,
		DisableBatching:         false,
		BatchingMaxMessages:     uint(numOfMessages),
		BatchingMaxPublishDelay: time.Second * 10,
		Properties: map[string]string{
			"producer-name": "test-producer-name",
			"producer-id":   "test-producer-id",
		},
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: subName,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	prefix := "msg-batch-async"
	msgCount := 0

	wg := sync.WaitGroup{}
	wg.Add(5)
	errors := internal.NewBlockingQueue(10)
	for i := 0; i < numOfMessages/2; i++ {
		messageContent := prefix + fmt.Sprintf("%d", i)
		producer.SendAsync(ctx, &ProducerMessage{
			Payload: []byte(messageContent),
		}, func(id MessageID, producerMessage *ProducerMessage, e error) {
			if e != nil {
				log.WithError(e).Error("Failed to publish")
				errors.Put(e)
			} else {
				log.Info("Published message ", id)
			}
			wg.Done()
		})
		assert.Nil(t, err)
	}
	err = producer.Flush()
	assert.Nil(t, err)
	wg.Wait()

	var ledgerID int64 = -1
	var entryID int64 = -1

	for i := 0; i < numOfMessages/2; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		msgCount++

		msgID := msg.ID().(*trackingMessageID)
		// Since messages are batched, they will be sharing the same ledgerId/entryId
		if ledgerID == -1 {
			ledgerID = msgID.ledgerID
			entryID = msgID.entryID
		} else {
			assert.Equal(t, ledgerID, msgID.ledgerID)
			assert.Equal(t, entryID, msgID.entryID)
		}
	}

	assert.Equal(t, msgCount, numOfMessages/2)

	wg.Add(5)
	for i := numOfMessages / 2; i < numOfMessages; i++ {
		messageContent := prefix + fmt.Sprintf("%d", i)
		producer.SendAsync(ctx, &ProducerMessage{
			Payload: []byte(messageContent),
		}, func(id MessageID, producerMessage *ProducerMessage, e error) {
			if e != nil {
				log.WithError(e).Error("Failed to publish")
				errors.Put(e)
			} else {
				log.Info("Published message ", id)
			}
			wg.Done()
		})
		assert.Nil(t, err)
	}

	err = producer.Flush()
	assert.Nil(t, err)
	wg.Wait()

	for i := numOfMessages / 2; i < numOfMessages; i++ {
		_, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		msgCount++
	}
	assert.Equal(t, msgCount, numOfMessages)
}

func TestFlushInPartitionedProducer(t *testing.T) {
	topicName := "public/default/partition-testFlushInPartitionedProducer"

	// call admin api to make it partitioned
	url := adminURL + "/" + "admin/v2/persistent/" + topicName + "/partitions"
	makeHTTPCall(t, http.MethodPut, url, "5")

	numberOfPartitions := 5
	numOfMessages := 10
	ctx := context.Background()

	// creat client connection
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "my-sub",
		Type:             Exclusive,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer and set batch message number numOfMessages, and max delay 10s
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   topicName,
		DisableBatching:         false,
		BatchingMaxMessages:     uint(numOfMessages / numberOfPartitions),
		BatchingMaxPublishDelay: time.Second * 10,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 5 messages
	prefix := "msg-batch-async-"
	wg := sync.WaitGroup{}
	wg.Add(5)
	errors := internal.NewBlockingQueue(5)
	for i := 0; i < numOfMessages/2; i++ {
		messageContent := prefix + fmt.Sprintf("%d", i)
		producer.SendAsync(ctx, &ProducerMessage{
			Payload: []byte(messageContent),
		}, func(id MessageID, producerMessage *ProducerMessage, e error) {
			if e != nil {
				log.WithError(e).Error("Failed to publish")
				errors.Put(e)
			} else {
				log.Info("Published message: ", id)
			}
			wg.Done()
		})
		assert.Nil(t, err)
	}

	// After flush, should be able to consume.
	err = producer.Flush()
	assert.Nil(t, err)

	wg.Wait()

	// Receive all messages
	msgCount := 0
	for i := 0; i < numOfMessages/2; i++ {
		msg, err := consumer.Receive(ctx)
		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))
		assert.Nil(t, err)
		consumer.Ack(msg)
		msgCount++
	}
	assert.Equal(t, msgCount, numOfMessages/2)
}

func TestRoundRobinRouterPartitionedProducer(t *testing.T) {
	topicName := "public/default/partition-testRoundRobinRouterPartitionedProducer"
	numberOfPartitions := 5

	// call admin api to make it partitioned
	url := adminURL + "/" + "admin/v2/persistent/" + topicName + "/partitions"
	makeHTTPCall(t, http.MethodPut, url, strconv.Itoa(numberOfPartitions))

	numOfMessages := 10
	ctx := context.Background()

	// creat client connection
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "my-sub",
		Type:             Exclusive,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 5 messages
	prefix := "msg-"

	for i := 0; i < numOfMessages; i++ {
		messageContent := prefix + fmt.Sprintf("%d", i)
		_, err = producer.Send(ctx, &ProducerMessage{
			Payload: []byte(messageContent),
		})
		assert.Nil(t, err)
	}

	// Receive all messages
	msgCount := 0
	msgPartitionMap := make(map[string]int)
	for i := 0; i < numOfMessages; i++ {
		msg, err := consumer.Receive(ctx)
		fmt.Printf("Received message msgId: %#v topic: %s-- content: '%s'\n",
			msg.ID(), msg.Topic(), string(msg.Payload()))
		assert.Nil(t, err)
		consumer.Ack(msg)
		msgCount++
		msgPartitionMap[msg.Topic()]++
	}
	assert.Equal(t, msgCount, numOfMessages)
	assert.Equal(t, numberOfPartitions, len(msgPartitionMap))
	for _, count := range msgPartitionMap {
		assert.Equal(t, count, numOfMessages/numberOfPartitions)
	}
}

func TestMessageRouter(t *testing.T) {
	// Create topic with 5 partitions
	topicAdminURL := "admin/v2/persistent/public/default/my-partitioned-topic/partitions"
	err := httpPut(topicAdminURL, 5)
	defer httpDelete(topicAdminURL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	// Only subscribe on the specific partition
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "my-partitioned-topic-partition-2",
		SubscriptionName: "my-sub",
	})

	assert.Nil(t, err)
	defer consumer.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: "my-partitioned-topic",
		MessageRouter: func(msg *ProducerMessage, tm TopicMetadata) int {
			fmt.Println("Routing message ", msg, " -- Partitions: ", tm.NumPartitions())
			return 2
		},
	})

	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()

	ID, err := producer.Send(ctx, &ProducerMessage{
		Payload: []byte("hello"),
	})
	assert.Nil(t, err)
	assert.NotNil(t, ID)

	fmt.Println("PUBLISHED")

	// Verify message was published on partition 2
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, msg)
	assert.Equal(t, string(msg.Payload()), "hello")
}
func TestMessageSingleRouter(t *testing.T) {
	// Create topic with 5 partitions
	topicAdminURL := "admin/v2/persistent/public/default/my-single-partitioned-topic/partitions"
	err := httpPut(topicAdminURL, 5)
	defer httpDelete(topicAdminURL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	numOfMessages := 10

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            "my-single-partitioned-topic",
		SubscriptionName: "my-sub",
	})

	assert.Nil(t, err)
	defer consumer.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:         "my-single-partitioned-topic",
		MessageRouter: NewSinglePartitionRouter(),
	})

	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()

	for i := 0; i < numOfMessages; i++ {
		ID, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte("hello"),
		})
		assert.Nil(t, err)
		assert.NotNil(t, ID)
	}

	// Verify message was published on single partition
	msgCount := 0
	msgPartitionMap := make(map[string]int)
	for i := 0; i < numOfMessages; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.NotNil(t, msg)
		consumer.Ack(msg)
		msgCount++
		msgPartitionMap[msg.Topic()]++
	}
	assert.Equal(t, msgCount, numOfMessages)
	assert.Equal(t, len(msgPartitionMap), 1)
	for _, i := range msgPartitionMap {
		assert.Equal(t, i, numOfMessages)
	}

}

func TestNonPersistentTopic(t *testing.T) {
	topicName := "non-persistent://public/default/testNonPersistentTopic"
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})

	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "my-sub",
	})
	assert.Nil(t, err)
	defer consumer.Close()
}

func TestProducerDuplicateNameOnSameTopic(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topicName := newTopicName()
	producerName := "my-producer"

	p1, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
		Name:  producerName,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p1.Close()

	_, err = client.CreateProducer(ProducerOptions{
		Topic: topicName,
		Name:  producerName,
	})
	assert.NotNil(t, err, "expected error when creating producer with same name")
}

func TestProducerMetadata(t *testing.T) {
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
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:      topic,
		Name:       "meta-data-producer",
		Properties: props,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	stats, err := topicStats(topic)
	if err != nil {
		t.Fatal(err)
	}

	meta := stats["publishers"].([]interface{})[0].(map[string]interface{})["metadata"].(map[string]interface{})
	assert.Equal(t, len(props), len(meta))
	for k, v := range props {
		mv := meta[k].(string)
		assert.Equal(t, v, mv)
	}
}

// test for issues #76, #114 and #123
func TestBatchMessageFlushing(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topic := newTopicName()
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	maxBytes := defaultMaxBatchSize
	genbytes := func(n int) []byte {
		c := []byte("a")[0]
		bytes := make([]byte, n)
		for i := 0; i < n; i++ {
			bytes[i] = c
		}
		return bytes
	}

	msgs := [][]byte{
		genbytes(maxBytes - 10),
		genbytes(11),
	}

	ch := make(chan struct{}, 2)
	ctx := context.Background()
	for _, msg := range msgs {
		msg := &ProducerMessage{
			Payload: msg,
		}
		producer.SendAsync(ctx, msg, func(id MessageID, producerMessage *ProducerMessage, err error) {
			ch <- struct{}{}
		})
	}

	published := 0
	keepGoing := true
	for keepGoing {
		select {
		case <-ch:
			published++
			if published == 2 {
				keepGoing = false
			}
		case <-time.After(defaultBatchingMaxPublishDelay * 10):
			fmt.Println("TestBatchMessageFlushing timeout waiting to publish messages")
			keepGoing = false
		}
	}

	assert.Equal(t, 2, published, "expected to publish two messages")
}

// test for issue #367
func TestBatchDelayMessage(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	batchingDelay := time.Second
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   topic,
		BatchingMaxPublishDelay: batchingDelay,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "subName",
	})
	assert.Nil(t, err)
	defer consumer.Close()

	ctx := context.Background()
	delayMsg := &ProducerMessage{
		Payload:      []byte("delay: 3s"),
		DeliverAfter: 3 * time.Second,
	}
	var delayMsgID int64
	ch := make(chan struct{}, 2)
	producer.SendAsync(ctx, delayMsg, func(id MessageID, producerMessage *ProducerMessage, err error) {
		atomic.StoreInt64(&delayMsgID, id.(*messageID).entryID)
		ch <- struct{}{}
	})
	delayMsgPublished := false
	select {
	case <-ch:
		delayMsgPublished = true
	case <-time.After(batchingDelay):
	}
	assert.Equal(t, delayMsgPublished, true, "delay message is not published individually when batching is enabled")

	noDelayMsg := &ProducerMessage{
		Payload: []byte("no delay"),
	}
	var noDelayMsgID int64
	producer.SendAsync(ctx, noDelayMsg, func(id MessageID, producerMessage *ProducerMessage, err error) {
		atomic.StoreInt64(&noDelayMsgID, id.(*messageID).entryID)
	})
	for i := 0; i < 2; i++ {
		msg, err := consumer.Receive(context.Background())
		assert.Nil(t, err, "unexpected error occurred when recving message from topic")

		switch msg.ID().(*trackingMessageID).entryID {
		case atomic.LoadInt64(&noDelayMsgID):
			assert.LessOrEqual(t, time.Since(msg.PublishTime()).Nanoseconds(), int64(batchingDelay*2))
		case atomic.LoadInt64(&delayMsgID):
			assert.GreaterOrEqual(t, time.Since(msg.PublishTime()).Nanoseconds(), int64(time.Second*3))
		default:
			t.Fatalf("got an unexpected message from topic, id:%v", msg.ID().Serialize())
		}
	}
}

func TestDelayRelative(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "subName",
		Type:             Shared,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	ID, err := producer.Send(context.Background(), &ProducerMessage{
		Payload:      []byte("test"),
		DeliverAfter: 3 * time.Second,
	})
	assert.Nil(t, err)
	assert.NotNil(t, ID)

	ctx, canc := context.WithTimeout(context.Background(), 1*time.Second)

	msg, err := consumer.Receive(ctx)
	assert.Error(t, err)
	assert.Nil(t, msg)
	canc()

	ctx, canc = context.WithTimeout(context.Background(), 5*time.Second)
	msg, err = consumer.Receive(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, msg)
	canc()
}

func TestDelayAbsolute(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "subName",
		Type:             Shared,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	ID, err := producer.Send(context.Background(), &ProducerMessage{
		Payload:   []byte("test"),
		DeliverAt: time.Now().Add(3 * time.Second),
	})
	assert.Nil(t, err)
	assert.NotNil(t, ID)

	ctx, canc := context.WithTimeout(context.Background(), 1*time.Second)

	msg, err := consumer.Receive(ctx)
	assert.Error(t, err)
	assert.Nil(t, msg)
	canc()

	ctx, canc = context.WithTimeout(context.Background(), 5*time.Second)
	msg, err = consumer.Receive(ctx)
	assert.Nil(t, err)
	assert.NotNil(t, msg)
	canc()
}

func TestMaxBatchSize(t *testing.T) {
	// Set to be < serverMaxMessageSize
	batchMaxMessageSize := 512 * 1024

	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           newTopicName(),
		BatchingMaxSize: uint(batchMaxMessageSize),
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	for bias := -1; bias <= 3; bias++ {
		payload := make([]byte, batchMaxMessageSize+bias)
		ID, err := producer.Send(context.Background(), &ProducerMessage{
			Payload: payload,
		})
		// regardless max batch size, if the batch size limit is reached, batching is triggered to send messages
		assert.NoError(t, err)
		assert.NotNil(t, ID)
	}
}

func TestBatchingDisabled(t *testing.T) {
	defaultMaxMessageSize := 128 * 1024
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	// when batching is disabled, the batching size has no effect
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           newTopicName(),
		DisableBatching: true,
		BatchingMaxSize: uint(defaultMaxBatchSize),
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	payload := make([]byte, defaultMaxMessageSize+100)
	ID, err := producer.Send(context.Background(), &ProducerMessage{
		Payload: payload,
	})
	// regardless max batch size, if the batch size limit is reached, batching is triggered to send messages
	assert.NoError(t, err)
	assert.NotNil(t, ID)
}

func TestMaxMessageSize(t *testing.T) {
	serverMaxMessageSize := 1024 * 1024

	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	// Need to set BatchingMaxSize > serverMaxMessageSize to avoid ErrMessageTooLarge
	// being masked by an earlier ErrFailAddToBatch
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           newTopicName(),
		BatchingMaxSize: uint(2 * serverMaxMessageSize),
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	// producer2 disable batching
	producer2, err := client.CreateProducer(ProducerOptions{
		Topic:           newTopicName(),
		DisableBatching: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer2)
	defer producer2.Close()

	// When serverMaxMessageSize=1024, the batch payload=1041
	// The totalSize includes:
	// | singleMsgMetadataLength | singleMsgMetadata | payload |
	// | ----------------------- | ----------------- | ------- |
	// | 4                       | 13                | 1024    |
	// So when bias <= 0, the uncompressed payload will not exceed maxMessageSize,
	// but encryptedPayloadSize exceeds maxMessageSize, Send() will return an internal error.
	// When bias = 1, the first check of maxMessageSize (for uncompressed payload) is valid,
	// Send() will return ErrMessageTooLarge
	for bias := -1; bias <= 1; bias++ {
		payload := make([]byte, serverMaxMessageSize+bias)
		ID, err := producer.Send(context.Background(), &ProducerMessage{
			Payload: payload,
		})
		if bias <= 0 {
			assert.Equal(t, true, errors.Is(err, internal.ErrExceedMaxMessageSize))
			assert.Nil(t, ID)
		} else {
			assert.True(t, errors.Is(err, ErrMessageTooLarge))
		}
	}

	for bias := -1; bias <= 1; bias++ {
		payload := make([]byte, serverMaxMessageSize+bias)
		ID, err := producer2.Send(context.Background(), &ProducerMessage{
			Payload: payload,
		})
		if bias <= 0 {
			assert.Equal(t, true, errors.Is(err, internal.ErrExceedMaxMessageSize))
			assert.Nil(t, ID)
		} else {
			assert.True(t, errors.Is(err, ErrMessageTooLarge))
		}
	}
}

func TestFailedSchemaEncode(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: NewAvroSchema("{\"type\":\"string\"}", nil),
	})

	assert.Nil(t, err)
	defer producer.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// producer should send return an error as message is Int64, but schema is String
		mid, err := producer.Send(ctx, &ProducerMessage{
			Value: int64(1),
		})
		assert.NotNil(t, err)
		assert.Nil(t, mid)
		wg.Done()
	}()

	wg.Add(1)
	// producer should send return an error as message is Int64, but schema is String
	producer.SendAsync(ctx, &ProducerMessage{
		Value: int64(1),
	}, func(messageID MessageID, producerMessage *ProducerMessage, err error) {
		assert.NotNil(t, err)
		assert.Nil(t, messageID)
		wg.Done()
	})
	wg.Wait()
}

func TestTopicTermination(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "topic_terminated_sub",
	})
	assert.Nil(t, err)
	defer consumer.Close() // subscribe but do nothing

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:       topicName,
		SendTimeout: 2 * time.Second,
	})
	assert.Nil(t, err)
	defer producer.Close()

	afterCh := time.After(5 * time.Second)
	terminatedChan := make(chan bool)
	go func() {
		for {
			_, err := producer.Send(context.Background(), &ProducerMessage{
				Payload: make([]byte, 1024),
			})
			if err != nil {
				if errors.Is(err, ErrTopicTerminated) || errors.Is(err, ErrProducerClosed) {
					terminatedChan <- true
				} else {
					terminatedChan <- false
				}
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	terminateURL := adminURL + "/admin/v2/persistent/public/default/" + topicName + "/terminate"
	log.Info(terminateURL)
	makeHTTPCall(t, http.MethodPost, terminateURL, "")

	for {
		select {
		case d := <-terminatedChan:
			assert.Equal(t, d, true)
			return
		case <-afterCh:
			assert.Fail(t, "Time is up. Topic should have been terminated by now")
			return
		}
	}
}

func TestSendTimeout(t *testing.T) {
	quotaURL := adminURL + "/admin/v2/namespaces/public/default/backlogQuota"
	quotaFmt := `{"limit": "%d", "policy": "producer_request_hold"}`
	makeHTTPCall(t, http.MethodPost, quotaURL, fmt.Sprintf(quotaFmt, 10*1024))

	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "send_timeout_sub",
	})
	assert.Nil(t, err)
	defer consumer.Close() // subscribe but do nothing

	noRetry := uint(0)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                topicName,
		SendTimeout:          2 * time.Second,
		MaxReconnectToBroker: &noRetry,
	})
	assert.Nil(t, err)
	defer producer.Close()

	for i := 0; i < 10; i++ {
		id, err := producer.Send(context.Background(), &ProducerMessage{
			Payload: make([]byte, 1024),
		})
		assert.Nil(t, err)
		assert.NotNil(t, id)
	}

	// waiting for the backlog check
	time.Sleep((5 + 1) * time.Second)

	id, err := producer.Send(context.Background(), &ProducerMessage{
		Payload: make([]byte, 1024),
	})
	assert.NotNil(t, err)
	assert.Nil(t, id)

	makeHTTPCall(t, http.MethodDelete, quotaURL, "")
}

func TestProducerWithBackoffPolicy(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()

	backoff := newTestBackoffPolicy(1*time.Second, 4*time.Second)
	_producer, err := client.CreateProducer(ProducerOptions{
		Topic:         topicName,
		SendTimeout:   2 * time.Second,
		BackoffPolicy: backoff,
	})
	assert.Nil(t, err)
	defer _producer.Close()

	partitionProducerImp := _producer.(*producer).producers[0].(*partitionProducer)
	// 1 s
	startTime := time.Now()
	partitionProducerImp.reconnectToBroker()
	assert.True(t, backoff.IsExpectedIntervalFrom(startTime))

	// 2 s
	startTime = time.Now()
	partitionProducerImp.reconnectToBroker()
	assert.True(t, backoff.IsExpectedIntervalFrom(startTime))

	// 4 s
	startTime = time.Now()
	partitionProducerImp.reconnectToBroker()
	assert.True(t, backoff.IsExpectedIntervalFrom(startTime))

	// 4 s
	startTime = time.Now()
	partitionProducerImp.reconnectToBroker()
	assert.True(t, backoff.IsExpectedIntervalFrom(startTime))
}

func TestSendContextExpired(t *testing.T) {
	quotaURL := adminURL + "/admin/v2/namespaces/public/default/backlogQuota"
	quotaFmt := `{"limit": "%d", "policy": "producer_request_hold"}`
	makeHTTPCall(t, http.MethodPost, quotaURL, fmt.Sprintf(quotaFmt, 1024))

	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "send_context_expired_sub",
	})
	assert.Nil(t, err)
	defer consumer.Close() // subscribe but do nothing

	noRetry := uint(0)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                topicName,
		MaxPendingMessages:   1,
		SendTimeout:          2 * time.Second,
		MaxReconnectToBroker: &noRetry,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// first send completes and fills the available backlog
	id, err := producer.Send(context.Background(), &ProducerMessage{
		Payload: make([]byte, 1024),
	})
	assert.Nil(t, err)
	assert.NotNil(t, id)

	// waiting for the backlog check
	time.Sleep((5 + 1) * time.Second)

	// next publish will not complete due to the backlog quota being full;
	// this consumes the only available MaxPendingMessages permit
	wg := sync.WaitGroup{}
	wg.Add(1)
	producer.SendAsync(context.Background(), &ProducerMessage{
		Payload: make([]byte, 1024),
	}, func(_ MessageID, _ *ProducerMessage, _ error) {
		// we're not interested in the result of this send, but we don't
		// want to exit this test case until it completes
		wg.Done()
	})

	// final publish will block waiting for a send permit to become available
	// then fail when the ctx times out
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	id, err = producer.Send(ctx, &ProducerMessage{
		Payload: make([]byte, 1024),
	})
	assert.NotNil(t, err)
	assert.Nil(t, id)

	wg.Wait()

	makeHTTPCall(t, http.MethodDelete, quotaURL, "")
}

func TestProducerWithRSAEncryption(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	msgCrypto, err := crypto.NewDefaultMessageCrypto("testing", true, plog.DefaultNopLogger())
	assert.Nil(t, err)

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			MessageCrypto: msgCrypto,
			Keys:          []string{"my-app.key"},
		},
		Schema: NewStringSchema(nil),
	})

	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Value: fmt.Sprintf("hello-%d", i),
		}); err != nil {
			log.Fatal(err)
		}
	}
}

func TestProducuerCreationFailOnNilKeyReader(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

	msgCrypto, err := crypto.NewDefaultMessageCrypto("testing", true, plog.DefaultNopLogger())
	assert.Nil(t, err)

	// create producer
	// Producer creation should fail as keyreader is nil
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
		Encryption: &ProducerEncryptionInfo{
			MessageCrypto: msgCrypto,
			Keys:          []string{"my-app.key"},
		},
		Schema: NewStringSchema(nil),
	})

	assert.NotNil(t, err)
	assert.Nil(t, producer)
}

func TestProducuerSendFailOnInvalidKey(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

	msgCrypto, err := crypto.NewDefaultMessageCrypto("testing", true, plog.DefaultNopLogger())
	assert.Nil(t, err)

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/invalid_pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			MessageCrypto: msgCrypto,
			Keys:          []string{"my-app.key"},
		},
		Schema: NewStringSchema(nil),
	})

	assert.Nil(t, err)
	assert.NotNil(t, producer)

	// producer should send return an error as keyreader is configured with wrong pub.key and fail while encrypting message
	mid, err := producer.Send(context.Background(), &ProducerMessage{
		Value: "test",
	})

	assert.NotNil(t, err)
	assert.Nil(t, mid)
}

type noopProduceInterceptor struct{}

func (noopProduceInterceptor) BeforeSend(producer Producer, message *ProducerMessage) {}

func (noopProduceInterceptor) OnSendAcknowledgement(producer Producer, message *ProducerMessage, msgID MessageID) {
}

// copyPropertyIntercepotr copy all keys in message properties map and add a suffix
type metricProduceInterceptor struct {
	sendn int
	ackn  int
}

func (x *metricProduceInterceptor) BeforeSend(producer Producer, message *ProducerMessage) {
	x.sendn++
}

func (x *metricProduceInterceptor) OnSendAcknowledgement(producer Producer, message *ProducerMessage, msgID MessageID) {
	x.ackn++
}

func TestProducerWithInterceptors(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/test-topic-interceptors"
	ctx := context.Background()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
		Type:             Exclusive,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	metric := &metricProduceInterceptor{}
	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
		Interceptors: ProducerInterceptors{
			noopProduceInterceptor{},
			metric,
		},
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			_, err := producer.Send(ctx, &ProducerMessage{
				Payload: []byte(fmt.Sprintf("hello-%d", i)),
				Key:     "pulsar",
				Properties: map[string]string{
					"key-1": "pulsar-1",
				},
			})
			assert.Nil(t, err)
		} else {
			producer.SendAsync(ctx, &ProducerMessage{
				Payload: []byte(fmt.Sprintf("hello-%d", i)),
				Key:     "pulsar",
				Properties: map[string]string{
					"key-1": "pulsar-1",
				},
			}, func(_ MessageID, _ *ProducerMessage, err error) {
				assert.Nil(t, err)
			})
			assert.Nil(t, err)
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

	assert.Equal(t, 10, metric.sendn)
	assert.Equal(t, 10, metric.ackn)
}

func TestProducerSendAfterClose(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	ID, err := producer.Send(context.Background(), &ProducerMessage{
		Payload: []byte("hello"),
	})

	assert.NoError(t, err)
	assert.NotNil(t, ID)

	producer.Close()
	ID, err = producer.Send(context.Background(), &ProducerMessage{
		Payload: []byte("hello"),
	})
	assert.Nil(t, ID)
	assert.Error(t, err)
}

func TestExactlyOnceWithProducerNameSpecified(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
		Name:  "p-name-1",
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	producer2, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
		Name:  "p-name-2",
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer2)
	defer producer2.Close()

	producer3, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
		Name:  "p-name-2",
	})

	assert.NotNil(t, err)
	assert.Nil(t, producer3)
}

func TestMultipleSchemaOfKeyBasedBatchProducerConsumer(t *testing.T) {
	const MsgBatchCount = 10
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	schema1 := NewAvroSchema(`{"fields":
		[	
			{"name":"id","type":"int"},
			{"default":null,"name":"name","type":["null","string"]}
		],
		"name":"MyAvro3","namespace":"PulsarTestCase","type":"record"}`, nil)
	schema2 := NewAvroSchema(`{"fields":
		[
				{"name":"id","type":"int"},{"default":null,"name":"name","type":["null","string"]},
			   {"default":null,"name":"age","type":["null","int"]}
		],
		"name":"MyAvro3","namespace":"PulsarTestCase","type":"record"}`, nil)
	v1 := map[string]interface{}{
		"id": 1,
		"name": map[string]interface{}{
			"string": "aac",
		},
	}
	v2 := map[string]interface{}{
		"id": 1,
		"name": map[string]interface{}{
			"string": "test",
		},
		"age": map[string]interface{}{
			"int": 10,
		},
	}
	topic := newTopicName()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:              topic,
		Schema:             schema1,
		BatcherBuilderType: KeyBasedBatchBuilder,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	keys := []string{"key1", "key2", "key3"}

	for i := 0; i < MsgBatchCount; i++ {
		var messageContent []byte
		var schema Schema
		for _, key := range keys {
			if i%2 == 0 {
				messageContent, err = schema1.Encode(v1)
				schema = schema1
				assert.NoError(t, err)
			} else {
				messageContent, err = schema2.Encode(v2)
				schema = schema2
				assert.NoError(t, err)
			}
			producer.SendAsync(context.Background(), &ProducerMessage{
				Payload: messageContent,
				Key:     key,
				Schema:  schema,
			}, func(id MessageID, producerMessage *ProducerMessage, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, id)
			})
		}

	}
	producer.Flush()

	//// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            "my-sub2",
		Type:                        Failover,
		Schema:                      schema1,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	for i := 0; i < MsgBatchCount*len(keys); i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		var v interface{}
		err = msg.GetSchemaValue(&v)
		t.Logf(`schemaVersion: %x recevice %s:%v`, msg.SchemaVersion(), msg.Key(), v)
		assert.Nil(t, err)
	}
}

func TestMultipleSchemaProducerConsumer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	schema1 := NewAvroSchema(`{"fields":
		[
			{"name":"id","type":"int"},{"default":null,"name":"name","type":["null","string"]}
		],
		"name":"MyAvro3","namespace":"PulsarTestCase","type":"record"}`, nil)
	schema2 := NewAvroSchema(`{"fields":
		[
			{"name":"id","type":"int"},{"default":null,"name":"name","type":["null","string"]},
			{"default":null,"name":"age","type":["null","int"]}
		],"name":"MyAvro3","namespace":"PulsarTestCase","type":"record"}`, nil)
	v1 := map[string]interface{}{
		"id": 1,
		"name": map[string]interface{}{
			"string": "aac",
		},
	}
	v2 := map[string]interface{}{
		"id": 1,
		"name": map[string]interface{}{
			"string": "test",
		},
		"age": map[string]interface{}{
			"int": 10,
		},
	}
	topic := newTopicName()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: schema1,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	for i := 0; i < 10; i++ {
		var messageContent []byte
		var key string
		var schema Schema
		if i%2 == 0 {
			messageContent, err = schema1.Encode(v1)
			key = "v1"
			schema = schema1
			assert.NoError(t, err)
		} else {
			messageContent, err = schema2.Encode(v2)
			key = "v2"
			schema = schema2
			assert.NoError(t, err)
		}
		producer.SendAsync(context.Background(), &ProducerMessage{
			Payload: messageContent,
			Key:     key,
			Schema:  schema,
		}, func(id MessageID, producerMessage *ProducerMessage, err error) {
			assert.NoError(t, err)
			assert.NotNil(t, id)
		})
	}
	producer.Flush()

	//// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            "my-sub2",
		Type:                        Failover,
		Schema:                      schema1,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		var v interface{}
		err = msg.GetSchemaValue(&v)
		t.Logf(`schemaVersion: %x recevice %s:%v`, msg.SchemaVersion(), msg.Key(), v)
		assert.Nil(t, err)
	}
}

func TestProducerWithSchemaAndConsumerSchemaNotFound(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	schema := NewAvroSchema(`{"fields":
	[
		{"name":"id","type":"int"},{"default":null,"name":"name","type":["null","string"]}
	],
	"name":"MyAvro","namespace":"schemaNotFoundTestCase","type":"record"}`, nil)

	topic := newTopicName()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: schema,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            "my-sub-schema-not-found",
		Type:                        Exclusive,
		Schema:                      schema,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)

	// each produced message will have schema version set in the message metadata
	for i := 0; i < 5; i++ {
		messageContent, err := schema.Encode(map[string]interface{}{
			"id": i,
			"name": map[string]interface{}{
				"string": "abc",
			},
		})
		assert.NoError(t, err)
		_, err = producer.Send(context.Background(), &ProducerMessage{
			Payload: messageContent,
		})
		assert.NoError(t, err)
	}

	// delete schema of topic
	topicSchemaDeleteURL := fmt.Sprintf("admin/v2/schemas/public/default/%v/schema", topic)
	err = httpDelete(topicSchemaDeleteURL)
	assert.NoError(t, err)

	// consume message
	msg, err := consumer.Receive(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, msg)

	// try to serialize message payload
	var v interface{}
	err = msg.GetSchemaValue(&v)
	// should fail with error but not panic
	assert.Error(t, err)
}

func TestExclusiveProducer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:              topicName,
		ProducerAccessMode: ProducerAccessModeExclusive,
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	_, err = client.CreateProducer(ProducerOptions{
		Topic:              topicName,
		ProducerAccessMode: ProducerAccessModeExclusive,
	})
	assert.Error(t, err, "Producer should be fenced")
	assert.True(t, strings.Contains(err.Error(), "ProducerFenced"))

	_, err = client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	assert.Error(t, err, "Producer should be failed")
	assert.True(t, strings.Contains(err.Error(), "ProducerBusy"))
}

func TestWaitForExclusiveProducer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
		// Set the request timeout is 200ms
		OperationTimeout: 200 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()
	producer1, err := client.CreateProducer(ProducerOptions{
		Topic:              topicName,
		ProducerAccessMode: ProducerAccessModeExclusive,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer1)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		producer2, err := client.CreateProducer(ProducerOptions{
			Topic:              topicName,
			ProducerAccessMode: ProducerAccessModeWaitForExclusive,
		})
		defer producer2.Close()
		assert.NoError(t, err)
		assert.NotNil(t, producer2)

		id, err := producer2.Send(context.Background(), &ProducerMessage{
			Payload: make([]byte, 1024),
		})
		assert.Nil(t, err)
		assert.NotNil(t, id)
		wg.Done()
	}()
	// Because set the request timeout is 200ms before.
	// Here waite 300ms to cover wait for exclusive producer never timeout
	time.Sleep(300 * time.Millisecond)
	producer1.Close()
	wg.Wait()
}

func TestMemLimitRejectProducerMessages(t *testing.T) {

	c, err := NewClient(ClientOptions{
		URL:              serviceURL,
		MemoryLimitBytes: 100 * 1024,
	})
	assert.NoError(t, err)
	defer c.Close()

	topicName := newTopicName()
	producer1, _ := c.CreateProducer(ProducerOptions{
		Topic:                   topicName,
		DisableBlockIfQueueFull: true,
		DisableBatching:         false,
		BatchingMaxPublishDelay: 100 * time.Second,
		SendTimeout:             2 * time.Second,
	})

	producer2, _ := c.CreateProducer(ProducerOptions{
		Topic:                   topicName,
		DisableBlockIfQueueFull: true,
		DisableBatching:         false,
		BatchingMaxPublishDelay: 100 * time.Second,
		SendTimeout:             2 * time.Second,
	})

	n := 101
	for i := 0; i < n/2; i++ {
		producer1.SendAsync(context.Background(), &ProducerMessage{
			Payload: make([]byte, 1024),
		}, func(id MessageID, message *ProducerMessage, e error) {})

		producer2.SendAsync(context.Background(), &ProducerMessage{
			Payload: make([]byte, 1024),
		}, func(id MessageID, message *ProducerMessage, e error) {})
	}
	// Last message in order to reach the limit
	producer1.SendAsync(context.Background(), &ProducerMessage{
		Payload: make([]byte, 1024),
	}, func(id MessageID, message *ProducerMessage, e error) {})
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(n*1024), c.(*client).memLimit.CurrentUsage())

	_, err = producer1.Send(context.Background(), &ProducerMessage{
		Payload: make([]byte, 1024),
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, getResultStr(ClientMemoryBufferIsFull))

	_, err = producer2.Send(context.Background(), &ProducerMessage{
		Payload: make([]byte, 1024),
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, getResultStr(ClientMemoryBufferIsFull))

	// flush pending msg
	err = producer1.Flush()
	assert.NoError(t, err)
	err = producer2.Flush()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), c.(*client).memLimit.CurrentUsage())

	_, err = producer1.Send(context.Background(), &ProducerMessage{
		Payload: make([]byte, 1024),
	})
	assert.NoError(t, err)
	_, err = producer2.Send(context.Background(), &ProducerMessage{
		Payload: make([]byte, 1024),
	})
	assert.NoError(t, err)
}

func TestMemLimitRejectProducerMessagesWithSchema(t *testing.T) {

	c, err := NewClient(ClientOptions{
		URL:              serviceURL,
		MemoryLimitBytes: 100 * 6,
	})
	assert.NoError(t, err)
	defer c.Close()

	schema := NewAvroSchema(`{"fields":
	[
		{"name":"id","type":"int"},{"default":null,"name":"name","type":["null","string"]}
	],
	"name":"MyAvro","namespace":"schemaNotFoundTestCase","type":"record"}`, nil)

	topicName := newTopicName()
	producer1, _ := c.CreateProducer(ProducerOptions{
		Topic:                   topicName,
		DisableBlockIfQueueFull: true,
		DisableBatching:         false,
		BatchingMaxPublishDelay: 100 * time.Second,
		SendTimeout:             2 * time.Second,
	})

	producer2, _ := c.CreateProducer(ProducerOptions{
		Topic:                   topicName,
		DisableBlockIfQueueFull: true,
		DisableBatching:         false,
		BatchingMaxPublishDelay: 100 * time.Second,
		SendTimeout:             2 * time.Second,
	})

	// the size of encoded value is 6 bytes
	value := map[string]interface{}{
		"id": 0,
		"name": map[string]interface{}{
			"string": "abc",
		},
	}

	n := 101
	for i := 0; i < n/2; i++ {
		producer1.SendAsync(context.Background(), &ProducerMessage{
			Value:  value,
			Schema: schema,
		}, func(id MessageID, message *ProducerMessage, e error) {})

		producer2.SendAsync(context.Background(), &ProducerMessage{
			Value:  value,
			Schema: schema,
		}, func(id MessageID, message *ProducerMessage, e error) {})
	}
	// Last message in order to reach the limit
	producer1.SendAsync(context.Background(), &ProducerMessage{
		Value:  value,
		Schema: schema,
	}, func(id MessageID, message *ProducerMessage, e error) {})
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(n*6), c.(*client).memLimit.CurrentUsage())

	_, err = producer1.Send(context.Background(), &ProducerMessage{
		Value:  value,
		Schema: schema,
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, getResultStr(ClientMemoryBufferIsFull))

	_, err = producer2.Send(context.Background(), &ProducerMessage{
		Value:  value,
		Schema: schema,
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, getResultStr(ClientMemoryBufferIsFull))

	// flush pending msg
	err = producer1.Flush()
	assert.NoError(t, err)
	err = producer2.Flush()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), c.(*client).memLimit.CurrentUsage())

	_, err = producer1.Send(context.Background(), &ProducerMessage{
		Value:  value,
		Schema: schema,
	})
	assert.NoError(t, err)
	_, err = producer2.Send(context.Background(), &ProducerMessage{
		Value:  value,
		Schema: schema,
	})
	assert.NoError(t, err)
}

func TestMemLimitRejectProducerMessagesWithChunking(t *testing.T) {
	c, err := NewClient(ClientOptions{
		URL:              serviceURL,
		MemoryLimitBytes: 5 * 1024,
	})
	assert.NoError(t, err)
	defer c.Close()

	topicName := newTopicName()
	producer1, _ := c.CreateProducer(ProducerOptions{
		Topic:                   topicName,
		DisableBlockIfQueueFull: true,
		DisableBatching:         true,
		EnableChunking:          true,
		SendTimeout:             2 * time.Second,
	})

	producer2, _ := c.CreateProducer(ProducerOptions{
		Topic:                   topicName,
		DisableBlockIfQueueFull: true,
		DisableBatching:         false,
		BatchingMaxPublishDelay: 100 * time.Millisecond,
		SendTimeout:             2 * time.Second,
	})

	producer2.SendAsync(context.Background(), &ProducerMessage{
		Payload: make([]byte, 5*1024+1),
	}, func(id MessageID, message *ProducerMessage, e error) {
		if e != nil {
			t.Fatal(e)
		}
	})

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(5*1024+1), c.(*client).memLimit.CurrentUsage())

	_, err = producer1.Send(context.Background(), &ProducerMessage{
		Payload: make([]byte, 1),
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, getResultStr(ClientMemoryBufferIsFull))

	// wait all the mem have been released
	retryAssert(t, 10, 200, func() {}, func(t assert.TestingT) bool {
		return assert.Equal(t, 0, int(c.(*client).memLimit.CurrentUsage()))
	})

	producer3, _ := c.CreateProducer(ProducerOptions{
		Topic:                   topicName,
		DisableBlockIfQueueFull: true,
		DisableBatching:         true,
		EnableChunking:          true,
		MaxPendingMessages:      1,
		ChunkMaxMessageSize:     1024,
		SendTimeout:             2 * time.Second,
	})

	// producer3 cannot reserve 2*1024 bytes because it reaches MaxPendingMessages in chunking
	_, _ = producer3.Send(context.Background(), &ProducerMessage{
		Payload: make([]byte, 2*1024),
	})
	assert.Zero(t, c.(*client).memLimit.CurrentUsage())
}

func TestMemLimitContextCancel(t *testing.T) {

	c, err := NewClient(ClientOptions{
		URL:              serviceURL,
		MemoryLimitBytes: 100 * 1024,
	})
	assert.NoError(t, err)
	defer c.Close()

	topicName := newTopicName()
	producer, _ := c.CreateProducer(ProducerOptions{
		Topic:                   topicName,
		DisableBlockIfQueueFull: false,
		DisableBatching:         false,
		BatchingMaxPublishDelay: 100 * time.Second,
		SendTimeout:             2 * time.Second,
	})

	n := 101
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < n; i++ {
		producer.SendAsync(ctx, &ProducerMessage{
			Payload: make([]byte, 1024),
		}, func(id MessageID, message *ProducerMessage, e error) {})
	}
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(n*1024), c.(*client).memLimit.CurrentUsage())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		producer.SendAsync(ctx, &ProducerMessage{
			Payload: make([]byte, 1024),
		}, func(id MessageID, message *ProducerMessage, e error) {
			assert.Error(t, e)
			assert.ErrorContains(t, e, getResultStr(TimeoutError))
			wg.Done()
		})
	}()

	// cancel pending msg
	cancel()
	wg.Wait()

	err = producer.Flush()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), c.(*client).memLimit.CurrentUsage())

	_, err = producer.Send(context.Background(), &ProducerMessage{
		Payload: make([]byte, 1024),
	})
	assert.NoError(t, err)
}

func TestBatchSendMessagesWithMetadata(t *testing.T) {
	testSendMessagesWithMetadata(t, false)
}

func TestNoBatchSendMessagesWithMetadata(t *testing.T) {
	testSendMessagesWithMetadata(t, true)
}

func testSendMessagesWithMetadata(t *testing.T, disableBatch bool) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: disableBatch,
	})
	assert.Nil(t, err)

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
	})
	assert.Nil(t, err)

	msg := &ProducerMessage{EventTime: time.Now().Local(),
		Key:         "my-key",
		OrderingKey: "my-ordering-key",
		Properties:  map[string]string{"k1": "v1", "k2": "v2"},
		Payload:     []byte("msg")}

	_, err = producer.Send(context.Background(), msg)
	assert.Nil(t, err)

	recvMsg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)

	assert.Equal(t, internal.TimestampMillis(msg.EventTime), internal.TimestampMillis(recvMsg.EventTime()))
	assert.Equal(t, msg.Key, recvMsg.Key())
	assert.Equal(t, msg.OrderingKey, recvMsg.OrderingKey())
	assert.Equal(t, msg.Properties, recvMsg.Properties())
}

func TestProducerSendWithContext(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topicName := newTopicName()
	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	// Make ctx be canceled to invalidate the context immediately
	cancel()
	_, err = producer.Send(ctx, &ProducerMessage{
		Payload: make([]byte, 1024*1024),
	})
	//  producer.Send should fail and return err context.Canceled
	assert.True(t, errors.Is(err, context.Canceled))
}

func TestFailPendingMessageWithClose(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()
	testProducer, err := client.CreateProducer(ProducerOptions{
		Topic:                   newTopicName(),
		DisableBlockIfQueueFull: false,
		BatchingMaxPublishDelay: 100000,
		BatchingMaxMessages:     1000,
	})

	assert.NoError(t, err)
	assert.NotNil(t, testProducer)
	for i := 0; i < 3; i++ {
		testProducer.SendAsync(context.Background(), &ProducerMessage{
			Payload: make([]byte, 1024),
		}, func(id MessageID, message *ProducerMessage, e error) {
			if e != nil {
				assert.True(t, errors.Is(e, ErrProducerClosed))
			}
		})
	}
	partitionProducerImp := testProducer.(*producer).producers[0].(*partitionProducer)
	partitionProducerImp.pendingQueue.Put(&pendingItem{})
	testProducer.Close()
	assert.Equal(t, 0, partitionProducerImp.pendingQueue.Size())
}

func TestProducerSendDuplicatedMessages(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()
	testProducer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, testProducer)
	_, err = testProducer.Send(context.Background(), &ProducerMessage{
		Payload: make([]byte, 1024),
	})
	assert.NoError(t, err)
	for i := 0; i < 3; i++ {
		var seqId int64 = 0
		msgId, err := testProducer.Send(context.Background(), &ProducerMessage{
			Payload:    make([]byte, 1024),
			SequenceID: &seqId,
		})
		assert.NoError(t, err)
		assert.NotNil(t, msgId)
		assert.Equal(t, int64(-1), msgId.LedgerID())
		assert.Equal(t, int64(-1), msgId.EntryID())
	}
	testProducer.Close()
}

type pendingQueueWrapper struct {
	pendingQueue   internal.BlockingQueue
	writtenBuffers *[]internal.Buffer
}

func (pqw *pendingQueueWrapper) Put(item interface{}) {
	pi := item.(*pendingItem)
	writerIdx := pi.buffer.WriterIndex()
	buf := internal.NewBuffer(int(writerIdx))
	buf.Write(pi.buffer.Get(0, writerIdx))
	*pqw.writtenBuffers = append(*pqw.writtenBuffers, buf)
	pqw.pendingQueue.Put(item)
}

func (pqw *pendingQueueWrapper) Take() interface{} {
	return pqw.pendingQueue.Take()
}

func (pqw *pendingQueueWrapper) Poll() interface{} {
	return pqw.pendingQueue.Poll()
}

func (pqw *pendingQueueWrapper) CompareAndPoll(compare func(interface{}) bool) interface{} {
	return pqw.pendingQueue.CompareAndPoll(compare)
}

func (pqw *pendingQueueWrapper) Peek() interface{} {
	return pqw.pendingQueue.Peek()
}

func (pqw *pendingQueueWrapper) PeekLast() interface{} {
	return pqw.pendingQueue.PeekLast()
}

func (pqw *pendingQueueWrapper) Size() int {
	return pqw.pendingQueue.Size()
}

func (pqw *pendingQueueWrapper) ReadableSlice() []interface{} {
	return pqw.pendingQueue.ReadableSlice()
}

func TestDisableReplication(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	testProducer, err := client.CreateProducer(ProducerOptions{
		Topic:           newTopicName(),
		DisableBatching: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, testProducer)
	defer testProducer.Close()

	writtenBuffers := make([]internal.Buffer, 0)
	pqw := &pendingQueueWrapper{
		pendingQueue:   internal.NewBlockingQueue(1000),
		writtenBuffers: &writtenBuffers,
	}

	partitionProducerImp := testProducer.(*producer).producers[0].(*partitionProducer)
	partitionProducerImp.pendingQueue = pqw

	ID, err := testProducer.Send(context.Background(), &ProducerMessage{
		Payload:            []byte("disable-replication"),
		DisableReplication: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, ID)

	assert.Equal(t, 1, len(writtenBuffers))
	buf := writtenBuffers[0]

	buf.Skip(4)                        // TOTAL_SIZE
	cmdSize := buf.ReadUint32()        // CMD_SIZE
	buf.Skip(cmdSize)                  // CMD
	buf.Skip(2)                        // MAGIC_NUMBER
	buf.Skip(4)                        // CHECKSUM
	metadataSize := buf.ReadUint32()   // METADATA_SIZE
	metadata := buf.Read(metadataSize) // METADATA

	var msgMetadata pb.MessageMetadata
	err = proto.Unmarshal(metadata, &msgMetadata)
	assert.NoError(t, err)
	assert.Equal(t, []string{"__local__"}, msgMetadata.GetReplicateTo())
}
