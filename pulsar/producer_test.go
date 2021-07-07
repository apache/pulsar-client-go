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
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/stretchr/testify/assert"

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
	log "github.com/sirupsen/logrus"
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

		msgID := msg.ID().(trackingMessageID)
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
	err := httpPut("admin/v2/persistent/public/default/my-partitioned-topic/partitions", 5)
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
		atomic.StoreInt64(&delayMsgID, id.(messageID).entryID)
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
		atomic.StoreInt64(&noDelayMsgID, id.(messageID).entryID)
	})
	for i := 0; i < 2; i++ {
		msg, err := consumer.Receive(context.Background())
		assert.Nil(t, err, "unexpected error occurred when recving message from topic")

		switch msg.ID().(trackingMessageID).entryID {
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

func TestMaxMessageSize(t *testing.T) {
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
	serverMaxMessageSize := 1024 * 1024
	for bias := -1; bias <= 1; bias++ {
		payload := make([]byte, serverMaxMessageSize+bias)
		ID, err := producer.Send(context.Background(), &ProducerMessage{
			Payload: payload,
		})
		if bias <= 0 {
			assert.NoError(t, err)
			assert.NotNil(t, ID)
		} else {
			assert.Equal(t, errMessageTooLarge, err)
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
		Topic:            topic,
		DisableBatching:  false,
		MessageKeyCrypto: msgCrypto,
		KeyReader:        crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem", "crypto/testdata/pri_key_rsa.pem"),
		Schema:           NewStringSchema(nil),
		EncryptionKeys:   []string{"my-app.key"},
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

func TestProducuerCreationFailOnInvalidKey(t *testing.T) {
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
		Topic:            topic,
		DisableBatching:  false,
		MessageKeyCrypto: msgCrypto,
		KeyReader:        crypto.NewFileKeyReader("crypto/testdata/invalid_pub_key_rsa.pem", "crypto/testdata/pri_key_rsa.pem"),
		Schema:           NewStringSchema(nil),
		EncryptionKeys:   []string{"my-app.key"},
	})

	assert.NotNil(t, err)
	assert.Nil(t, producer)
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
