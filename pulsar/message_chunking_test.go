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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var _brokerMaxMessageSize = 1024 * 1024

func TestInvalidChunkingConfig(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           newTopicName(),
		DisableBatching: false,
		EnableChunking:  true,
	})

	assert.Error(t, err, "producer creation should have fail")
	assert.Nil(t, producer)
}

func TestLargeMessage(t *testing.T) {
	rand.Seed(time.Now().Unix())

	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

	// create producer without MaxChunkSize
	producer1, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
		EnableChunking:  true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer1)
	defer producer1.Close()

	// create producer with MaxChunkSize
	producer2, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
		EnableChunking:  true,
		MaxChunkSize:    5,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer2)
	defer producer2.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		Type:             Exclusive,
		SubscriptionName: "chunk-subscriber",
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	expectMsgs := make([][]byte, 0, 10)

	// test send chunk with serverMaxMessageSize limit
	for i := 0; i < 5; i++ {
		msg := createTestMessagePayload(_brokerMaxMessageSize + 1)
		expectMsgs = append(expectMsgs, msg)
		ID, err := producer1.Send(context.Background(), &ProducerMessage{
			Payload: msg,
		})
		assert.NoError(t, err)
		assert.NotNil(t, ID)
	}

	// test receive chunk with serverMaxMessageSize limit
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		msg, err := consumer.Receive(ctx)
		cancel()
		assert.NoError(t, err)

		expectMsg := expectMsgs[i]

		assert.Equal(t, expectMsg, msg.Payload())
		// ack message
		err = consumer.Ack(msg)
		assert.NoError(t, err)
	}

	// test send chunk with MaxChunkSize limit
	for i := 0; i < 5; i++ {
		msg := createTestMessagePayload(50)
		expectMsgs = append(expectMsgs, msg)
		ID, err := producer2.Send(context.Background(), &ProducerMessage{
			Payload: msg,
		})
		assert.NoError(t, err)
		assert.NotNil(t, ID)
	}

	// test receive chunk with MaxChunkSize limit
	for i := 5; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		assert.NoError(t, err)

		expectMsg := expectMsgs[i]

		assert.Equal(t, expectMsg, msg.Payload())
		// ack message
		err = consumer.Ack(msg)
		assert.NoError(t, err)
	}
}

func TestPublishChunkWithFailure(t *testing.T) {
	rand.Seed(time.Now().Unix())

	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

	// create producer without MaxChunkSize
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	ID, err := producer.Send(context.Background(), &ProducerMessage{
		Payload: createTestMessagePayload(_brokerMaxMessageSize + 1),
	})
	assert.Error(t, err)
	assert.Nil(t, ID)
}

func TestMaxPendingChunkMessages(t *testing.T) {
	rand.Seed(time.Now().Unix())

	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

	totalProducers := 5
	producers := make([]Producer, 0, 20)
	defer func() {
		for _, p := range producers {
			p.Close()
		}
	}()

	clients := make([]Client, 0, 20)
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	for j := 0; j < totalProducers; j++ {
		pc, err := NewClient(ClientOptions{
			URL: lookupURL,
		})
		assert.Nil(t, err)
		clients = append(clients, pc)
		producer, err := pc.CreateProducer(ProducerOptions{
			Topic:           topic,
			DisableBatching: true,
			EnableChunking:  true,
			MaxChunkSize:    10,
		})
		assert.NoError(t, err)
		assert.NotNil(t, producer)
		producers = append(producers, producer)
	}

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                    topic,
		Type:                     Exclusive,
		SubscriptionName:         "chunk-subscriber",
		MaxPendingChunkedMessage: 1,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	totalMsgs := 40
	wg := sync.WaitGroup{}
	wg.Add(totalMsgs * totalProducers)
	for i := 0; i < totalMsgs; i++ {
		for j := 0; j < totalProducers; j++ {
			p := producers[j]
			go func() {
				ID, err := p.Send(context.Background(), &ProducerMessage{
					Payload: createTestMessagePayload(50),
				})
				assert.NoError(t, err)
				assert.NotNil(t, ID)
				wg.Done()
			}()
		}
	}
	wg.Wait()

	received := 0
	for i := 0; i < totalMsgs*totalProducers; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		msg, err := consumer.Receive(ctx)
		cancel()
		if msg == nil || (err != nil && errors.Is(err, context.DeadlineExceeded)) {
			break
		}

		received++

		err = consumer.Ack(msg)
		assert.NoError(t, err)
	}

	assert.NotEqual(t, totalMsgs*totalProducers, received)
}

func TestExpireIncompleteChunks(t *testing.T) {
	rand.Seed(time.Now().Unix())
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

	c, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		Type:                        Exclusive,
		SubscriptionName:            "chunk-subscriber",
		ExpireTimeOfIncompleteChunk: time.Millisecond * 300,
	})
	assert.NoError(t, err)
	defer c.Close()

	uuid := "test-uuid"
	chunkCtxMap := c.(*consumer).consumers[0].chunkedMsgCtxMap
	chunkCtxMap.addIfAbsent(uuid, 2, 100)
	ctx := chunkCtxMap.get(uuid)
	assert.NotNil(t, ctx)

	time.Sleep(400 * time.Millisecond)

	ctx = chunkCtxMap.get(uuid)
	assert.Nil(t, ctx)
}

func TestChunksEnqueueFailed(t *testing.T) {
	rand.Seed(time.Now().Unix())

	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   topic,
		EnableChunking:          true,
		DisableBatching:         true,
		MaxPendingMessages:      10,
		MaxChunkSize:            50,
		DisableBlockIfQueueFull: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	ID, err := producer.Send(context.Background(), &ProducerMessage{
		Payload: createTestMessagePayload(1000),
	})
	assert.Error(t, err)
	assert.Nil(t, ID)
}

func TestSeekChunkMessages(t *testing.T) {
	rand.Seed(time.Now().Unix())

	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	totalMessages := 5

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		EnableChunking:  true,
		DisableBatching: true,
		MaxChunkSize:    50,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		Type:             Exclusive,
		SubscriptionName: "default-seek",
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	msgIDs := make([]MessageID, 0)
	for i := 0; i < totalMessages; i++ {
		ID, err := producer.Send(context.Background(), &ProducerMessage{
			Payload: createTestMessagePayload(100),
		})
		assert.NoError(t, err)
		msgIDs = append(msgIDs, ID)
	}

	for i := 0; i < totalMessages; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		msg, err := consumer.Receive(ctx)
		cancel()
		assert.NoError(t, err)
		assert.NotNil(t, msg)
		assert.Equal(t, msgIDs[i].Serialize(), msg.ID().Serialize())
	}

	err = consumer.Seek(msgIDs[1])
	assert.NoError(t, err)

	for i := 1; i < totalMessages; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		msg, err := consumer.Receive(ctx)
		cancel()
		assert.NoError(t, err)
		assert.NotNil(t, msg)
		assert.Equal(t, msgIDs[i].Serialize(), msg.ID().Serialize())
	}

	// todo: add reader seek test when support reader read chunk message
}

func TestChunkAckAndNAck(t *testing.T) {
	rand.Seed(time.Now().Unix())

	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		EnableChunking:  true,
		DisableBatching: true,
		MaxChunkSize:    50,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:               topic,
		Type:                Exclusive,
		SubscriptionName:    "default-seek",
		NackRedeliveryDelay: time.Second,
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	content := createTestMessagePayload(100)

	_, err = producer.Send(context.Background(), &ProducerMessage{
		Payload: content,
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	msg, err := consumer.Receive(ctx)
	cancel()
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	assert.Equal(t, msg.Payload(), content)

	consumer.Nack(msg)
	time.Sleep(time.Second * 2)

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	msg, err = consumer.Receive(ctx)
	cancel()
	assert.NoError(t, err)
	assert.NotNil(t, msg)
	assert.Equal(t, msg.Payload(), content)
}

func TestChunkSize(t *testing.T) {
	rand.Seed(time.Now().Unix())

	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	//the default message metadata size for string schema
	payloadChunkSize := _brokerMaxMessageSize - 18

	topic := newTopicName()

	producer, err := client.CreateProducer(ProducerOptions{
		Name:            "test",
		Topic:           topic,
		EnableChunking:  true,
		DisableBatching: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	for size := payloadChunkSize; size <= _brokerMaxMessageSize; size++ {
		msgID, err := producer.Send(context.Background(), &ProducerMessage{
			Payload: createTestMessagePayload(size),
		})
		assert.NoError(t, err)
		if size <= payloadChunkSize {
			_, ok := msgID.(messageID)
			assert.Equal(t, true, ok)
		} else {
			_, ok := msgID.(chunkMessageID)
			assert.Equal(t, true, ok)
		}
	}
}

func createTestMessagePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(rand.Intn(100))
	}
	return payload
}
