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
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"

	"google.golang.org/protobuf/proto"

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
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

	// create producer without ChunkMaxMessageSize
	producer1, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
		EnableChunking:  true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer1)
	defer producer1.Close()

	// create producer with ChunkMaxMessageSize
	producer2, err := client.CreateProducer(ProducerOptions{
		Topic:               topic,
		DisableBatching:     true,
		EnableChunking:      true,
		ChunkMaxMessageSize: 5,
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

	// test send chunk with ChunkMaxMessageSize limit
	for i := 0; i < 5; i++ {
		msg := createTestMessagePayload(50)
		expectMsgs = append(expectMsgs, msg)
		ID, err := producer2.Send(context.Background(), &ProducerMessage{
			Payload: msg,
		})
		assert.NoError(t, err)
		assert.NotNil(t, ID)
	}

	// test receive chunk with ChunkMaxMessageSize limit
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

func TestMaxPendingChunkMessages(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:               topic,
		DisableBatching:     true,
		EnableChunking:      true,
		ChunkMaxMessageSize: 10,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	c, err := client.Subscribe(ConsumerOptions{
		Topic:                    topic,
		Type:                     Exclusive,
		SubscriptionName:         "chunk-subscriber",
		MaxPendingChunkedMessage: 1,
	})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	defer c.Close()
	pc := c.(*consumer).consumers[0]

	sendSingleChunk(producer, "0", 0, 2)
	// MaxPendingChunkedMessage is 1, the chunked message with uuid 0 will be discarded
	sendSingleChunk(producer, "1", 0, 2)

	// chunkedMsgCtx with uuid 0 should be discarded
	retryAssert(t, 3, 200, func() {}, func(t assert.TestingT) bool {
		pc.chunkedMsgCtxMap.mu.Lock()
		defer pc.chunkedMsgCtxMap.mu.Unlock()
		return assert.Equal(t, 1, len(pc.chunkedMsgCtxMap.chunkedMsgCtxs))
	})

	sendSingleChunk(producer, "1", 1, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	msg, err := c.Receive(ctx)
	cancel()

	assert.NoError(t, err)
	assert.Equal(t, "chunk-1-0|chunk-1-1|", string(msg.Payload()))

	// Ensure that the chunked message of uuid 0 is discarded.
	sendSingleChunk(producer, "0", 1, 2)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	msg, err = c.Receive(ctx)
	cancel()
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

func TestExpireIncompleteChunks(t *testing.T) {
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
		ChunkMaxMessageSize:     50,
		DisableBlockIfQueueFull: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	// Reduce publish rate to prevent the producer sending messages too fast
	url := adminURL + "/" + "admin/v2/persistent/public/default/" + topic + "/publishRate"
	makeHTTPCall(t, http.MethodPost, url, "{\"publishThrottlingRateInMsg\": 1,\"publishThrottlingRateInByte\": 1000}")

	// Need to wait some time to let the rate limiter take effect
	time.Sleep(2 * time.Second)

	ID, err := producer.Send(context.Background(), &ProducerMessage{
		Payload: createTestMessagePayload(1000),
	})
	assert.Error(t, err)
	assert.Nil(t, ID)
}

func TestSeekChunkMessages(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	totalMessages := 5

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:               topic,
		EnableChunking:      true,
		DisableBatching:     true,
		ChunkMaxMessageSize: 50,
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
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:               topic,
		EnableChunking:      true,
		DisableBatching:     true,
		ChunkMaxMessageSize: 50,
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
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	// the default message metadata size for string schema
	// The proto messageMetaData size as following. (all with tag) (maxMessageSize = 1024 * 1024)
	// | producerName | sequenceID | publishTime | uncompressedSize |
	// | ------------ | ---------- | ----------- | ---------------- |
	// | 6            | 2          | 7           | 4                |
	payloadChunkSize := _brokerMaxMessageSize - 19

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
			_, ok := msgID.(*messageID)
			assert.Equal(t, true, ok)
		} else {
			_, ok := msgID.(*chunkMessageID)
			assert.Equal(t, true, ok)
		}
	}
}

func TestChunkMultiTopicConsumerReceive(t *testing.T) {
	topic1 := newTopicName()
	topic2 := newTopicName()

	client, err := NewClient(ClientOptions{
		URL: lookupURL,
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

	maxSize := 50

	// produce messages
	for i, topic := range topics {
		p, err := client.CreateProducer(ProducerOptions{
			Topic:               topic,
			DisableBatching:     true,
			EnableChunking:      true,
			ChunkMaxMessageSize: uint(maxSize),
		})
		if err != nil {
			t.Fatal(err)
		}
		err = genMessages(p, 10, func(idx int) string {
			return fmt.Sprintf("topic-%d-hello-%d-%s", i+1, idx, string(createTestMessagePayload(100)))
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
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
		case <-ctx.Done():
			t.Error(ctx.Err())
		}
		cancel()
	}
	assert.Equal(t, receivedTopic1, receivedTopic2)
}

func TestChunkBlockIfQueueFull(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	if err != nil {
		t.Fatal(err)
	}

	topic := newTopicName()

	producer, err := client.CreateProducer(ProducerOptions{
		Name:                "test",
		Topic:               topic,
		EnableChunking:      true,
		DisableBatching:     true,
		MaxPendingMessages:  1,
		ChunkMaxMessageSize: 10,
	})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Large messages will be split into 11 chunks, exceeding the length of pending queue
	_, err = producer.Send(ctx, &ProducerMessage{
		Payload: createTestMessagePayload(100),
	})
	assert.Error(t, err)
}

func createTestMessagePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(rand.Intn(100))
	}
	return payload
}

//nolint:all
func sendSingleChunk(p Producer, uuid string, chunkID int, totalChunks int) {
	msg := &ProducerMessage{
		Payload: []byte(fmt.Sprintf("chunk-%s-%d|", uuid, chunkID)),
	}
	wholePayload := msg.Payload
	producerImpl := p.(*producer).getProducer(0).(*partitionProducer)
	mm := producerImpl.genMetadata(msg, len(wholePayload), time.Now())
	mm.Uuid = proto.String(uuid)
	mm.NumChunksFromMsg = proto.Int32(int32(totalChunks))
	mm.TotalChunkMsgSize = proto.Int32(int32(len(wholePayload)))
	mm.ChunkId = proto.Int32(int32(chunkID))
	producerImpl.updateMetadataSeqID(mm, msg)
	producerImpl.internalSingleSend(
		mm,
		msg.Payload,
		&sendRequest{
			callback: func(id MessageID, producerMessage *ProducerMessage, err error) {
			},
			callbackOnce:        &sync.Once{},
			ctx:                 context.Background(),
			msg:                 msg,
			producer:            producerImpl,
			flushImmediately:    true,
			totalChunks:         totalChunks,
			chunkID:             chunkID,
			uuid:                uuid,
			chunkRecorder:       newChunkRecorder(),
			uncompressedPayload: wholePayload,
			uncompressedSize:    int64(len(wholePayload)),
			compressedPayload:   wholePayload,
			compressedSize:      len(wholePayload),
			payloadChunkSize:    internal.MaxMessageSize - proto.Size(mm),
			mm:                  mm,
			deliverAt:           time.Now(),
			maxMessageSize:      internal.MaxMessageSize,
		},
		uint32(internal.MaxMessageSize),
	)
}
