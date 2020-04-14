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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/pulsar-client-go/pulsar/internal"
)

func TestReaderConfigErrors(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	consumer, err := client.CreateReader(ReaderOptions{
		Topic: "my-topic",
	})
	assert.Nil(t, consumer)
	assert.NotNil(t, err)

	consumer, err = client.CreateReader(ReaderOptions{
		StartMessageID: EarliestMessageID(),
	})
	assert.Nil(t, consumer)
	assert.NotNil(t, err)
}

func TestReader(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create reader
	reader, err := client.CreateReader(ReaderOptions{
		Topic:          topic,
		StartMessageID: EarliestMessageID(),
	})
	assert.Nil(t, err)
	defer reader.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		_, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.NoError(t, err)
	}

	// receive 10 messages
	for i := 0; i < 10; i++ {
		msg, err := reader.Next(context.Background())
		assert.NoError(t, err)

		expectMsg := fmt.Sprintf("hello-%d", i)
		assert.Equal(t, []byte(expectMsg), msg.Payload())
	}
}

func TestReaderConnectError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://invalid-hostname:6650",
	})

	assert.Nil(t, err)

	defer client.Close()

	reader, err := client.CreateReader(ReaderOptions{
		Topic:          "my-topic",
		StartMessageID: LatestMessageID(),
	})

	// Expect error in creating consumer
	assert.Nil(t, reader)
	assert.NotNil(t, err)

	assert.Equal(t, err.Error(), "connection error")
}

func TestReaderOnSpecificMessage(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	msgIDs := [10]MessageID{}
	for i := 0; i < 10; i++ {
		msgID, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.NoError(t, err)
		assert.NotNil(t, msgID)
		msgIDs[i] = msgID
	}

	// create reader on 5th message (not included)
	reader, err := client.CreateReader(ReaderOptions{
		Topic:          topic,
		StartMessageID: msgIDs[4],
	})

	assert.Nil(t, err)
	defer reader.Close()

	// receive the remaining 5 messages
	for i := 5; i < 10; i++ {
		msg, err := reader.Next(context.Background())
		assert.NoError(t, err)

		expectMsg := fmt.Sprintf("hello-%d", i)
		assert.Equal(t, []byte(expectMsg), msg.Payload())
	}

	// create reader on 5th message (included)
	readerInclusive, err := client.CreateReader(ReaderOptions{
		Topic:                   topic,
		StartMessageID:          msgIDs[4],
		StartMessageIDInclusive: true,
	})

	assert.Nil(t, err)
	defer readerInclusive.Close()

	// receive the remaining 6 messages
	for i := 4; i < 10; i++ {
		msg, err := readerInclusive.Next(context.Background())
		assert.NoError(t, err)

		expectMsg := fmt.Sprintf("hello-%d", i)
		assert.Equal(t, []byte(expectMsg), msg.Payload())
	}
}

func TestReaderOnSpecificMessageWithBatching(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   topic,
		DisableBatching:         false,
		BatchingMaxMessages:     3,
		BatchingMaxPublishDelay: 1 * time.Second,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	msgIDs := [10]MessageID{}
	for i := 0; i < 10; i++ {
		idx := i

		producer.SendAsync(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}, func(id MessageID, producerMessage *ProducerMessage, err error) {
			assert.NoError(t, err)
			assert.NotNil(t, id)
			msgIDs[idx] = id
		})
	}

	err = producer.Flush()
	assert.NoError(t, err)

	// create reader on 5th message (not included)
	reader, err := client.CreateReader(ReaderOptions{
		Topic:          topic,
		StartMessageID: msgIDs[4],
	})

	assert.Nil(t, err)
	defer reader.Close()

	// receive the remaining 5 messages
	for i := 5; i < 10; i++ {
		msg, err := reader.Next(context.Background())
		assert.NoError(t, err)

		expectMsg := fmt.Sprintf("hello-%d", i)
		assert.Equal(t, []byte(expectMsg), msg.Payload())
	}

	// create reader on 5th message (included)
	readerInclusive, err := client.CreateReader(ReaderOptions{
		Topic:                   topic,
		StartMessageID:          msgIDs[4],
		StartMessageIDInclusive: true,
	})

	assert.Nil(t, err)
	defer readerInclusive.Close()

	// receive the remaining 6 messages
	for i := 4; i < 10; i++ {
		msg, err := readerInclusive.Next(context.Background())
		assert.NoError(t, err)

		expectMsg := fmt.Sprintf("hello-%d", i)
		assert.Equal(t, []byte(expectMsg), msg.Payload())
	}
}

func TestReaderOnLatestWithBatching(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   topic,
		DisableBatching:         false,
		BatchingMaxMessages:     4,
		BatchingMaxPublishDelay: 1 * time.Second,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	msgIDs := [10]MessageID{}
	for i := 0; i < 10; i++ {
		idx := i

		producer.SendAsync(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}, func(id MessageID, producerMessage *ProducerMessage, err error) {
			assert.NoError(t, err)
			assert.NotNil(t, id)
			msgIDs[idx] = id
		})
	}

	err = producer.Flush()
	assert.NoError(t, err)

	// create reader on 5th message (not included)
	reader, err := client.CreateReader(ReaderOptions{
		Topic:                   topic,
		StartMessageID:          LatestMessageID(),
		StartMessageIDInclusive: false,
	})

	assert.Nil(t, err)
	defer reader.Close()

	// Reader should yield no message since it's at the end of the topic
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	msg, err := reader.Next(ctx)
	assert.Error(t, err)
	assert.Nil(t, msg)
	cancel()
}

func TestReaderHasNext(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		msgID, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.NoError(t, err)
		assert.NotNil(t, msgID)
	}

	// create reader on 5th message (not included)
	reader, err := client.CreateReader(ReaderOptions{
		Topic:          topic,
		StartMessageID: EarliestMessageID(),
	})

	assert.Nil(t, err)
	defer reader.Close()

	i := 0
	for reader.HasNext() {
		msg, err := reader.Next(context.Background())
		assert.NoError(t, err)

		expectMsg := fmt.Sprintf("hello-%d", i)
		assert.Equal(t, []byte(expectMsg), msg.Payload())

		i++
	}

	assert.Equal(t, 10, i)
}

func TestReaderSeek(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	require.Nil(t, err)
	defer client.Close()

	topicName := newTopicName()
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})
	require.Nil(t, err)
	defer producer.Close()

	reader, err := client.CreateReader(ReaderOptions{
		Topic:          topicName,
		StartMessageID: EarliestMessageID(),
	})
	require.Nil(t, err)
	defer reader.Close()

	const N = 10
	var seekID MessageID
	for i := 0; i < N; i++ {
		id, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		require.Nil(t, err)

		if i == 4 {
			seekID = id
		}
	}
	err = producer.Flush()
	assert.NoError(t, err)

	for i := 0; i < N; i++ {
		msg, err := reader.Next(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("hello-%d", i), string(msg.Payload()))
	}

	err = reader.Seek(seekID)
	require.Nil(t, err)

	msg, err := reader.Next(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, "hello-4", string(msg.Payload()))
}

func TestReaderSeekByTime(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	require.Nil(t, err)
	defer client.Close()

	topicName := newTopicName()
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		DisableBatching: false,
	})
	require.Nil(t, err)
	defer producer.Close()

	reader, err := client.CreateReader(ReaderOptions{
		Topic:          topicName,
		StartMessageID: LatestMessageID(),
	})
	require.Nil(t, err)
	defer reader.Close()

	const N = 10
	resetTimeStr := "100s"
	retentionTimeInSecond, err := internal.ParseRelativeTimeInSeconds(resetTimeStr)
	assert.Nil(t, err)

	for i := 0; i < N; i++ {
		_, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		require.Nil(t, err)
	}

	for i := 0; i < N; i++ {
		msg, err := reader.Next(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("hello-%d", i), string(msg.Payload()))
	}

	currentTimestamp := time.Now()
	err = reader.SeekByTime(currentTimestamp.Add(-retentionTimeInSecond))
	require.Nil(t, err)

	for i := 0; i < N; i++ {
		msg, err := reader.Next(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("hello-%d", i), string(msg.Payload()))
	}
}
