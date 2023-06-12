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

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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

func TestReaderConfigSubscribeName(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	consumer, err := client.CreateReader(ReaderOptions{
		StartMessageID:   EarliestMessageID(),
		Topic:            uuid.New().String(),
		SubscriptionName: uuid.New().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	assert.NotNil(t, consumer)
}

func TestReaderConfigChunk(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	r1, err := client.CreateReader(ReaderOptions{
		Topic:                       "my-topic1",
		StartMessageID:              EarliestMessageID(),
		MaxPendingChunkedMessage:    50,
		ExpireTimeOfIncompleteChunk: 30 * time.Second,
		AutoAckIncompleteChunk:      true,
	})
	assert.Nil(t, err)
	defer r1.Close()

	// verify specified chunk options
	pcOpts := r1.(*reader).pc.options
	assert.Equal(t, 50, pcOpts.maxPendingChunkedMessage)
	assert.Equal(t, 30*time.Second, pcOpts.expireTimeOfIncompleteChunk)
	assert.True(t, pcOpts.autoAckIncompleteChunk)

	r2, err := client.CreateReader(ReaderOptions{
		Topic:          "my-topic2",
		StartMessageID: EarliestMessageID(),
	})
	assert.Nil(t, err)
	defer r2.Close()

	// verify default chunk options
	pcOpts = r2.(*reader).pc.options
	assert.Equal(t, 100, pcOpts.maxPendingChunkedMessage)
	assert.Equal(t, time.Minute, pcOpts.expireTimeOfIncompleteChunk)
	assert.False(t, pcOpts.autoAckIncompleteChunk)
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

func TestReaderHasNextAgainstEmptyTopic(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	// create reader on 5th message (not included)
	reader, err := client.CreateReader(ReaderOptions{
		Topic:          "an-empty-topic",
		StartMessageID: EarliestMessageID(),
	})

	assert.Nil(t, err)
	defer reader.Close()

	assert.Equal(t, reader.HasNext(), false)
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

type myMessageID struct {
	data []byte
}

func (id *myMessageID) Serialize() []byte {
	return id.data
}

func (id *myMessageID) LedgerID() int64 {
	return id.LedgerID()
}

func (id *myMessageID) EntryID() int64 {
	return id.EntryID()
}

func (id *myMessageID) BatchIdx() int32 {
	return id.BatchIdx()
}

func (id *myMessageID) BatchSize() int32 {
	return id.BatchSize()
}

func (id *myMessageID) PartitionIdx() int32 {
	return id.PartitionIdx()
}

func (id *myMessageID) String() string {
	mid, err := DeserializeMessageID(id.data)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%d:%d:%d", mid.LedgerID(), mid.EntryID(), mid.PartitionIdx())
}

func (id *myMessageID) equal(other MessageID) bool {
	return id.LedgerID() == other.LedgerID() &&
		id.EntryID() == other.EntryID() &&
		id.BatchIdx() == other.BatchIdx()
}

func TestReaderOnSpecificMessageWithCustomMessageID(t *testing.T) {
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

	// custom start message ID
	myStartMsgID := &myMessageID{
		data: msgIDs[4].Serialize(),
	}

	// attempt to create reader on 5th message (not included)
	var reader Reader
	assert.NotPanics(t, func() {
		reader, err = client.CreateReader(ReaderOptions{
			Topic:          topic,
			StartMessageID: myStartMsgID,
		})
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
		StartMessageID:          myStartMsgID,
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

func TestReaderSeek(t *testing.T) {
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

	reader, err := client.CreateReader(ReaderOptions{
		Topic:          topicName,
		StartMessageID: EarliestMessageID(),
	})
	assert.Nil(t, err)
	defer reader.Close()

	const N = 10
	var seekID MessageID
	for i := 0; i < N; i++ {
		id, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)

		if i == 4 {
			seekID = id
		}
	}
	err = producer.Flush()
	assert.NoError(t, err)

	for i := 0; i < N; i++ {
		msg, err := reader.Next(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("hello-%d", i), string(msg.Payload()))
	}

	err = reader.Seek(seekID)
	assert.Nil(t, err)

	readerOfSeek, err := client.CreateReader(ReaderOptions{
		Topic:                   topicName,
		StartMessageID:          seekID,
		StartMessageIDInclusive: true,
	})
	assert.Nil(t, err)

	msg, err := readerOfSeek.Next(ctx)
	assert.Nil(t, err)
	assert.Equal(t, "hello-4", string(msg.Payload()))
}

func TestReaderLatestInclusiveHasNext(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create reader on the last message (inclusive)
	reader0, err := client.CreateReader(ReaderOptions{
		Topic:                   topic,
		StartMessageID:          LatestMessageID(),
		StartMessageIDInclusive: true,
	})

	assert.Nil(t, err)
	defer reader0.Close()

	assert.False(t, reader0.HasNext())

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	var lastMsgID MessageID
	for i := 0; i < 10; i++ {
		lastMsgID, err = producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.NoError(t, err)
		assert.NotNil(t, lastMsgID)
	}

	// create reader on the last message (inclusive)
	reader, err := client.CreateReader(ReaderOptions{
		Topic:                   topic,
		StartMessageID:          LatestMessageID(),
		StartMessageIDInclusive: true,
	})

	assert.Nil(t, err)
	defer reader.Close()

	assert.True(t, reader.HasNext())
	msg, err := reader.Next(context.Background())
	assert.NoError(t, err)

	assert.Equal(t, []byte("hello-9"), msg.Payload())
	assert.Equal(t, lastMsgID.Serialize(), msg.ID().Serialize())

	assert.False(t, reader.HasNext())
}

func TestReaderWithMultiHosts(t *testing.T) {
	// Multi hosts included an unreached port and the actual port for verify retry logic
	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6600,localhost:6650",
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

func TestProducerReaderRSAEncryption(t *testing.T) {
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
		Decryption: &MessageDecryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			ConsumerCryptoFailureAction: crypto.ConsumerCryptoFailureActionFail,
		},
	})
	assert.Nil(t, err)
	defer reader.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
		Encryption: &ProducerEncryptionInfo{
			KeyReader: crypto.NewFileKeyReader("crypto/testdata/pub_key_rsa.pem",
				"crypto/testdata/pri_key_rsa.pem"),
			ProducerCryptoFailureAction: crypto.ProducerCryptoFailureActionFail,
			Keys:                        []string{"client-rsa.pem"},
		},
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

func TestReaderWithSchema(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	schema := NewStringSchema(nil)

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: schema,
	})
	assert.Nil(t, err)
	defer producer.Close()

	value := "hello pulsar"
	_, err = producer.Send(context.Background(), &ProducerMessage{
		Value: value,
	})
	assert.Nil(t, err)

	// create reader
	reader, err := client.CreateReader(ReaderOptions{
		Topic:          topic,
		StartMessageID: EarliestMessageID(),
		Schema:         schema,
	})
	assert.Nil(t, err)
	defer reader.Close()

	msg, err := reader.Next(context.Background())
	assert.NoError(t, err)

	var res *string
	err = msg.GetSchemaValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, *res, value)
}

func newTestBackoffPolicy(minBackoff, maxBackoff time.Duration) *testBackoffPolicy {
	return &testBackoffPolicy{
		curBackoff: 0,
		minBackoff: minBackoff,
		maxBackoff: maxBackoff,
	}
}

type testBackoffPolicy struct {
	curBackoff, minBackoff, maxBackoff time.Duration
	retryTime                          int
}

func (b *testBackoffPolicy) Next() time.Duration {
	// Double the delay each time
	b.curBackoff += b.curBackoff
	if b.curBackoff.Nanoseconds() < b.minBackoff.Nanoseconds() {
		b.curBackoff = b.minBackoff
	} else if b.curBackoff.Nanoseconds() > b.maxBackoff.Nanoseconds() {
		b.curBackoff = b.maxBackoff
	}
	b.retryTime++

	return b.curBackoff
}

func (b *testBackoffPolicy) IsExpectedIntervalFrom(startTime time.Time) bool {
	// Approximately equal to expected interval
	if time.Since(startTime) < b.curBackoff-time.Second {
		return false
	}
	if time.Since(startTime) > b.curBackoff+time.Second {
		return false
	}
	return true
}

func TestReaderWithBackoffPolicy(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	backoff := newTestBackoffPolicy(1*time.Second, 4*time.Second)
	_reader, err := client.CreateReader(ReaderOptions{
		Topic:          "my-topic",
		StartMessageID: LatestMessageID(),
		BackoffPolicy:  backoff,
	})
	assert.NotNil(t, _reader)
	assert.Nil(t, err)

	partitionConsumerImp := _reader.(*reader).pc
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
