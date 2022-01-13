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
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	"github.com/stretchr/testify/assert"
)

func newPartitionedTopic(t *testing.T) string {
	topicName := fmt.Sprintf("testReaderPartitions-%v", time.Now().Nanosecond())
	topic := fmt.Sprintf("persistent://public/default/%v", topicName)
	testURL := adminURL + "/" + fmt.Sprintf("admin/v2/persistent/public/default/%v/partitions", topicName)

	makeHTTPCall(t, http.MethodPut, testURL, "5")
	return topic
}
func TestMultiTopicReaderConfigErrors(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := newPartitionedTopic(t)

	reader, err := client.CreateReader(ReaderOptions{
		Topic: topic,
	})
	assert.Nil(t, reader)
	assert.NotNil(t, err)

	// only earliest/latest message id allowed for partitioned topic
	reader, err = client.CreateReader(ReaderOptions{
		StartMessageID: newMessageID(1, 1, 1, 1),
		Topic:          topic,
	})

	assert.NotNil(t, err)
	assert.Nil(t, reader)
}

func TestMultiTopicReader(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	ctx := context.Background()
	topic := newPartitionedTopic(t)

	// create reader
	reader, err := client.CreateReader(ReaderOptions{
		Topic:          topic,
		StartMessageID: EarliestMessageID(),
	})
	assert.Nil(t, err)
	defer reader.Close()

	// expected reader should be of type multiTopicReader
	_, ok := reader.(*multiTopicReader)
	assert.True(t, ok)

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
	count := 0
	for reader.HasNext() {
		_, err := reader.Next(context.Background())
		assert.NoError(t, err)
		count++
	}

	assert.Equal(t, 10, count)
}

func TestMultiTopicReaderOnLatestWithBatching(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	ctx := context.Background()
	topic := newPartitionedTopic(t)

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

	reader, err := client.CreateReader(ReaderOptions{
		Topic:                   topic,
		StartMessageID:          LatestMessageID(),
		StartMessageIDInclusive: false,
	})

	assert.Nil(t, err)
	defer reader.Close()

	// expected reader should be of type multiTopicReader
	_, ok := reader.(*multiTopicReader)
	assert.True(t, ok)

	// Reader should yield no message since it's at the end of the topic
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	msg, err := reader.Next(ctx)
	assert.Error(t, err)
	assert.Nil(t, msg)
	cancel()
}

func TestShouldFailMultiTopicReaderOnSpecificMessage(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	ctx := context.Background()
	topic := newPartitionedTopic(t)
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

	// reader creation should fail as only latest/latest message ids are supported
	assert.NotNil(t, err)
	assert.Nil(t, reader)
}

func TestMultiTopicReaderHasNextAgainstEmptyTopic(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newPartitionedTopic(t)

	// create reader on 5th message (not included)
	reader, err := client.CreateReader(ReaderOptions{
		Topic:          topic,
		StartMessageID: EarliestMessageID(),
	})

	assert.Nil(t, err)
	defer reader.Close()

	// expected reader should be of type multiTopicReader
	_, ok := reader.(*multiTopicReader)
	assert.True(t, ok)

	// should be false since no messages are produced
	assert.Equal(t, reader.HasNext(), false)
}

func TestMultiTopicReaderHasNext(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newPartitionedTopic(t)
	ctx := context.Background()

	reader, err := client.CreateReader(ReaderOptions{
		Topic:          topic,
		StartMessageID: EarliestMessageID(),
	})

	assert.Nil(t, err)
	defer reader.Close()

	// expected reader should be of type multiTopicReader
	_, ok := reader.(*multiTopicReader)
	assert.True(t, ok)

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

	i := 0
	// hashNext should be false if no more messages are available
	for reader.HasNext() {
		_, err := reader.Next(ctx)
		assert.NoError(t, err)

		i++
	}
	assert.Equal(t, 10, i)
}

func TestMultuTopicReaderHasNextWithReceiverQueueSize(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newPartitionedTopic(t)
	ctx := context.Background()

	reader, err := client.CreateReader(ReaderOptions{
		Topic:             topic,
		StartMessageID:    EarliestMessageID(),
		ReceiverQueueSize: 1,
	})

	assert.Nil(t, err)
	defer reader.Close()

	// expected reader should be of type multiTopicReader
	_, ok := reader.(*multiTopicReader)
	assert.True(t, ok)

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

	count := 0

	// HashNext should be false if no messages are available
	for reader.HasNext() {
		msg, err := reader.Next(ctx)
		assert.Nil(t, err)
		assert.NotNil(t, msg)

		count++
	}

	assert.Equal(t, 10, count)
}

func TestMultiTopicReaderWithLatestMsg(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newPartitionedTopic(t)
	ctx := context.Background()

	// create reader on the last message (inclusive)
	reader0, err := client.CreateReader(ReaderOptions{
		Topic:                   topic,
		StartMessageID:          LatestMessageID(),
		StartMessageIDInclusive: true,
	})

	assert.Nil(t, err)
	defer reader0.Close()

	// expected reader should be of type multiTopicReader
	_, ok := reader0.(*multiTopicReader)
	assert.True(t, ok)

	assert.False(t, reader0.HasNext())

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	msgIDMap := make(map[int32]struct{})
	// send 10 messages
	for i := 0; i < 10; i++ {
		msgID, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.NoError(t, err)
		assert.NotNil(t, msgID)

		// to track the partitions used for message
		msgIDMap[msgID.PartitionIdx()] = struct{}{}
	}

	reader, err := client.CreateReader(ReaderOptions{
		Topic:                   topic,
		StartMessageID:          LatestMessageID(),
		StartMessageIDInclusive: true,
	})

	assert.Nil(t, err)
	defer reader.Close()

	// expected reader should be of type multiTopicReader
	_, ok = reader.(*multiTopicReader)
	assert.True(t, ok)

	count := 0
	// should only read latest message from each partition
	for reader.HasNext() {
		msg, err := reader.Next(ctx)
		assert.Nil(t, err)
		assert.NotNil(t, msg)

		count++
	}

	// should only read latest message from each partition
	assert.Equal(t, len(msgIDMap), count)
}

func TestMultiTopicProducerReaderRSAEncryption(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newPartitionedTopic(t)
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

	// expected reader should be of type multiTopicReader
	_, ok := reader.(*multiTopicReader)
	assert.True(t, ok)

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
	count := 0
	for reader.HasNext() {
		msg, err := reader.Next(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, msg)
		count++
	}

	assert.Equal(t, 10, count)
}
