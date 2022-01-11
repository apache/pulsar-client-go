package pulsar

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMultiTopicReaderConfigErrors(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/testReaderPartitions"
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/testReaderPartitions/partitions"

	makeHTTPCall(t, http.MethodPut, testURL, "64")

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
	topic := "persistent://public/default/testReaderPartitions"
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/testReaderPartitions/partitions"

	makeHTTPCall(t, http.MethodPut, testURL, "5")

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
	for i := 0; i < 10; i++ {
		msg, err := reader.Next(context.Background())
		assert.NoError(t, err)

		expectMsg := fmt.Sprintf("hello-%d", i)
		assert.Equal(t, []byte(expectMsg), msg.Payload())
	}
}

func TestMultiTopicReaderOnLatestWithBatching(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	ctx := context.Background()
	topic := "persistent://public/default/testReaderPartitions"
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/testReaderPartitions/partitions"

	makeHTTPCall(t, http.MethodPut, testURL, "5")

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
	topic := "persistent://public/default/testReaderPartitions"
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/testReaderPartitions/partitions"

	makeHTTPCall(t, http.MethodPut, testURL, "5")

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

	assert.NotNil(t, err)
	assert.Nil(t, reader)
}

func TestMultiTopicReaderHasNextAgainstEmptyTopic(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/testReaderPartitionsEmpty"
	testURL := adminURL + "/" + "admin/v2/persistent/public/default/testReaderPartitionsEmpty/partitions"

	makeHTTPCall(t, http.MethodPut, testURL, "5")

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

	assert.Equal(t, reader.HasNext(), false)
}

func TestMultiTopicReaderHasNext(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topicName := fmt.Sprintf("testReaderPartitionsHashNext-%v", time.Now().Nanosecond())
	topic := fmt.Sprintf("persistent://public/default/%v", topicName)
	testURL := adminURL + "/" + fmt.Sprintf("admin/v2/persistent/public/default/%v/partitions", topicName)

	makeHTTPCall(t, http.MethodPut, testURL, "5")
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
	for reader.HasNext() {
		msg, err := reader.Next(ctx)
		assert.NoError(t, err)

		expectMsg := fmt.Sprintf("hello-%d", i)
		assert.Equal(t, []byte(expectMsg), msg.Payload())

		i++
	}

	assert.Equal(t, 10, i)
}
