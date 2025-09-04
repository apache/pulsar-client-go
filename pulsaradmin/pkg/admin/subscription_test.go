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

package admin

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestGetMessagesByID(t *testing.T) {

	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName

	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	ctx := context.Background()

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	// create producer
	numberMessages := 10
	batchingMaxMessages := numberMessages / 2
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   topic,
		DisableBatching:         false,
		BatchingMaxMessages:     uint(batchingMaxMessages),
		BatchingMaxPublishDelay: time.Second * 60,
	})
	assert.Nil(t, err)
	defer producer.Close()

	var wg sync.WaitGroup
	wg.Add(numberMessages)
	messageIDMap := make(map[string]int32)
	for i := 0; i <= numberMessages; i++ {
		producer.SendAsync(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}, func(id pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
			assert.Nil(t, err)
			messageIDMap[id.String()]++
			wg.Done()
		})
	}
	wg.Wait()
	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)
	for id, i := range messageIDMap {
		assert.Equal(t, i, int32(batchingMaxMessages))
		messageID, err := utils.ParseMessageID(id)
		assert.Nil(t, err)
		messages, err := admin.Subscriptions().GetMessagesByID(*topicName, messageID.LedgerID, messageID.EntryID)
		assert.Nil(t, err)
		assert.Equal(t, batchingMaxMessages, len(messages))
	}

	_, err = admin.Subscriptions().GetMessagesByID(*topicName, 1024, 2048)
	assert.Errorf(t, err, "Message id not found")

}

func TestPeekMessageForPartitionedTopic(t *testing.T) {
	ctx := context.Background()
	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName
	topicName, _ := utils.GetTopicName(topic)
	subName := "test-sub"

	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	err = admin.Topics().Create(*topicName, 2)
	assert.NoError(t, err)

	err = admin.Subscriptions().Create(*topicName, subName, utils.Earliest)
	assert.NoError(t, err)

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.NoError(t, err)
	defer producer.Close()

	for i := 0; i < 100; i++ {
		producer.SendAsync(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}, nil)
	}
	err = producer.FlushWithCtx(ctx)
	if err != nil {
		return
	}

	for i := 0; i < 2; i++ {
		topicWithPartition := fmt.Sprintf("%s-partition-%d", topic, i)
		topicName, err := utils.GetTopicName(topicWithPartition)
		assert.NoError(t, err)
		messages, err := admin.Subscriptions().PeekMessages(*topicName, subName, 10)
		assert.NoError(t, err)
		assert.NotNil(t, messages)
		for _, msg := range messages {
			assert.Equal(t, msg.GetMessageID().PartitionIndex, i)
		}
	}
}

func TestPeekMessagesWithProperties(t *testing.T) {
	tests := map[string]struct {
		batched bool
	}{
		"non-batched": {
			batched: false,
		},
		"batched": {
			batched: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			randomName := newTopicName()
			topic := "persistent://public/default/" + randomName
			topicName, _ := utils.GetTopicName(topic)
			subName := "test-sub"

			cfg := &config.Config{}
			admin, err := New(cfg)
			assert.NoError(t, err)
			assert.NotNil(t, admin)

			client, err := pulsar.NewClient(pulsar.ClientOptions{
				URL: lookupURL,
			})
			assert.NoError(t, err)
			defer client.Close()

			var producer pulsar.Producer
			batchSize := 5
			if tc.batched {
				producer, err = client.CreateProducer(pulsar.ProducerOptions{
					Topic:                   topic,
					DisableBatching:         false,
					BatchingMaxMessages:     uint(batchSize),
					BatchingMaxPublishDelay: time.Second * 2,
				})
				assert.NoError(t, err)
				defer producer.Close()
			} else {
				producer, err = client.CreateProducer(pulsar.ProducerOptions{
					Topic:           topic,
					DisableBatching: true,
				})
				assert.NoError(t, err)
				defer producer.Close()
			}

			props := map[string]string{
				"key1":        "value1",
				"KEY2":        "VALUE2",
				"KeY3":        "VaLuE3",
				"details=man": "good at playing basketball",
			}

			var wg sync.WaitGroup
			numberOfMessagesToWaitFor := 10
			numberOfMessagesToSend := numberOfMessagesToWaitFor
			if tc.batched {
				// If batched send one extra message to cause the batch to be sent immediately
				numberOfMessagesToSend += 1
			}
			wg.Add(numberOfMessagesToWaitFor)

			for i := 0; i < numberOfMessagesToSend; i++ {
				producer.SendAsync(ctx, &pulsar.ProducerMessage{
					Payload:    []byte("test-message"),
					Properties: props,
				}, func(id pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
					assert.Nil(t, err)
					if i < numberOfMessagesToWaitFor {
						wg.Done()
					}
				})
			}
			wg.Wait()

			// Peek messages
			messages, err := admin.Subscriptions().PeekMessages(*topicName, subName, batchSize)
			assert.NoError(t, err)
			assert.NotNil(t, messages)
			assert.Len(t, messages, batchSize)

			// Verify properties of messages
			for _, msg := range messages {
				assert.Equal(t, "value1", msg.Properties["key1"])
				assert.Equal(t, "VALUE2", msg.Properties["KEY2"])
				assert.Equal(t, "VaLuE3", msg.Properties["KeY3"])
				assert.Equal(t, "good at playing basketball", msg.Properties["details=man"])
				// Standard pulsar properties, set by pulsar
				assert.NotEmpty(t, msg.Properties["publish-time"])
				if tc.batched {
					assert.NotEmpty(t, msg.Properties[BatchHeader])
					assert.Equal(t, strconv.Itoa(batchSize), msg.Properties[BatchHeader])
				}
			}
		})
	}
}
func TestGetMessageByID(t *testing.T) {
	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName

	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	ctx := context.Background()

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	// create producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	messageID, err := producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte("hello"),
	})
	assert.Nil(t, err)

	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)
	id, err := admin.Subscriptions().GetMessageByID(*topicName, messageID.LedgerID(), messageID.EntryID())
	assert.Nil(t, err)
	assert.Equal(t, id.MessageID.LedgerID, messageID.LedgerID())
	assert.Equal(t, id.MessageID.EntryID, messageID.EntryID())
}
