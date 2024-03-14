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
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
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
