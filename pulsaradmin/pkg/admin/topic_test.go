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
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

var (
	lookupURL = "pulsar://localhost:6650"
)

func TestCreateTopic(t *testing.T) {
	checkError := func(err error) {
		if err != nil {
			t.Error(err)
		}
	}

	cfg := &config.Config{}
	admin, err := New(cfg)
	checkError(err)

	topic := "persistent://public/default/testCreateTopic"

	topicName, err := utils.GetTopicName(topic)
	checkError(err)

	err = admin.Topics().Create(*topicName, 0)
	checkError(err)

	topicLists, err := admin.Namespaces().GetTopics("public/default")
	checkError(err)

	for _, t := range topicLists {
		if t == topic {
			return
		}
	}
	t.Error("Couldn't find topic: " + topic)
}

func TestPartitionState(t *testing.T) {
	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName

	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	// Create partition topic
	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)
	err = admin.Topics().Create(*topicName, 4)
	assert.NoError(t, err)

	// Send message
	ctx := context.Background()

	// create consumer
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
		Type:             pulsar.Exclusive,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Key:     "pulsar",
			Properties: map[string]string{
				"key-1": "pulsar-1",
			},
		}); err != nil {
			log.Fatal(err)
		}
	}

	stats, err := admin.Topics().GetPartitionedStatsWithOption(*topicName, true, utils.GetStatsOptions{
		GetPreciseBacklog:        false,
		SubscriptionBacklogSize:  false,
		GetEarliestTimeInBacklog: false,
		ExcludePublishers:        true,
		ExcludeConsumers:         true,
	})
	assert.Nil(t, err)
	assert.Equal(t, len(stats.Publishers), 0)

	for _, topicStats := range stats.Partitions {
		assert.Equal(t, len(topicStats.Publishers), 0)
		for _, subscriptionStats := range topicStats.Subscriptions {
			assert.Equal(t, len(subscriptionStats.Consumers), 0)
		}
	}

	for _, subscriptionStats := range stats.Subscriptions {
		assert.Equal(t, len(subscriptionStats.Consumers), 0)
	}

}
func TestNonPartitionState(t *testing.T) {
	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName

	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	// Create partition topic
	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)
	err = admin.Topics().Create(*topicName, 0)
	assert.NoError(t, err)

	// Send message
	ctx := context.Background()

	// create consumer
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
		Type:             pulsar.Exclusive,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Key:     "pulsar",
			Properties: map[string]string{
				"key-1": "pulsar-1",
			},
		}); err != nil {
			log.Fatal(err)
		}
	}

	stats, err := admin.Topics().GetStatsWithOption(*topicName, utils.GetStatsOptions{
		GetPreciseBacklog:        false,
		SubscriptionBacklogSize:  false,
		GetEarliestTimeInBacklog: false,
		ExcludePublishers:        true,
		ExcludeConsumers:         true,
	})
	assert.Nil(t, err)
	assert.Equal(t, len(stats.Publishers), 0)
	for _, subscriptionStats := range stats.Subscriptions {
		assert.Equal(t, len(subscriptionStats.Consumers), 0)
	}

}

func newTopicName() string {
	return fmt.Sprintf("my-topic-%v", time.Now().Nanosecond())
}
