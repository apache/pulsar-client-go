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
	"log"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

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

func TestTopics_CreateWithProperties(t *testing.T) {
	topic := newTopicName()
	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	// Create non-partition topic
	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)
	err = admin.Topics().CreateWithProperties(*topicName, 0, map[string]string{
		"key1": "value1",
	})
	assert.NoError(t, err)

	properties, err := admin.Topics().GetProperties(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, properties["key1"], "value1")

	// Create partition topic
	topic = newTopicName()
	topicName, err = utils.GetTopicName(topic)
	assert.NoError(t, err)
	err = admin.Topics().CreateWithProperties(*topicName, 4, map[string]string{
		"key2": "value2",
	})
	assert.NoError(t, err)

	properties, err = admin.Topics().GetProperties(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, properties["key2"], "value2")
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
	subName := "my-sub"
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subName,
		Type:             pulsar.Exclusive,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer
	producerName := "test-producer"
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
		Name:            producerName,
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

	partition, err := topicName.GetPartition(0)
	assert.Nil(t, err)
	topicState, err := admin.Topics().GetStats(*partition)
	assert.Nil(t, err)
	assert.Equal(t, len(topicState.Publishers), 1)
	publisher := topicState.Publishers[0]
	assert.Equal(t, publisher.AccessModel, utils.ProduceModeShared)
	assert.Equal(t, publisher.IsSupportsPartialProducer, false)
	assert.Equal(t, publisher.ProducerName, producerName)
	assert.Contains(t, publisher.Address, "127.0.0.1")
	assert.Contains(t, publisher.ClientVersion, "Pulsar Go version")

	sub := topicState.Subscriptions[subName]
	assert.Equal(t, sub.BytesOutCounter, int64(0))
	assert.Equal(t, sub.MsgOutCounter, int64(0))
	assert.Equal(t, sub.MessageAckRate, float64(0))
	assert.Equal(t, sub.ChunkedMessageRate, float64(0))
	assert.Equal(t, sub.BacklogSize, int64(0))
	assert.Equal(t, sub.EarliestMsgPublishTimeInBacklog, int64(0))
	assert.Equal(t, sub.LastExpireTimestamp, int64(0))
	assert.Equal(t, sub.TotalMsgExpired, int64(0))
	assert.Equal(t, sub.LastMarkDeleteAdvancedTimestamp, int64(0))
	assert.Equal(t, sub.IsDurable, true)
	assert.Equal(t, sub.AllowOutOfOrderDelivery, false)
	assert.Equal(t, sub.ConsumersAfterMarkDeletePosition, map[string]string{})
	assert.Equal(t, sub.NonContiguousDeletedMessagesRanges, 0)
	assert.Equal(t, sub.NonContiguousDeletedMessagesRangesSrzSize, 0)
	assert.Equal(t, sub.DelayedMessageIndexSizeInBytes, int64(0))
	assert.Equal(t, sub.SubscriptionProperties, map[string]string{})
	assert.Equal(t, sub.FilterProcessedMsgCount, int64(0))
	assert.Equal(t, sub.FilterAcceptedMsgCount, int64(0))
	assert.Equal(t, sub.FilterRejectedMsgCount, int64(0))
	assert.Equal(t, sub.FilterRescheduledMsgCount, int64(0))

	assert.Equal(t, len(sub.Consumers), 1)
	consumerState := sub.Consumers[0]
	assert.Equal(t, consumerState.BytesOutCounter, int64(0))
	assert.Equal(t, consumerState.MsgOutCounter, int64(0))
	assert.Equal(t, consumerState.MessageAckRate, float64(0))
	assert.Equal(t, consumerState.ChunkedMessageRate, float64(0))
	assert.Equal(t, consumerState.AvgMessagesPerEntry, int(0))
	assert.Contains(t, consumerState.Address, "127.0.0.1")
	assert.Contains(t, consumerState.ClientVersion, "Pulsar Go version")
	assert.Equal(t, consumerState.LastAckedTimestamp, int64(0))
	assert.Equal(t, consumerState.LastConsumedTimestamp, int64(0))
	assert.True(t, consumerState.LastConsumedFlowTimestamp > 0)

}
func TestNonPartitionState(t *testing.T) {
	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName

	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	// Create non-partition topic
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

func TestDeleteNonPartitionedTopic(t *testing.T) {
	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName

	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)
	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)
	err = admin.Topics().Create(*topicName, 0)
	assert.NoError(t, err)
	err = admin.Topics().Delete(*topicName, false, true)
	assert.NoError(t, err)
	topicList, err := admin.Namespaces().GetTopics("public/default")
	assert.NoError(t, err)
	isTopicExist := false
	for _, topicIterator := range topicList {
		if topicIterator == topic {
			isTopicExist = true
		}
	}
	assert.Equal(t, false, isTopicExist)
}

func TestDeletePartitionedTopic(t *testing.T) {
	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName

	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)
	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)
	err = admin.Topics().Create(*topicName, 3)
	assert.NoError(t, err)
	err = admin.Topics().Delete(*topicName, false, false)
	assert.NoError(t, err)
	topicList, err := admin.Namespaces().GetTopics("public/default")
	assert.NoError(t, err)
	isTopicExist := false
	for _, topicIterator := range topicList {
		if topicIterator == topic {
			isTopicExist = true
		}
	}
	assert.Equal(t, false, isTopicExist)
}

func TestUpdateTopicPartitions(t *testing.T) {
	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName

	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)
	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)
	err = admin.Topics().Create(*topicName, 3)
	assert.NoError(t, err)
	topicMetadata, err := admin.Topics().GetMetadata(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, 3, topicMetadata.Partitions)

	err = admin.Topics().Update(*topicName, 4)
	assert.NoError(t, err)
	topicMetadata, err = admin.Topics().GetMetadata(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, 4, topicMetadata.Partitions)
}

func TestGetMessageID(t *testing.T) {
	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName
	topicPartitionZero := topic + "-partition-0"
	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)
	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)
	topicPartitionZeroName, err := utils.GetTopicName(topicPartitionZero)
	assert.NoError(t, err)
	err = admin.Topics().Create(*topicName, 1)
	assert.NoError(t, err)
	ctx := context.Background()

	// create consumer
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "my-sub",
		Type:             pulsar.Exclusive,
	})
	assert.NoError(t, err)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.NoError(t, err)
	defer producer.Close()
	_, err = producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte("hello"),
		Key:     "pulsar",
		Properties: map[string]string{
			"key-1": "pulsar-1",
		},
	})
	assert.NoError(t, err)

	// ack message
	msg, err := consumer.Receive(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), msg.Payload())
	assert.Equal(t, "pulsar", msg.Key())
	err = consumer.Ack(msg)
	assert.NoError(t, err)

	messageID, err := admin.Topics().GetMessageID(
		*topicPartitionZeroName,
		msg.PublishTime().Unix()*1000-1000,
	)
	assert.NoError(t, err)
	assert.Equal(t, msg.ID().EntryID(), messageID.EntryID)
	assert.Equal(t, msg.ID().LedgerID(), messageID.LedgerID)
	assert.Equal(t, int(msg.ID().PartitionIdx()), messageID.PartitionIndex)
}

func TestMessageTTL(t *testing.T) {
	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName
	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)
	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)
	err = admin.Topics().Create(*topicName, 4)
	assert.NoError(t, err)

	messageTTL, err := admin.Topics().GetMessageTTL(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, 0, messageTTL)
	err = admin.Topics().SetMessageTTL(*topicName, 600)
	assert.NoError(t, err)
	//	topic policy is an async operation,
	//	so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			messageTTL, err = admin.Topics().GetMessageTTL(*topicName)
			return err == nil && messageTTL == 600
		},
		10*time.Second,
		100*time.Millisecond,
	)
	err = admin.Topics().RemoveMessageTTL(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			messageTTL, err = admin.Topics().GetMessageTTL(*topicName)
			return err == nil && messageTTL == 0
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestRetention(t *testing.T) {
	randomName := newTopicName()
	topic := "persistent://public/default/" + randomName
	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)
	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)
	err = admin.Topics().Create(*topicName, 4)
	assert.NoError(t, err)

	topicRetentionPolicy, err := admin.Topics().GetRetention(*topicName, false)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), topicRetentionPolicy.RetentionSizeInMB)
	assert.Equal(t, 0, topicRetentionPolicy.RetentionTimeInMinutes)
	err = admin.Topics().SetRetention(*topicName, utils.RetentionPolicies{
		RetentionSizeInMB:      20480,
		RetentionTimeInMinutes: 1440,
	})
	assert.NoError(t, err)
	//	topic policy is an async operation,
	//	so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			topicRetentionPolicy, err = admin.Topics().GetRetention(*topicName, false)
			return err == nil &&
				topicRetentionPolicy.RetentionSizeInMB == int64(20480) &&
				topicRetentionPolicy.RetentionTimeInMinutes == 1440
		},
		10*time.Second,
		100*time.Millisecond,
	)
	err = admin.Topics().RemoveRetention(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			topicRetentionPolicy, err = admin.Topics().GetRetention(*topicName, false)
			return err == nil &&
				topicRetentionPolicy.RetentionSizeInMB == int64(0) &&
				topicRetentionPolicy.RetentionTimeInMinutes == 0
		},
		10*time.Second,
		100*time.Millisecond,
	)
}
