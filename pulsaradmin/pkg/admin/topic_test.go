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
	"strings"
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

func TestTopics_Properties(t *testing.T) {
	t.Run("NonPartitioned", func(t *testing.T) {
		internalTestTopicsProperties(t, 0)
	})
	t.Run("Partitioned", func(t *testing.T) {
		internalTestTopicsProperties(t, 4)
	})
}

func verifyTopicProperties(t *testing.T, admin Client, topic *utils.TopicName,
	expected map[string]string) {
	properties, err := admin.Topics().GetProperties(*topic)
	assert.NoError(t, err)
	assert.Equal(t, expected, properties)
}

func internalTestTopicsProperties(t *testing.T, partitions int) {
	topic := newTopicName()
	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)
	err = admin.Topics().CreateWithProperties(*topicName, partitions, map[string]string{
		"key1": "value1",
	})
	assert.NoError(t, err)
	verifyTopicProperties(t, admin, topicName, map[string]string{"key1": "value1"})

	properties, err := admin.Topics().GetProperties(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, properties["key1"], "value1")

	newProperties := map[string]string{
		"key1": "value1-updated",
		"key2": "value2",
	}
	err = admin.Topics().UpdateProperties(*topicName, newProperties)
	assert.NoError(t, err)
	verifyTopicProperties(t, admin, topicName, newProperties)

	err = admin.Topics().UpdateProperties(*topicName, map[string]string{"key3": "value3"})
	assert.NoError(t, err)
	verifyTopicProperties(t, admin, topicName, map[string]string{
		"key1": "value1-updated",
		"key2": "value2",
		"key3": "value3",
	})

	err = admin.Topics().RemoveProperty(*topicName, "key1")
	assert.NoError(t, err)
	verifyTopicProperties(t, admin, topicName, map[string]string{
		"key2": "value2",
		"key3": "value3",
	})
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
	assert.Contains(t, publisher.ClientVersion, "Pulsar-Go-version")
	assert.NotContains(t, publisher.ClientVersion, " ")

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
	assert.Contains(t, consumerState.ClientVersion, "Pulsar-Go-version")
	assert.NotContains(t, consumerState.ClientVersion, " ")
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
	assert.Equal(t, -1, messageTTL)
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
			return err == nil && messageTTL == -1
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

	// Initial state: policy not configured, should return nil
	topicRetentionPolicy, err := admin.Topics().GetRetention(*topicName, false)
	assert.NoError(t, err)
	assert.Nil(t, topicRetentionPolicy, "Expected nil when retention policy is not configured")
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
	// After removal, should return nil (not configured)
	assert.Eventually(
		t,
		func() bool {
			topicRetentionPolicy, err = admin.Topics().GetRetention(*topicName, false)
			return err == nil && topicRetentionPolicy == nil
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestSubscribeRate(t *testing.T) {
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

	// Initial state: policy not configured, should return nil
	initialSubscribeRate, err := admin.Topics().GetSubscribeRate(*topicName)
	assert.NoError(t, err)
	assert.Nil(t, initialSubscribeRate, "Expected nil when subscribe rate is not configured")

	// Set new subscribe rate
	newSubscribeRate := utils.SubscribeRate{
		SubscribeThrottlingRatePerConsumer: 10,
		RatePeriodInSecond:                 60,
	}
	err = admin.Topics().SetSubscribeRate(*topicName, newSubscribeRate)
	assert.NoError(t, err)

	// topic policy is an async operation,
	// so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			subscribeRate, err := admin.Topics().GetSubscribeRate(*topicName)
			return err == nil &&
				subscribeRate.SubscribeThrottlingRatePerConsumer == 10 &&
				subscribeRate.RatePeriodInSecond == 60
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove subscribe rate policy - should return nil
	err = admin.Topics().RemoveSubscribeRate(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			subscribeRate, err := admin.Topics().GetSubscribeRate(*topicName)
			return err == nil && subscribeRate == nil
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestSubscriptionDispatchRate(t *testing.T) {
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

	// Initial state: policy not configured, should return nil
	initialDispatchRate, err := admin.Topics().GetSubscriptionDispatchRate(*topicName)
	assert.NoError(t, err)
	assert.Nil(t, initialDispatchRate, "Expected nil when subscription dispatch rate is not configured")

	// Set new subscription dispatch rate
	newDispatchRate := utils.DispatchRateData{
		DispatchThrottlingRateInMsg:  1000,
		DispatchThrottlingRateInByte: 1048576, // 1MB
		RatePeriodInSecond:           30,
		RelativeToPublishRate:        true,
	}
	err = admin.Topics().SetSubscriptionDispatchRate(*topicName, newDispatchRate)
	assert.NoError(t, err)

	// topic policy is an async operation,
	// so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			dispatchRate, err := admin.Topics().GetSubscriptionDispatchRate(*topicName)
			return err == nil &&
				dispatchRate.DispatchThrottlingRateInMsg == 1000 &&
				dispatchRate.DispatchThrottlingRateInByte == 1048576 &&
				dispatchRate.RatePeriodInSecond == 30 &&
				dispatchRate.RelativeToPublishRate == true
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove subscription dispatch rate policy - should return nil
	err = admin.Topics().RemoveSubscriptionDispatchRate(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			dispatchRate, err := admin.Topics().GetSubscriptionDispatchRate(*topicName)
			return err == nil && dispatchRate == nil
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestMaxConsumersPerSubscription(t *testing.T) {
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

	// Get default max consumers per subscription
	maxConsumers, err := admin.Topics().GetMaxConsumersPerSubscription(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, -1, maxConsumers)

	// Set new max consumers per subscription
	err = admin.Topics().SetMaxConsumersPerSubscription(*topicName, 10)
	assert.NoError(t, err)

	// topic policy is an async operation,
	// so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			maxConsumers, err = admin.Topics().GetMaxConsumersPerSubscription(*topicName)
			return err == nil && maxConsumers == 10
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove max consumers per subscription policy
	err = admin.Topics().RemoveMaxConsumersPerSubscription(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			maxConsumers, err = admin.Topics().GetMaxConsumersPerSubscription(*topicName)
			return err == nil && maxConsumers == -1
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestMaxMessageSize(t *testing.T) {
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

	// Get default max message size
	maxMessageSize, err := admin.Topics().GetMaxMessageSize(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, -1, maxMessageSize)

	// Set new max message size (1MB)
	err = admin.Topics().SetMaxMessageSize(*topicName, 1048576)
	assert.NoError(t, err)

	// topic policy is an async operation,
	// so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			maxMessageSize, err = admin.Topics().GetMaxMessageSize(*topicName)
			return err == nil && maxMessageSize == 1048576
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove max message size policy
	err = admin.Topics().RemoveMaxMessageSize(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			maxMessageSize, err = admin.Topics().GetMaxMessageSize(*topicName)
			return err == nil && maxMessageSize == -1
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestMaxSubscriptionsPerTopic(t *testing.T) {
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

	// Get default max subscriptions per topic
	maxSubscriptions, err := admin.Topics().GetMaxSubscriptionsPerTopic(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, -1, maxSubscriptions)

	// Set new max subscriptions per topic
	err = admin.Topics().SetMaxSubscriptionsPerTopic(*topicName, 100)
	assert.NoError(t, err)

	// topic policy is an async operation,
	// so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			maxSubscriptions, err = admin.Topics().GetMaxSubscriptionsPerTopic(*topicName)
			return err == nil && maxSubscriptions == 100
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove max subscriptions per topic policy
	err = admin.Topics().RemoveMaxSubscriptionsPerTopic(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			maxSubscriptions, err = admin.Topics().GetMaxSubscriptionsPerTopic(*topicName)
			return err == nil && maxSubscriptions == -1
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestSchemaValidationEnforced(t *testing.T) {
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

	// Get default schema validation enforced
	schemaValidationEnforced, err := admin.Topics().GetSchemaValidationEnforced(*topicName)
	if err != nil {
		// Skip test if API is not available (e.g., 405 Method Not Allowed)
		if strings.Contains(err.Error(), "405") || strings.Contains(err.Error(), "Method Not Allowed") {
			t.Skip("SchemaValidationEnforced API not available on this Pulsar version")
			return
		}
		assert.NoError(t, err)
	}
	initialValidationEnforced := schemaValidationEnforced

	// Set schema validation enforced to true
	err = admin.Topics().SetSchemaValidationEnforced(*topicName, true)
	if err != nil {
		// Skip test if API is not available
		if strings.Contains(err.Error(), "405") || strings.Contains(err.Error(), "Method Not Allowed") {
			t.Skip("SetSchemaValidationEnforced API not available on this Pulsar version")
			return
		}
		assert.NoError(t, err)
	}

	// topic policy is an async operation,
	// so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			schemaValidationEnforced, err := admin.Topics().GetSchemaValidationEnforced(*topicName)
			return err == nil && schemaValidationEnforced == true
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove schema validation enforced policy
	err = admin.Topics().RemoveSchemaValidationEnforced(*topicName)
	if err != nil {
		// Skip removal check if API is not available
		if strings.Contains(err.Error(), "405") || strings.Contains(err.Error(), "Method Not Allowed") {
			t.Skip("RemoveSchemaValidationEnforced API not available on this Pulsar version")
			return
		}
		assert.NoError(t, err)
	}
	assert.Eventually(
		t,
		func() bool {
			schemaValidationEnforced, err := admin.Topics().GetSchemaValidationEnforced(*topicName)
			return err == nil && schemaValidationEnforced == initialValidationEnforced
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestDeduplicationSnapshotInterval(t *testing.T) {
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

	// Get default deduplication snapshot interval
	interval, err := admin.Topics().GetDeduplicationSnapshotInterval(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, -1, interval)

	// Set new deduplication snapshot interval
	err = admin.Topics().SetDeduplicationSnapshotInterval(*topicName, 1000)
	assert.NoError(t, err)

	// topic policy is an async operation,
	// so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			interval, err = admin.Topics().GetDeduplicationSnapshotInterval(*topicName)
			return err == nil && interval == 1000
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove deduplication snapshot interval policy
	err = admin.Topics().RemoveDeduplicationSnapshotInterval(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			interval, err = admin.Topics().GetDeduplicationSnapshotInterval(*topicName)
			return err == nil && interval == -1
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestReplicatorDispatchRate(t *testing.T) {
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

	// Initial state: policy not configured, should return nil
	initialDispatchRate, err := admin.Topics().GetReplicatorDispatchRate(*topicName)
	assert.NoError(t, err)
	assert.Nil(t, initialDispatchRate, "Expected nil when replicator dispatch rate is not configured")

	// Set new replicator dispatch rate
	newDispatchRate := utils.DispatchRateData{
		DispatchThrottlingRateInMsg:  500,
		DispatchThrottlingRateInByte: 524288, // 512KB
		RatePeriodInSecond:           60,
		RelativeToPublishRate:        true,
	}
	err = admin.Topics().SetReplicatorDispatchRate(*topicName, newDispatchRate)
	assert.NoError(t, err)

	// topic policy is an async operation,
	// so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			dispatchRate, err := admin.Topics().GetReplicatorDispatchRate(*topicName)
			return err == nil &&
				dispatchRate.DispatchThrottlingRateInMsg == 500 &&
				dispatchRate.DispatchThrottlingRateInByte == 524288 &&
				dispatchRate.RatePeriodInSecond == 60 &&
				dispatchRate.RelativeToPublishRate == true
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove replicator dispatch rate policy - should return nil
	err = admin.Topics().RemoveReplicatorDispatchRate(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			dispatchRate, err := admin.Topics().GetReplicatorDispatchRate(*topicName)
			return err == nil && dispatchRate == nil
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestOffloadPolicies(t *testing.T) {
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

	// Initial state: policy not configured, should return nil
	offloadPolicies, err := admin.Topics().GetOffloadPolicies(*topicName)
	assert.NoError(t, err)
	assert.Nil(t, offloadPolicies, "Expected nil when offload policies are not configured")

	// Set new offload policies
	newOffloadPolicies := utils.OffloadPolicies{
		ManagedLedgerOffloadDriver:                        "aws-s3",
		ManagedLedgerOffloadMaxThreads:                    4,
		ManagedLedgerOffloadThresholdInBytes:              1073741824, // 1GB
		ManagedLedgerOffloadDeletionLagInMillis:           3600000,    // 1 hour
		ManagedLedgerOffloadAutoTriggerSizeThresholdBytes: 2147483648, // 2GB
		S3ManagedLedgerOffloadBucket:                      "test-bucket",
		S3ManagedLedgerOffloadRegion:                      "us-west-2",
		S3ManagedLedgerOffloadServiceEndpoint:             "https://s3.amazonaws.com",
	}
	err = admin.Topics().SetOffloadPolicies(*topicName, newOffloadPolicies)
	assert.NoError(t, err)

	// topic policy is an async operation,
	// so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			offloadPolicies, err = admin.Topics().GetOffloadPolicies(*topicName)
			return err == nil &&
				offloadPolicies.ManagedLedgerOffloadDriver == "aws-s3" &&
				offloadPolicies.ManagedLedgerOffloadMaxThreads == 4 &&
				offloadPolicies.ManagedLedgerOffloadThresholdInBytes == 1073741824 &&
				offloadPolicies.S3ManagedLedgerOffloadBucket == "test-bucket"
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove offload policies - should return nil
	err = admin.Topics().RemoveOffloadPolicies(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			offloadPolicies, err = admin.Topics().GetOffloadPolicies(*topicName)
			return err == nil && offloadPolicies == nil
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestAutoSubscriptionCreation(t *testing.T) {
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

	// Initial state: policy not configured, should return nil
	autoSubCreation, err := admin.Topics().GetAutoSubscriptionCreation(*topicName)
	assert.NoError(t, err)
	assert.Nil(t, autoSubCreation, "Expected nil when auto subscription creation is not configured")

	// Set auto subscription creation to true
	newAutoSubCreation := utils.AutoSubscriptionCreationOverride{
		AllowAutoSubscriptionCreation: true,
	}
	err = admin.Topics().SetAutoSubscriptionCreation(*topicName, newAutoSubCreation)
	assert.NoError(t, err)

	// topic policy is an async operation,
	// so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			autoSubCreation, err = admin.Topics().GetAutoSubscriptionCreation(*topicName)
			return err == nil &&
				autoSubCreation.AllowAutoSubscriptionCreation == true
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove auto subscription creation policy - should return nil
	err = admin.Topics().RemoveAutoSubscriptionCreation(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			autoSubCreation, err = admin.Topics().GetAutoSubscriptionCreation(*topicName)
			return err == nil && autoSubCreation == nil
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestSchemaCompatibilityStrategy(t *testing.T) {
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

	// Get default schema compatibility strategy (adapt to actual server behavior)
	initialStrategy, err := admin.Topics().GetSchemaCompatibilityStrategy(*topicName)
	assert.NoError(t, err)
	// Server may return empty string instead of "UNDEFINED"

	// Set new schema compatibility strategy
	err = admin.Topics().SetSchemaCompatibilityStrategy(*topicName, utils.SchemaCompatibilityStrategyBackward)
	assert.NoError(t, err)

	// topic policy is an async operation,
	// so we need to wait for a while to get current value
	assert.Eventually(
		t,
		func() bool {
			strategy, err := admin.Topics().GetSchemaCompatibilityStrategy(*topicName)
			return err == nil &&
				strategy == utils.SchemaCompatibilityStrategyBackward
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove schema compatibility strategy policy
	err = admin.Topics().RemoveSchemaCompatibilityStrategy(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			strategy, err := admin.Topics().GetSchemaCompatibilityStrategy(*topicName)
			return err == nil &&
				strategy == initialStrategy
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestTopics_MaxProducers(t *testing.T) {
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

	// Get default (should be -1)
	maxProducers, err := admin.Topics().GetMaxProducers(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, -1, maxProducers)

	// Set to 0 explicitly (unlimited)
	err = admin.Topics().SetMaxProducers(*topicName, 0)
	assert.NoError(t, err)

	// Verify returns 0
	assert.Eventually(
		t,
		func() bool {
			maxProducers, err = admin.Topics().GetMaxProducers(*topicName)
			return err == nil && maxProducers == 0
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Set to positive value
	err = admin.Topics().SetMaxProducers(*topicName, 10)
	assert.NoError(t, err)

	// Verify returns value
	assert.Eventually(
		t,
		func() bool {
			maxProducers, err = admin.Topics().GetMaxProducers(*topicName)
			return err == nil && maxProducers == 10
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove
	err = admin.Topics().RemoveMaxProducers(*topicName)
	assert.NoError(t, err)
}

func TestTopics_MaxConsumers(t *testing.T) {
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

	// Get default (should be -1)
	maxConsumers, err := admin.Topics().GetMaxConsumers(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, -1, maxConsumers)

	// Set to 0 explicitly
	err = admin.Topics().SetMaxConsumers(*topicName, 0)
	assert.NoError(t, err)

	// Verify returns 0
	assert.Eventually(
		t,
		func() bool {
			maxConsumers, err = admin.Topics().GetMaxConsumers(*topicName)
			return err == nil && maxConsumers == 0
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Set to positive value
	err = admin.Topics().SetMaxConsumers(*topicName, 20)
	assert.NoError(t, err)

	// Verify returns value
	assert.Eventually(
		t,
		func() bool {
			maxConsumers, err = admin.Topics().GetMaxConsumers(*topicName)
			return err == nil && maxConsumers == 20
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove
	err = admin.Topics().RemoveMaxConsumers(*topicName)
	assert.NoError(t, err)
}

func TestTopics_MaxUnackMessagesPerConsumer(t *testing.T) {
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

	// Get default (should be -1)
	maxUnack, err := admin.Topics().GetMaxUnackMessagesPerConsumer(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, -1, maxUnack)

	// Set to 0 explicitly
	err = admin.Topics().SetMaxUnackMessagesPerConsumer(*topicName, 0)
	assert.NoError(t, err)

	// Verify returns 0
	assert.Eventually(
		t,
		func() bool {
			maxUnack, err = admin.Topics().GetMaxUnackMessagesPerConsumer(*topicName)
			return err == nil && maxUnack == 0
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Set to positive value
	err = admin.Topics().SetMaxUnackMessagesPerConsumer(*topicName, 1000)
	assert.NoError(t, err)

	// Verify returns value
	assert.Eventually(
		t,
		func() bool {
			maxUnack, err = admin.Topics().GetMaxUnackMessagesPerConsumer(*topicName)
			return err == nil && maxUnack == 1000
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove
	err = admin.Topics().RemoveMaxUnackMessagesPerConsumer(*topicName)
	assert.NoError(t, err)
}

func TestTopics_MaxUnackMessagesPerSubscription(t *testing.T) {
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

	// Get default (should be -1)
	maxUnack, err := admin.Topics().GetMaxUnackMessagesPerSubscription(*topicName)
	assert.NoError(t, err)
	assert.Equal(t, -1, maxUnack)

	// Set to 0 explicitly
	err = admin.Topics().SetMaxUnackMessagesPerSubscription(*topicName, 0)
	assert.NoError(t, err)

	// Verify returns 0
	assert.Eventually(
		t,
		func() bool {
			maxUnack, err = admin.Topics().GetMaxUnackMessagesPerSubscription(*topicName)
			return err == nil && maxUnack == 0
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Set to positive value
	err = admin.Topics().SetMaxUnackMessagesPerSubscription(*topicName, 5000)
	assert.NoError(t, err)

	// Verify returns value
	assert.Eventually(
		t,
		func() bool {
			maxUnack, err = admin.Topics().GetMaxUnackMessagesPerSubscription(*topicName)
			return err == nil && maxUnack == 5000
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove
	err = admin.Topics().RemoveMaxUnackMessagesPerSubscription(*topicName)
	assert.NoError(t, err)
}

func TestTopics_Persistence(t *testing.T) {
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

	// Initial state: policy not configured, should return nil
	persistence, err := admin.Topics().GetPersistence(*topicName)
	assert.NoError(t, err)
	assert.Nil(t, persistence, "Expected nil when persistence is not configured")

	// Set new persistence policy
	newPersistence := utils.PersistenceData{
		BookkeeperEnsemble:             1,
		BookkeeperWriteQuorum:          1,
		BookkeeperAckQuorum:            1,
		ManagedLedgerMaxMarkDeleteRate: 10.0,
	}
	err = admin.Topics().SetPersistence(*topicName, newPersistence)
	assert.NoError(t, err)

	// Verify persistence is set
	assert.Eventually(
		t,
		func() bool {
			persistence, err = admin.Topics().GetPersistence(*topicName)
			return err == nil && persistence != nil &&
				persistence.BookkeeperEnsemble == 1 &&
				persistence.BookkeeperWriteQuorum == 1
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove persistence policy - should return nil
	err = admin.Topics().RemovePersistence(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			persistence, err = admin.Topics().GetPersistence(*topicName)
			return err == nil && persistence == nil
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestTopics_DelayedDelivery(t *testing.T) {
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

	// Initial state: policy not configured, should return nil
	delayedDelivery, err := admin.Topics().GetDelayedDelivery(*topicName)
	assert.NoError(t, err)
	assert.Nil(t, delayedDelivery, "Expected nil when delayed delivery is not configured")

	// Set new delayed delivery policy
	newDelayedDelivery := utils.DelayedDeliveryData{
		Active:           true,
		TickTime:         1000,
		MaxDelayInMillis: 60000,
	}
	err = admin.Topics().SetDelayedDelivery(*topicName, newDelayedDelivery)
	assert.NoError(t, err)

	// Verify delayed delivery is set
	assert.Eventually(
		t,
		func() bool {
			delayedDelivery, err = admin.Topics().GetDelayedDelivery(*topicName)
			return err == nil && delayedDelivery != nil &&
				delayedDelivery.Active == true &&
				delayedDelivery.TickTime == 1000
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove delayed delivery policy - should return nil
	err = admin.Topics().RemoveDelayedDelivery(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			delayedDelivery, err = admin.Topics().GetDelayedDelivery(*topicName)
			return err == nil && delayedDelivery == nil
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestTopics_DispatchRate(t *testing.T) {
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

	// Initial state: policy not configured, should return nil
	dispatchRate, err := admin.Topics().GetDispatchRate(*topicName)
	assert.NoError(t, err)
	assert.Nil(t, dispatchRate, "Expected nil when dispatch rate is not configured")

	// Set new dispatch rate
	newDispatchRate := utils.DispatchRateData{
		DispatchThrottlingRateInMsg:  100,
		DispatchThrottlingRateInByte: 1048576,
		RatePeriodInSecond:           60,
		RelativeToPublishRate:        false,
	}
	err = admin.Topics().SetDispatchRate(*topicName, newDispatchRate)
	assert.NoError(t, err)

	// Verify dispatch rate is set
	assert.Eventually(
		t,
		func() bool {
			dispatchRate, err = admin.Topics().GetDispatchRate(*topicName)
			return err == nil && dispatchRate != nil &&
				dispatchRate.DispatchThrottlingRateInMsg == 100
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove dispatch rate policy - should return nil
	err = admin.Topics().RemoveDispatchRate(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			dispatchRate, err = admin.Topics().GetDispatchRate(*topicName)
			return err == nil && dispatchRate == nil
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestTopics_PublishRate(t *testing.T) {
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

	// Initial state: policy not configured, should return nil
	publishRate, err := admin.Topics().GetPublishRate(*topicName)
	assert.NoError(t, err)
	assert.Nil(t, publishRate, "Expected nil when publish rate is not configured")

	// Set new publish rate
	newPublishRate := utils.PublishRateData{
		PublishThrottlingRateInMsg:  200,
		PublishThrottlingRateInByte: 2097152,
	}
	err = admin.Topics().SetPublishRate(*topicName, newPublishRate)
	assert.NoError(t, err)

	// Verify publish rate is set
	assert.Eventually(
		t,
		func() bool {
			publishRate, err = admin.Topics().GetPublishRate(*topicName)
			return err == nil && publishRate != nil &&
				publishRate.PublishThrottlingRateInMsg == 200
		},
		10*time.Second,
		100*time.Millisecond,
	)

	// Remove publish rate policy - should return nil
	err = admin.Topics().RemovePublishRate(*topicName)
	assert.NoError(t, err)
	assert.Eventually(
		t,
		func() bool {
			publishRate, err = admin.Topics().GetPublishRate(*topicName)
			return err == nil && publishRate == nil
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func TestTopicStatsTimestamps(t *testing.T) {
	topic := fmt.Sprintf("persistent://public/default/test-topic-stats-timestamps-%d", time.Now().Nanosecond())
	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)

	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)

	// Create topic
	err = admin.Topics().Create(*topicName, 0)
	assert.NoError(t, err)
	defer admin.Topics().Delete(*topicName, true, true)

	// Get stats and verify CreationTimestamp
	stats, err := admin.Topics().GetStats(*topicName)
	assert.NoError(t, err)
	assert.Greater(t, stats.TopicCreationTimeStamp, int64(0), "CreationTimestamp should be greater than 0")
	// LastPublishTimestamp should be 0 before any message is published
	assert.Equal(t, int64(0), stats.LastPublishTimestamp, "LastPublishTimestamp should be 0 initially")

	// Publish a message
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	assert.NoError(t, err)
	defer producer.Close()

	ctx := context.Background()
	_, err = producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte("test-message"),
	})
	assert.NoError(t, err)

	// Wait for stats to update (stats are updated asynchronously in broker)
	assert.Eventually(t, func() bool {
		s, err := admin.Topics().GetStats(*topicName)
		if err != nil {
			return false
		}
		return s.LastPublishTimestamp > 0
	}, 10*time.Second, 500*time.Millisecond, "LastPublishTimestamp should update after publishing")
}

func TestPartitionedTopicStatsTimestamps(t *testing.T) {
	topic := fmt.Sprintf("persistent://public/default/test-partitioned-topic-stats-timestamps-%d", time.Now().Nanosecond())
	cfg := &config.Config{}
	admin, err := New(cfg)
	assert.NoError(t, err)

	topicName, err := utils.GetTopicName(topic)
	assert.NoError(t, err)

	// Create partitioned topic
	err = admin.Topics().Create(*topicName, 2)
	assert.NoError(t, err)
	defer admin.Topics().Delete(*topicName, true, true)

	// Get partitioned stats and verify CreationTimestamp
	stats, err := admin.Topics().GetPartitionedStats(*topicName, true)
	assert.NoError(t, err)
	assert.Greater(t, stats.TopicCreationTimeStamp, int64(0), "CreationTimestamp should be greater than 0")
	assert.Equal(t, int64(0), stats.LastPublishTimestamp, "LastPublishTimestamp should be 0 initially")

	// Publish a message
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	assert.NoError(t, err)
	defer producer.Close()

	ctx := context.Background()
	_, err = producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: []byte("test-message"),
	})
	assert.NoError(t, err)

	// Wait for stats to update
	assert.Eventually(t, func() bool {
		s, err := admin.Topics().GetPartitionedStats(*topicName, true)
		if err != nil {
			return false
		}
		// Partitioned stats LastPublishTimestamp is usually an aggregation or max of partitions
		return s.LastPublishTimestamp > 0
	}, 10*time.Second, 500*time.Millisecond, "LastPublishTimestamp should update after publishing")
}
