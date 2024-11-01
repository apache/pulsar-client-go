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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"

	"github.com/stretchr/testify/assert"
)

func TestMultiTopicConsumerReceive(t *testing.T) {
	topic1 := newTopicName()
	topic2 := newTopicName()

	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
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

	// produce messages
	for i, topic := range topics {
		p, err := client.CreateProducer(ProducerOptions{
			Topic:           topic,
			DisableBatching: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = genMessages(p, 10, func(idx int) string {
			return fmt.Sprintf("topic-%d-hello-%d", i+1, idx)
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
		}
	}
	assert.Equal(t, receivedTopic1, receivedTopic2)
}

func TestMultiTopicConsumerUnsubscribe(t *testing.T) {
	topic1 := newTopicName()
	topic2 := newTopicName()

	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
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

	err = consumer.Unsubscribe()
	assert.Nil(t, err)

	err = consumer.Unsubscribe()
	assert.Error(t, err)

}
func TestMultiTopicConsumerForceUnsubscribe(t *testing.T) {
	topic1 := newTopicName()
	topic2 := newTopicName()

	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
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
	err = consumer.UnsubscribeForce()
	assert.Nil(t, err)

	err = consumer.UnsubscribeForce()
	assert.Error(t, err)
}

func TestMultiTopicGetLastMessageIDs(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic1Partition, topic2Partition, topic3Partition := 1, 2, 3

	topic1 := newTopicName()
	err = createPartitionedTopic(topic1, topic1Partition)
	assert.Nil(t, err)

	topic2 := newTopicName()
	err = createPartitionedTopic(topic2, topic2Partition)
	assert.Nil(t, err)

	topic3 := newTopicName()
	err = createPartitionedTopic(topic3, topic3Partition)
	assert.Nil(t, err)

	topics := []string{topic1, topic2, topic3}
	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topics:           topics,
		SubscriptionName: "my-sub",
		Type:             Shared,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	// produce messages
	totalMessage := 30
	for i, topic := range topics {
		p, err := client.CreateProducer(ProducerOptions{
			Topic:           topic,
			DisableBatching: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = genMessages(p, totalMessage, func(idx int) string {
			return fmt.Sprintf("topic-%d-hello-%d", i+1, idx)
		})
		p.Close()
		if err != nil {
			assert.Nil(t, err)
		}
	}

	// create admin
	admin, err := pulsaradmin.NewClient(&config.Config{})
	assert.Nil(t, err)

	topicMessageIDs, err := consumer.GetLastMessageIDs()
	assert.Nil(t, err)
	assert.Equal(t, topic1Partition+topic2Partition+topic3Partition, len(topicMessageIDs))
	for _, id := range topicMessageIDs {
		if strings.Contains(id.Topic(), topic1) {
			assert.Equal(t, int(id.EntryID()), totalMessage/topic1Partition-1)
		} else if strings.Contains(id.Topic(), topic2) {
			assert.Equal(t, int(id.EntryID()), totalMessage/topic2Partition-1)
		} else if strings.Contains(id.Topic(), topic3) {
			assert.Equal(t, int(id.EntryID()), totalMessage/topic3Partition-1)
		}

		topicName, err := utils.GetTopicName(id.Topic())
		assert.Nil(t, err)
		messages, err := admin.Subscriptions().GetMessagesByID(*topicName, id.LedgerID(), id.EntryID())
		assert.Nil(t, err)
		assert.Equal(t, 1, len(messages))

	}

}

func TestMultiTopicAckIDList(t *testing.T) {
	for _, params := range []bool{true, false} {
		t.Run(fmt.Sprintf("TestMultiTopicConsumerAckIDList%v", params), func(t *testing.T) {
			runMultiTopicAckIDList(t, params)
		})
	}
}

func runMultiTopicAckIDList(t *testing.T, regex bool) {
	topicPrefix := fmt.Sprintf("multiTopicAckIDList%v", time.Now().UnixNano())
	topic1 := "persistent://public/default/" + topicPrefix + "1"
	topic2 := "persistent://public/default/" + topicPrefix + "2"

	client, err := NewClient(ClientOptions{URL: "pulsar://localhost:6650"})
	assert.Nil(t, err)
	defer client.Close()

	if regex {
		admin, err := pulsaradmin.NewClient(&config.Config{})
		assert.Nil(t, err)
		for _, topic := range []string{topic1, topic2} {
			topicName, err := utils.GetTopicName(topic)
			assert.Nil(t, err)
			admin.Topics().Create(*topicName, 0)
		}
	}

	createConsumer := func() Consumer {
		options := ConsumerOptions{
			SubscriptionName: "sub",
			Type:             Shared,
			AckWithResponse:  true,
		}
		if regex {
			options.TopicsPattern = topicPrefix + ".*"
		} else {
			options.Topics = []string{topic1, topic2}
		}
		consumer, err := client.Subscribe(options)
		assert.Nil(t, err)
		return consumer
	}
	consumer := createConsumer()

	sendMessages(t, client, topic1, 0, 3, false)
	sendMessages(t, client, topic2, 0, 2, false)

	receiveMessageMap := func(consumer Consumer, numMessages int) map[string][]Message {
		msgs := receiveMessages(t, consumer, numMessages)
		topicToMsgs := make(map[string][]Message)
		for _, msg := range msgs {
			topicToMsgs[msg.Topic()] = append(topicToMsgs[msg.Topic()], msg)
		}
		return topicToMsgs
	}

	topicToMsgs := receiveMessageMap(consumer, 5)
	assert.Equal(t, 3, len(topicToMsgs[topic1]))
	for i := 0; i < 3; i++ {
		assert.Equal(t, fmt.Sprintf("msg-%d", i), string(topicToMsgs[topic1][i].Payload()))
	}
	assert.Equal(t, 2, len(topicToMsgs[topic2]))
	for i := 0; i < 2; i++ {
		assert.Equal(t, fmt.Sprintf("msg-%d", i), string(topicToMsgs[topic2][i].Payload()))
	}

	assert.Nil(t, consumer.AckIDList([]MessageID{
		topicToMsgs[topic1][0].ID(),
		topicToMsgs[topic1][2].ID(),
		topicToMsgs[topic2][1].ID(),
	}))

	consumer.Close()
	consumer = createConsumer()
	topicToMsgs = receiveMessageMap(consumer, 2)
	assert.Equal(t, 1, len(topicToMsgs[topic1]))
	assert.Equal(t, "msg-1", string(topicToMsgs[topic1][0].Payload()))
	assert.Equal(t, 1, len(topicToMsgs[topic2]))
	assert.Equal(t, "msg-0", string(topicToMsgs[topic2][0].Payload()))
	consumer.Close()

	msgID0 := topicToMsgs[topic1][0].ID()
	err = consumer.AckIDList([]MessageID{msgID0})
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "consumer state is closed")

	msgID1 := topicToMsgs[topic2][0].ID()
	msgIDs := []MessageID{msgID0, msgID1}
	if ackError, ok := consumer.AckIDList(msgIDs).(AckError); ok {
		assert.Equal(t, 2, len(ackError))
		for _, msgID := range msgIDs {
			assert.Equal(t, "consumer state is closed", ackError[msgID].Error())
			assert.True(t, strings.Contains(ackError.Error(), "consumer state is closed"))
			assert.True(t, strings.Contains(ackError.Error(), msgID.String()))
		}
	} else {
		assert.Fail(t, "AckIDList should return AckError")
	}
}
