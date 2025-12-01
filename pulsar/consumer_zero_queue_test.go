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
	"log"
	"log/slog"
	"os"
	"testing"
	"time"

	plog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestRetryEnableZeroQueueConsumer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()

	// create consumer
	_, err = client.Subscribe(ConsumerOptions{
		Topic:                   topic,
		SubscriptionName:        "my-sub",
		RetryEnable:             true,
		EnableZeroQueueConsumer: true,
	})
	assert.ErrorContains(t, err, "ZeroQueueConsumer is not supported with RetryEnable")
}

func TestNormalZeroQueueConsumer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topic,
		SubscriptionName:        "my-sub",
		EnableZeroQueueConsumer: true,
	})
	assert.Nil(t, err)
	_, ok := consumer.(*zeroQueueConsumer)
	assert.True(t, ok)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		msg, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Key:     "pulsar",
			Properties: map[string]string{
				"key-1": "pulsar-1",
			},
		})
		assert.Nil(t, err)
		log.Printf("send message: %s", msg.String())
	}

	// receive 10 messages
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		expectMsg := fmt.Sprintf("hello-%d", i)
		expectProperties := map[string]string{
			"key-1": "pulsar-1",
		}
		assert.Equal(t, []byte(expectMsg), msg.Payload())
		assert.Equal(t, "pulsar", msg.Key())
		assert.Equal(t, expectProperties, msg.Properties())
		// ack message
		err = consumer.Ack(msg)
		assert.Nil(t, err)
		log.Printf("receive message: %s", msg.ID().String())
	}
	err = consumer.Unsubscribe()
	assert.Nil(t, err)
}
func TestReconnectConsumer(t *testing.T) {

	req := testcontainers.ContainerRequest{
		Name:         "pulsar-test",
		Image:        getPulsarTestImage(),
		ExposedPorts: []string{"6650/tcp", "8080/tcp"},
		WaitingFor:   wait.ForExposedPort(),
		HostConfigModifier: func(config *container.HostConfig) {
			config.PortBindings = map[nat.Port][]nat.PortBinding{
				"6650/tcp": {{HostIP: "0.0.0.0", HostPort: "6659"}},
				"8080/tcp": {{HostIP: "0.0.0.0", HostPort: "8089"}},
			}
		},
		Cmd: []string{"bin/pulsar", "standalone", "-nfw"},
	}
	c, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            true,
	})
	require.NoError(t, err, "Failed to start the pulsar container")
	endpoint, err := c.PortEndpoint(context.Background(), "6650", "pulsar")
	require.NoError(t, err, "Failed to get the pulsar endpoint")

	client, err := NewClient(ClientOptions{
		URL: endpoint,
	})
	assert.Nil(t, err)
	adminEndpoint, err := c.PortEndpoint(context.Background(), "8080", "http")
	assert.Nil(t, err)
	admin, err := pulsaradmin.NewClient(&config.Config{
		WebServiceURL: adminEndpoint,
	})
	assert.Nil(t, err)

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()
	var consumer Consumer
	require.Eventually(t, func() bool {
		consumer, err = client.Subscribe(ConsumerOptions{
			Topic:                   topic,
			SubscriptionName:        "my-sub",
			EnableZeroQueueConsumer: true,
		})
		return err == nil
	}, 30*time.Second, 1*time.Second)

	assert.Nil(t, err)
	_, ok := consumer.(*zeroQueueConsumer)
	assert.True(t, ok)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)

	// send 10 messages
	for i := 0; i < 10; i++ {
		msg, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Key:     "pulsar",
			Properties: map[string]string{
				"key-1": "pulsar-1",
			},
		})
		assert.Nil(t, err)
		log.Printf("send message: %s", msg.String())
	}

	ch := make(chan struct{})

	go func() {
		time.Sleep(3 * time.Second)
		log.Println("unloading topic")
		topicName, err := utils.GetTopicName(topic)
		assert.Nil(t, err)
		// unload topic to trigger consumer reconnect
		err = admin.Topics().Unload(*topicName)
		assert.Nil(t, err)
		log.Println("unloaded topic")
		ch <- struct{}{}
	}()

	// receive 10 messages
	for i := 0; i < 10; i++ {
		if i == 3 {
			<-ch
		}
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		expectMsg := fmt.Sprintf("hello-%d", i)
		expectProperties := map[string]string{
			"key-1": "pulsar-1",
		}
		assert.Equal(t, []byte(expectMsg), msg.Payload())
		assert.Equal(t, "pulsar", msg.Key())
		assert.Equal(t, expectProperties, msg.Properties())
		// ack message
		err = consumer.Ack(msg)
		assert.Nil(t, err)
		log.Printf("receive message: %s", msg.ID().String())
	}
	err = consumer.Unsubscribe()
	assert.Nil(t, err)
	consumer.Close()
	producer.Close()
	defer c.Terminate(ctx)
}

func TestReconnectedBrokerSendPermits(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	topic := newTopicName()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topic,
		SubscriptionName:        "my-sub",
		EnableZeroQueueConsumer: true,
		Type:                    Shared, // using Shared subscription type to support unack subscription stats
	})
	assert.Nil(t, err)
	ctx := context.Background()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)

	// send 10 messages
	for i := 0; i < 10; i++ {
		msg, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Key:     "pulsar",
			Properties: map[string]string{
				"key-1": "pulsar-1",
			},
		})
		assert.Nil(t, err)
		log.Printf("send message: %s", msg.String())
	}

	admin, err := pulsaradmin.NewClient(&config.Config{})
	assert.Nil(t, err)
	log.Println("unloading topic")
	topicName, err := utils.GetTopicName(topic)
	assert.Nil(t, err)
	err = admin.Topics().Unload(*topicName)
	assert.Nil(t, err)
	log.Println("unloaded topic")
	time.Sleep(5 * time.Second) // wait for topic unload finish

	// receive 10 messages
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			assert.Nil(t, err)
		}

		expectMsg := fmt.Sprintf("hello-%d", i)
		expectProperties := map[string]string{
			"key-1": "pulsar-1",
		}
		assert.Equal(t, []byte(expectMsg), msg.Payload())
		assert.Equal(t, "pulsar", msg.Key())
		assert.Equal(t, expectProperties, msg.Properties())
		// ack message
		err = consumer.Ack(msg)
		assert.Nil(t, err)
		log.Printf("receive message: %s", msg.ID().String())
	}
	//	send one more message and we do not manually receive it
	_, err = producer.Send(ctx, &ProducerMessage{
		Payload: []byte(fmt.Sprintf("hello-%d", 10)),
		Key:     "pulsar",
		Properties: map[string]string{
			"key-1": "pulsar-1",
		},
	})
	assert.Nil(t, err)
	//	wait for broker send messages to consumer and topic stats update finish
	time.Sleep(5 * time.Second)
	topicStats, err := admin.Topics().GetStats(*topicName)
	assert.Nil(t, err)
	for _, subscriptionStats := range topicStats.Subscriptions {
		assert.Equal(t, subscriptionStats.MsgBacklog, int64(1))
		assert.Equal(t, subscriptionStats.Consumers[0].UnAckedMessages, 0)
	}

	// ack
	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = consumer.Ack(msg)
	assert.Nil(t, err)

	// check topic stats
	time.Sleep(5 * time.Second)
	topicStats, err = admin.Topics().GetStats(*topicName)
	assert.Nil(t, err)
	for _, subscriptionStats := range topicStats.Subscriptions {
		assert.Equal(t, subscriptionStats.MsgBacklog, int64(0))
		assert.Equal(t, subscriptionStats.Consumers[0].UnAckedMessages, 0)
	}

}

func TestUnloadTopicBeforeConsume(t *testing.T) {

	sLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client, err := NewClient(ClientOptions{
		URL:    lookupURL,
		Logger: plog.NewLoggerWithSlog(sLogger),
	})
	assert.Nil(t, err)
	admin, err := pulsaradmin.NewClient(&config.Config{})
	assert.Nil(t, err)

	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topic,
		SubscriptionName:        "my-sub",
		EnableZeroQueueConsumer: true,
	})

	assert.Nil(t, err)
	_, ok := consumer.(*zeroQueueConsumer)
	assert.True(t, ok)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)

	// send 10 messages
	for i := 0; i < 10; i++ {
		msg, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Key:     "pulsar",
			Properties: map[string]string{
				"key-1": "pulsar-1",
			},
		})
		assert.Nil(t, err)
		log.Printf("send message: %s", msg.String())
	}

	log.Println("unloading topic")
	topicName, err := utils.GetTopicName(topic)
	assert.Nil(t, err)
	// unload topic to trigger consumer reconnect and send permits
	err = admin.Topics().Unload(*topicName)
	assert.Nil(t, err)
	log.Println("unloaded topic")

	// receive 10 messages
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		expectMsg := fmt.Sprintf("hello-%d", i)
		expectProperties := map[string]string{
			"key-1": "pulsar-1",
		}
		assert.Equal(t, []byte(expectMsg), msg.Payload())
		assert.Equal(t, "pulsar", msg.Key())
		assert.Equal(t, expectProperties, msg.Properties())
		// ack message
		err = consumer.Ack(msg)
		assert.Nil(t, err)
		log.Printf("receive message: %s", msg.ID().String())
	}
	// Make sure there are no more messages
	timeout, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = consumer.Receive(timeout)
	assert.Equal(t, context.DeadlineExceeded, err)

	err = consumer.Unsubscribe()
	assert.Nil(t, err)
	consumer.Close()
	producer.Close()
}

func TestMultipleConsumer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create consumer1
	consumer1, err := client.Subscribe(ConsumerOptions{
		Topic:                   topic,
		SubscriptionName:        "my-sub",
		Type:                    Shared,
		EnableZeroQueueConsumer: true,
	})
	assert.Nil(t, err)
	_, ok := consumer1.(*zeroQueueConsumer)
	assert.True(t, ok)
	defer consumer1.Close()

	// create consumer2
	consumer2, err := client.Subscribe(ConsumerOptions{
		Topic:                   topic,
		SubscriptionName:        "my-sub",
		Type:                    Shared,
		EnableZeroQueueConsumer: true,
	})
	assert.Nil(t, err)
	_, ok = consumer2.(*zeroQueueConsumer)
	assert.True(t, ok)
	defer consumer2.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	sendNum := 10
	// send 10 messages
	for i := 0; i < sendNum; i++ {
		msg, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Key:     "pulsar",
			Properties: map[string]string{
				"key-1": "pulsar-1",
			},
		})
		assert.Nil(t, err)
		log.Printf("send message: %s", msg.String())
	}

	// receive messages
	for i := 0; i < sendNum/2; i++ {
		msg, err := consumer1.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("consumer1 receive message: %s %s", msg.ID().String(), msg.Payload())
		// ack message
		consumer1.Ack(msg)
	}

	// receive messages
	for i := 0; i < sendNum/2; i++ {
		msg, err := consumer2.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("consumer2 receive message: %s %s", msg.ID().String(), msg.Payload())
		// ack message
		consumer2.Ack(msg)
	}

}

func TestPartitionZeroQueueConsumer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	err = createPartitionedTopic(topic, 2)
	assert.Nil(t, err)

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topic,
		SubscriptionName:        "my-sub",
		EnableZeroQueueConsumer: true,
	})
	assert.Nil(t, consumer)
	assert.Error(t, err, "ZeroQueueConsumer is not supported for partitioned topics")
}
func TestSpecifiedPartitionZeroQueueConsumer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()
	err = createPartitionedTopic(topic, 2)
	assert.Nil(t, err)
	topics, err := client.TopicPartitions(topic)
	assert.Nil(t, err)

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topics[1],
		SubscriptionName:        "my-sub",
		EnableZeroQueueConsumer: true,
	})
	assert.Nil(t, err)
	_, ok := consumer.(*zeroQueueConsumer)
	assert.True(t, ok)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topics[1],
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		msg, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
			Key:     "pulsar",
			Properties: map[string]string{
				"key-1": "pulsar-1",
			},
		})
		assert.Nil(t, err)
		log.Printf("send message: %s", msg.String())
	}

	// receive 10 messages
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		expectMsg := fmt.Sprintf("hello-%d", i)
		expectProperties := map[string]string{
			"key-1": "pulsar-1",
		}
		assert.Equal(t, []byte(expectMsg), msg.Payload())
		assert.Equal(t, "pulsar", msg.Key())
		assert.Equal(t, expectProperties, msg.Properties())
		// ack message
		err = consumer.Ack(msg)
		assert.Nil(t, err)
		log.Printf("receive message: %s", msg.ID().String())
	}
	err = consumer.Unsubscribe()
	assert.Nil(t, err)
}

func TestZeroQueueConsumerGetLastMessageIDs(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	partition := 1
	topic := newTopicName()
	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topic,
		SubscriptionName:        "my-sub",
		Type:                    Shared,
		EnableZeroQueueConsumer: true,
	})
	_, ok := consumer.(*zeroQueueConsumer)
	assert.True(t, ok)
	assert.Nil(t, err)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()
	// send messages
	totalMessage := 10
	for i := 0; i < totalMessage; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			assert.Nil(t, err)
		}
	}

	// create admin
	admin, err := pulsaradmin.NewClient(&config.Config{})
	assert.Nil(t, err)

	messageIDs, err := consumer.GetLastMessageIDs()
	assert.Nil(t, err)
	assert.Equal(t, partition, len(messageIDs))

	id := messageIDs[0]
	topicName, err := utils.GetTopicName(id.Topic())
	assert.Nil(t, err)
	messages, err := admin.Subscriptions().GetMessagesByID(*topicName, id.LedgerID(), id.EntryID())
	assert.Nil(t, err)
	assert.Equal(t, 1, len(messages))
	err = consumer.UnsubscribeForce()
	assert.Nil(t, err)

}

func TestZeroQueueConsumer_Chan(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topic,
		SubscriptionName:        "my-sub",
		EnableZeroQueueConsumer: true,
	})
	assert.Nil(t, err)
	_, ok := consumer.(*zeroQueueConsumer)
	assert.True(t, ok)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		msg, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)
		log.Printf("send message: %s", msg.String())
	}
	assertPanic(t, "zeroQueueConsumer cannot support Chan method", func() {
		consumer.Chan()
	})
}

func assertPanic(t *testing.T, panicValue interface{}, f func()) {
	defer func() {
		if r := recover(); r != panicValue {
			t.Errorf("Expected panic %v, but got %v", panicValue, r)
		}
	}()
	f()
}

func TestZeroQueueConsumer_AckCumulativeConsumer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create consumer
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topic,
		SubscriptionName:        "my-sub",
		EnableZeroQueueConsumer: true,
	})
	assert.Nil(t, err)
	_, ok := consumer.(*zeroQueueConsumer)
	assert.True(t, ok)
	defer consumer.Close()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		msg, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)
		log.Printf("send message: %s", msg.String())
	}

	var lastID MessageID
	// receive 10 messages
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		lastID = msg.ID()
		if err != nil {
			log.Fatal(err)
		}
		expectMsg := fmt.Sprintf("hello-%d", i)
		assert.Equal(t, []byte(expectMsg), msg.Payload())
	}
	err = consumer.AckIDCumulative(lastID)
	assert.Nil(t, err)

}

func TestZeroQueueConsumer_Nack(t *testing.T) {
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

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topicName,
		SubscriptionName:        "sub-1",
		Type:                    Shared,
		NackRedeliveryDelay:     1 * time.Second,
		EnableZeroQueueConsumer: true,
	})
	assert.Nil(t, err)
	_, ok := consumer.(*zeroQueueConsumer)
	assert.True(t, ok)
	defer consumer.Close()

	const N = 100

	for i := 0; i < N; i++ {
		if _, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))

		if i%2 == 0 {
			// Only acks even messages
			err = consumer.Ack(msg)
			assert.Nil(t, err)
		} else {
			// Fails to process odd messages
			consumer.Nack(msg)
		}
	}

	// Failed messages should be resent

	// We should only receive the odd messages
	for i := 1; i < N; i += 2 {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("msg-content-%d", i), string(msg.Payload()))

		err = consumer.Ack(msg)
		assert.Nil(t, err)
	}
}

func TestZeroQueueConsumer_Seek(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topicName := newTopicName()
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topicName,
		EnableZeroQueueConsumer: true,
		SubscriptionName:        "sub-1",
		StartMessageIDInclusive: true,
	})
	assert.Nil(t, err)
	_, ok := consumer.(*zeroQueueConsumer)
	assert.True(t, ok)
	defer consumer.Close()

	const N = 10
	var seekID MessageID
	for i := 0; i < N; i++ {
		id, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)

		if i == N-5 {
			seekID = id
		}
	}

	// Don't consume all messages so some stay in queues
	for i := 0; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("hello-%d", i), string(msg.Payload()))
		err = consumer.Ack(msg)
		assert.Nil(t, err)
	}

	err = consumer.Seek(seekID)
	assert.Nil(t, err)

	time.Sleep(time.Second * 3)
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	log.Printf("msg: %s", string(msg.Payload()))
	assert.Equal(t, fmt.Sprintf("hello-%d", N-5), string(msg.Payload()))
}

func TestZeroQueueConsumer_SeekByTime(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	topicName := newTopicName()
	ctx := context.Background()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topicName,
		DisableBatching: false,
	})
	assert.Nil(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                   topicName,
		EnableZeroQueueConsumer: true,
		SubscriptionName:        "my-sub",
	})
	assert.Nil(t, err)
	_, ok := consumer.(*zeroQueueConsumer)
	assert.True(t, ok)
	defer consumer.Close()

	const N = 10
	resetTimeStr := "100s"
	retentionTimeInSecond, err := internal.ParseRelativeTimeInSeconds(resetTimeStr)
	assert.Nil(t, err)

	for i := 0; i < N; i++ {
		_, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		assert.Nil(t, err)
	}

	// Don't consume all messages so some stay in queues
	for i := 0; i < N-2; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("hello-%d", i), string(msg.Payload()))
		err = consumer.Ack(msg)
		assert.Nil(t, err)
	}

	currentTimestamp := time.Now()
	err = consumer.SeekByTime(currentTimestamp.Add(-retentionTimeInSecond))
	assert.Nil(t, err)

	time.Sleep(time.Second * 3)

	for i := 0; i < N; i++ {
		msg, err := consumer.Receive(ctx)
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("hello-%d", i), string(msg.Payload()))
		err = consumer.Ack(msg)
		assert.Nil(t, err)
	}
}
