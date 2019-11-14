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
	"strings"
	"testing"

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
		if err := produceHelloMessages(client, topic, 5, fmt.Sprintf("topic-%d", i+1)); err != nil {
			t.Fatal(err)
		}
	}

	receivedTopic1 := 0
	receivedTopic2 := 0
	for receivedTopic1+receivedTopic2 < 10 {
		select {
		case cm := <-consumer.Chan():
			msg := string(cm.Payload())
			if strings.HasPrefix(msg, "topic-1") {
				receivedTopic1++
			} else if strings.HasPrefix(msg, "topic-2") {
				receivedTopic2++
			}
			consumer.Ack(cm.Message)
		}
	}
	assert.Equal(t, receivedTopic1, receivedTopic2)
}

func produceHelloMessages(client Client, topic string, numMessages int, prefix string) error {
	p, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	if err != nil {
		return err
	}
	defer p.Close()
	ctx := context.Background()
	for i := 0; i < numMessages; i++ {
		m := &ProducerMessage{
			Payload: []byte(fmt.Sprintf("%s-hello-%d", prefix, i)),
		}
		if err := p.Send(ctx, m); err != nil {
			return err
		}
	}

	return nil
}
