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

package main

import (
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	client, err := pulsar.NewClient("pulsar://localhost:6650")
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	channel := make(chan pulsar.ConsumerMessage, 100)

	consumer, err := client.Subscribe(pulsar.WithSubscriptionName("my-subscription"), pulsar.WithTopics("topic-1"),
		pulsar.WithSubscriptionType(pulsar.Shared),
		pulsar.WithMessageChannel(channel))
	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	// Receive messages from channel. The channel returns a struct which contains message and the consumer from where
	// the message was received. It's not necessary here since we have 1 single consumer, but the channel could be
	// shared across multiple consumers as well
	for cm := range channel {
		msg := cm.Message
		fmt.Printf("Received message  msgId: %v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		if err := consumer.Ack(msg); err != nil {
			log.Fatal(err)
		}
	}
}
