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
	"time"
)

// Pair of a Consumer and Message
type ConsumerMessage struct {
	Consumer
	Message
}

// SubscriptionType of subscription supported by Pulsar
type SubscriptionType int

const (
	// Exclusive there can be only 1 consumer on the same topic with the same subscription name
	Exclusive SubscriptionType = iota

	// Shared subscription mode, multiple consumer will be able to use the same subscription name
	// and the messages will be dispatched according to
	// a round-robin rotation between the connected consumers
	Shared

	// Failover subscription mode, multiple consumer will be able to use the same subscription name
	// but only 1 consumer will receive the messages.
	// If that consumer disconnects, one of the other connected consumers will start receiving messages.
	Failover

	// KeyShared subscription mode, multiple consumer will be able to use the same subscription and all messages with the same key
	// will be dispatched to only one consumer
	KeyShared
)

type SubscriptionInitialPosition int

const (
	// Latest position which means the start consuming position will be the last message
	SubscriptionPositionLatest SubscriptionInitialPosition = iota

	// Earliest position which means the start consuming position will be the first message
	SubscriptionPositionEarliest
)

// ConsumerOptions is used to configure and create instances of Consumer
type ConsumerOptions struct {
	// Specify the topic this consumer will subscribe on.
	// Either a topic, a list of topics or a topics pattern are required when subscribing
	Topic string

	// Specify a list of topics this consumer will subscribe on.
	// Either a topic, a list of topics or a topics pattern are required when subscribing
	Topics []string

	// Specify a regular expression to subscribe to multiple topics under the same namespace.
	// Either a topic, a list of topics or a topics pattern are required when subscribing
	TopicsPattern string

	// Specify the interval in which to poll for new partitions or new topics if using a TopicsPattern.
	AutoDiscoveryPeriod time.Duration

	// Specify the subscription name for this consumer
	// This argument is required when subscribing
	SubscriptionName string

	// Attach a set of application defined properties to the consumer
	// This properties will be visible in the topic stats
	Properties map[string]string

	// Select the subscription type to be used when subscribing to the topic.
	// Default is `Exclusive`
	Type SubscriptionType

	// InitialPosition at which the cursor will be set when subscribe
	// Default is `Latest`
	SubscriptionInitialPosition

	// Sets a `MessageChannel` for the consumer
	// When a message is received, it will be pushed to the channel for consumption
	MessageChannel chan ConsumerMessage

	// Sets the size of the consumer receive queue.
	// The consumer receive queue controls how many messages can be accumulated by the `Consumer` before the
	// application calls `Consumer.receive()`. Using a higher value could potentially increase the consumer
	// throughput at the expense of bigger memory utilization.
	// Default value is `1000` messages and should be good for most use cases.
	// Set to -1 to disable prefetching in consumer
	ReceiverQueueSize int

	// Set the max total receiver queue size across partitions.
	// This setting will be used to reduce the receiver queue size for individual partitions
	// ReceiverQueueSize(int) if the total exceeds this value (default: 50000).
	MaxTotalReceiverQueueSizeAcrossPartitions int

	// The delay after which to redeliver the messages that failed to be
	// processed. Default is 1min. (See `Consumer.Nack()`)
	NackRedeliveryDelay time.Duration

	// Set the consumer name.
	Name string

	// If enabled, the consumer will read messages from the compacted topic rather than reading the full message backlog
	// of the topic. This means that, if the topic has been compacted, the consumer will only see the latest value for
	// each key in the topic, up until the point in the topic message backlog that has been compacted. Beyond that
	// point, the messages will be sent as normal.
	//
	// ReadCompacted can only be enabled subscriptions to persistent topics, which have a single active consumer (i.e.
	//  failure or exclusive subscriptions). Attempting to enable it on subscriptions to a non-persistent topics or on a
	//  shared subscription, will lead to the subscription call throwing a PulsarClientException.
	ReadCompacted bool
}

// Consumer is an interface that abstracts behavior of Pulsar's consumer
type Consumer interface {
	// Subscription get a subscription for the consumer
	Subscription() string

	// Unsubscribe the consumer
	Unsubscribe() error

	// Receive a single message.
	// This calls blocks until a message is available.
	Receive(context.Context) (Message, error)

	// Chan returns a channel to consume messages from
	Chan() <-chan ConsumerMessage

	// Ack the consumption of a single message
	Ack(Message)

	// AckID the consumption of a single message, identified by its MessageID
	AckID(MessageID)

	// Acknowledge the failure to process a single message.
	//
	// When a message is "negatively acked" it will be marked for redelivery after
	// some fixed delay. The delay is configurable when constructing the consumer
	// with ConsumerOptions.NAckRedeliveryDelay .
	//
	// This call is not blocking.
	Nack(Message)

	// Acknowledge the failure to process a single message.
	//
	// When a message is "negatively acked" it will be marked for redelivery after
	// some fixed delay. The delay is configurable when constructing the consumer
	// with ConsumerOptions.NackRedeliveryDelay .
	//
	// This call is not blocking.
	NackID(MessageID)

	// Close the consumer and stop the broker to push more messages
	Close()
}
