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
	"math"
	"time"
)

// ProducerMessage abstraction used in Pulsar producer
type ProducerMessage struct {
	// Payload for the message
	Payload []byte

	// Key sets the key of the message for routing policy
	Key string

	// Properties attach application defined properties on the message
	Properties map[string]string

	// EventTime set the event time for a given message
	EventTime *time.Time

	// ReplicationClusters override the replication clusters for this message.
	ReplicationClusters []string

	// SequenceID set the sequence id to assign to the current message
	SequenceID *int64
}

// Message abstraction used in Pulsar
type Message interface {
	// Topic get the topic from which this message originated from
	Topic() string

	// Properties are application defined key/value pairs that will be attached to the message.
	// Return the properties attached to the message.
	Properties() map[string]string

	// Payload get the payload of the message
	Payload() []byte

	// ID get the unique message ID associated with this message.
	// The message id can be used to univocally refer to a message without having the keep the entire payload in memory.
	ID() MessageID

	// PublishTime get the publish time of this message. The publish time is the timestamp that a client
	// publish the message.
	PublishTime() time.Time

	// EventTime get the event time associated with this message. It is typically set by the applications via
	// `ProducerMessage.EventTime`.
	// If there isn't any event time associated with this event, it will be nil.
	EventTime() time.Time

	// Key get the key of the message, if any
	Key() string
}

// MessageID identifier for a particular message
type MessageID interface {
	// Serialize the message id into a sequence of bytes that can be stored somewhere else
	Serialize() []byte
}

// DeserializeMessageID reconstruct a MessageID object from its serialized representation
func DeserializeMessageID(data []byte) (MessageID, error) {
	return deserializeMessageID(data)
}

// EarliestMessageID returns a messageID that points to the earliest message available in a topic
func EarliestMessageID() MessageID {
	return newMessageID(-1, -1, -1, -1)
}

// LatestMessage returns a messageID that points to the latest message
func LatestMessageID() MessageID {
	return newMessageID(math.MaxInt64, math.MaxInt64, -1, -1)
}
