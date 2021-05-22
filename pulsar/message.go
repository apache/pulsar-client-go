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
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// ProducerMessage abstraction used in Pulsar producer
type ProducerMessage struct {
	// Payload for the message
	Payload []byte

	//Value and payload is mutually exclusive, `Value interface{}` for schema message.
	Value interface{}

	// Key sets the key of the message for routing policy
	Key string

	// OrderingKey sets the ordering key of the message
	OrderingKey string

	// Properties attach application defined properties on the message
	Properties map[string]string

	// EventTime set the event time for a given message
	// By default, messages don't have an event time associated, while the publish
	// time will be be always present.
	// Set the event time to a non-zero timestamp to explicitly declare the time
	// that the event "happened", as opposed to when the message is being published.
	EventTime time.Time

	// ReplicationClusters override the replication clusters for this message.
	ReplicationClusters []string

	// SequenceID set the sequence id to assign to the current message
	SequenceID *int64

	// Request to deliver the message only after the specified relative delay.
	// Note: messages are only delivered with delay when a consumer is consuming
	//     through a `SubscriptionType=Shared` subscription. With other subscription
	//     types, the messages will still be delivered immediately.
	DeliverAfter time.Duration

	// Deliver the message only at or after the specified absolute timestamp.
	// Note: messages are only delivered with delay when a consumer is consuming
	//     through a `SubscriptionType=Shared` subscription. With other subscription
	//     types, the messages will still be delivered immediately.
	DeliverAt time.Time
}

// Message abstraction used in Pulsar
type Message interface {
	// Topic get the topic from which this message originated from
	Topic() string

	// ProducerName returns the name of the producer that has published the message.
	ProducerName() string

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
	// If EventTime is 0, it means there isn't any event time associated with this message.
	EventTime() time.Time

	// Key get the key of the message, if any
	Key() string

	// OrderingKey get the ordering key of the message, if any
	OrderingKey() string

	// Get message redelivery count, redelivery count maintain in pulsar broker. When client nack acknowledge messages,
	// broker will dispatch message again with message redelivery count in CommandMessage defined.
	//
	// Message redelivery increases monotonically in a broker, when topic switch ownership to a another broker
	// redelivery count will be recalculated.
	RedeliveryCount() uint32

	// Check whether the message is replicated from other cluster.
	IsReplicated() bool

	// Get name of cluster, from which the message is replicated.
	GetReplicatedFrom() string

	//Get the de-serialized value of the message, according the configured
	GetSchemaValue(v interface{}) error
}

// MessageID identifier for a particular message
type MessageID interface {
	// Serialize the message id into a sequence of bytes that can be stored somewhere else
	Serialize() []byte
	// String the message id represented as a string
	String() string
	// Equals indicates to message IDs are equal
	Equals(other MessageID) bool
}

// DeserializeMessageID reconstruct a MessageID object from its serialized representation
func DeserializeMessageID(data []byte) (MessageID, error) {
	return deserializeMessageID(data)
}

func MessageIDFromParts(ledgerID, entryID int64, batchIdx, partitionIdx int32) MessageID {
	return newMessageID(ledgerID, entryID, batchIdx, partitionIdx)
}

func MessageIDFromString(str string) (MessageID, error) {

	s := strings.Split(str, ":")

	if len(s) < 2 || len(s) > 4 {
		return nil, errors.Errorf("invalid message id string. %s", str)
	}

	ledgerID, err := strconv.ParseInt(s[0], 10, 64)
	if err != nil {
		return nil, errors.Errorf("invalid ledger id. %s", str)
	}

	entryID, err := strconv.ParseInt(s[1], 10, 64)
	if err != nil {
		return nil, errors.Errorf("invalid entry id. %s", str)
	}

	partitionIdx := int32(-1)
	if len(s) > 2 {
		pi, err := strconv.Atoi(s[2])
		if err != nil {
			return nil, errors.Errorf("invalid partition index. %s", str)
		}
		partitionIdx = int32(pi)
	}

	batchIdx := int32(-1)
	if len(s) == 4 {
		bi, err := strconv.Atoi(s[3])
		if err != nil {
			return nil, errors.Errorf("invalid batch index. %s", str)
		}
		batchIdx = int32(bi)
	}
	return newMessageID(ledgerID, entryID, batchIdx, partitionIdx), nil
}

// EarliestMessageID returns a messageID that points to the earliest message available in a topic
func EarliestMessageID() MessageID {
	return newMessageID(-1, -1, -1, -1)
}

// LatestMessageID returns a messageID that points to the latest message
func LatestMessageID() MessageID {
	return newMessageID(math.MaxInt64, math.MaxInt64, -1, -1)
}
