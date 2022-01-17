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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type defaultRouter struct {
	currentPartitionCursor uint32

	lastChangeTimestamp int64
	msgCounter          uint32
	cumulativeBatchSize uint32
	sync.RWMutex
}

// NewDefaultRouter set the message routing mode for the partitioned producer.
// Default routing mode is round-robin routing if no partition key is specified.
// If the batching is enabled, it honors the different thresholds for batching i.e. maximum batch size,
// maximum number of messages, maximum delay to publish a batch. When one of the threshold is reached the next partition
// is used.
func NewDefaultRouter(
	hashFunc func(string) uint32,
	maxBatchingMessages uint,
	maxBatchingSize uint,
	maxBatchingDelay time.Duration,
	disableBatching bool) func(*ProducerMessage, uint32) int {
	state := &defaultRouter{
		currentPartitionCursor: rand.Uint32(),
		lastChangeTimestamp:    math.MinInt64,
	}

	readClockAfterNumMessages := uint32(maxBatchingMessages / 10)
	return func(message *ProducerMessage, numPartitions uint32) int {
		if numPartitions == 1 {
			// When there are no partitions, don't even bother
			return 0
		}

		if len(message.OrderingKey) != 0 {
			// When an OrderingKey is specified, use the hash of that key
			return int(hashFunc(message.OrderingKey) % numPartitions)
		}

		if len(message.Key) != 0 {
			// When a key is specified, use the hash of that key
			return int(hashFunc(message.Key) % numPartitions)
		}

		// If there's no key, we do round-robin across partition. If no batching go to next partition.
		if disableBatching {
			p := int(state.currentPartitionCursor % numPartitions)
			atomic.AddUint32(&state.currentPartitionCursor, 1)
			return p
		}

		// If there's no key, we do round-robin across partition, sticking with a given
		// partition for a certain amount of messages or volume buffered or the max delay to batch is reached so that
		// we ensure having a decent amount of batching of the messages.
		// Use critical section to protect a group of counters and increment the currentPartitionCursor
		var now int64
		batchReached := false
		size := uint32(len(message.Payload))
		state.Lock()
		defer state.Unlock()

		if state.msgCounter >= uint32(maxBatchingMessages-1) {
			batchReached = true
		} else if size >= (uint32(maxBatchingSize) - state.cumulativeBatchSize) {
			batchReached = true
		} else if readClockAfterNumMessages == 0 || state.msgCounter%readClockAfterNumMessages == 0 {
			now = time.Now().UnixNano()
			batchReached = now-state.lastChangeTimestamp >= maxBatchingDelay.Nanoseconds()
		}
		if batchReached {
			// only the current partition cursor when the current batch is ready to flush
			// so that we move to another partition for the next batch
			state.currentPartitionCursor++
			state.msgCounter = 0
			state.cumulativeBatchSize = 0
			if now != 0 {
				state.lastChangeTimestamp = now
			}
			return int(state.currentPartitionCursor % numPartitions)
		}

		state.msgCounter++
		state.cumulativeBatchSize += size
		if now != 0 {
			state.lastChangeTimestamp = now
		}
		return int(state.currentPartitionCursor % numPartitions)
	}
}
