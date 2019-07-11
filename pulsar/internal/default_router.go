//
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
//

package internal

import (
	"math/rand"
	"time"
)

type defaultRouter struct {
	clock            Clock
	shiftIdx         uint32
	maxBatchingDelay time.Duration
	hashFunc         func(string) uint32
}

type Clock func() uint64

func NewSystemClock() Clock {
	return func() uint64 {
		return uint64(time.Now().UnixNano())
	}
}

func NewDefaultRouter(clock Clock, hashFunc func(string) uint32, maxBatchingDelay time.Duration) func(string, uint32) int {
	state := &defaultRouter{
		clock:            clock,
		shiftIdx:         rand.Uint32(),
		maxBatchingDelay: maxBatchingDelay,
		hashFunc:         hashFunc,
	}

	return func(key string, numPartitions uint32) int {
		if numPartitions == 1 {
			// When there are no partitions, don't even bother
			return 0
		}

		if key != "" {
			// When a key is specified, use the hash of that key
			return int(state.hashFunc(key) % numPartitions)
		}

		// If there's no key, we do round-robin across partition, sticking with a given
		// partition for a certain amount of time, to ensure we can have a decent amount
		// of batching of the messages.
		//
		//currentMs / maxBatchingDelayMs + startPtnIdx
		if maxBatchingDelay.Nanoseconds() != 0 {
			n := uint32(state.clock()/uint64(maxBatchingDelay.Nanoseconds())) + state.shiftIdx
			return int(n % numPartitions)
		}
		return 0
	}
}
