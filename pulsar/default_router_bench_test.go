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
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
)

var (
	targetPartition int
)

func BenchmarkDefaultRouter(b *testing.B) {
	const numPartitions = uint32(200)
	msg := &ProducerMessage{
		Payload: []byte("message 1"),
	}
	router := newBenchDefaultRouter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		targetPartition = router(msg, numPartitions)
	}
}

func BenchmarkDefaultRouterParallel(b *testing.B) {
	const numPartitions = uint32(200)
	msg := &ProducerMessage{
		Payload: []byte("message 1"),
	}
	router := newBenchDefaultRouter()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			targetPartition = router(msg, numPartitions)
		}
	})
}

func newBenchDefaultRouter() func(*ProducerMessage, uint32) int {
	const (
		maxBatchingMessages = 2000
		maxBatchingSize     = 524288
		maxBatchingDelay    = 100 * time.Millisecond
	)
	return NewDefaultRouter(internal.JavaStringHash, maxBatchingMessages, maxBatchingSize, maxBatchingDelay, false)
}
