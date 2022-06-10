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

	"github.com/skulkarni-ns/pulsar-client-go/pulsar/internal"
	"github.com/stretchr/testify/assert"
)

const oneHourPublishMaxDelay = time.Hour

func TestDefaultRouterRoutingBecauseBatchingDisabled(t *testing.T) {
	router := NewDefaultRouter(internal.JavaStringHash, 20, 100, oneHourPublishMaxDelay, true)
	const numPartitions = uint32(3)
	p1 := router(&ProducerMessage{
		Payload: []byte("message 1"),
	}, numPartitions)
	assert.LessOrEqual(t, p1, int(numPartitions))

	p2 := router(&ProducerMessage{
		Payload: []byte("message 2"),
	}, numPartitions)
	if p1 == int(numPartitions-1) {
		assert.Equal(t, 0, p2)
	} else {
		assert.Equal(t, p1+1, p2)
	}
}

func TestDefaultRouterRoutingBecauseMaxPublishDelayReached(t *testing.T) {
	maxPublishDelay := time.Nanosecond * 10
	router := NewDefaultRouter(internal.JavaStringHash, 10, 100, maxPublishDelay, false)
	const numPartitions = uint32(3)
	p1 := router(&ProducerMessage{
		Payload: []byte("message 1"),
	}, 3)
	assert.LessOrEqual(t, p1, int(numPartitions))

	time.Sleep(maxPublishDelay)

	p2 := router(&ProducerMessage{
		Payload: []byte("message 2"),
	}, numPartitions)
	if p1 == int(numPartitions-1) {
		assert.Equal(t, 0, p2)
	} else {
		assert.Equal(t, p1+1, p2)
	}
}

func TestDefaultRouterRoutingBecauseMaxNumberOfMessagesReached(t *testing.T) {
	router := NewDefaultRouter(internal.JavaStringHash, 2, 100, oneHourPublishMaxDelay, false)
	const numPartitions = uint32(3)
	p1 := router(&ProducerMessage{
		Payload: []byte("message 1"),
	}, numPartitions)
	assert.LessOrEqual(t, p1, int(numPartitions))

	p2 := router(&ProducerMessage{
		Payload: []byte("message 2"),
	}, numPartitions)
	assert.Equal(t, p1, p2)

	p3 := router(&ProducerMessage{
		Payload: []byte("message 3"),
	}, numPartitions)
	if p2 == int(numPartitions-1) {
		assert.Equal(t, 0, p3)
	} else {
		assert.Equal(t, p2+1, p3)
	}
}

func TestDefaultRouterRoutingBecauseMaxVolumeReached(t *testing.T) {
	router := NewDefaultRouter(internal.JavaStringHash, 10, 10, oneHourPublishMaxDelay, false)
	const numPartitions = uint32(3)
	p1 := router(&ProducerMessage{
		Payload: []byte("message 1"),
	}, 3)
	assert.LessOrEqual(t, p1, int(numPartitions))

	p2 := router(&ProducerMessage{
		Payload: []byte("message 2"),
	}, numPartitions)
	if p1 == int(numPartitions-1) {
		assert.Equal(t, 0, p2)
	} else {
		assert.Equal(t, p1+1, p2)
	}
}

func TestDefaultRouterNoRoutingBecausePartitionKeyIsSpecified(t *testing.T) {
	router := NewDefaultRouter(internal.JavaStringHash, 1, 1, 0, false)
	p1 := router(&ProducerMessage{
		Key:     "my-key",
		Payload: []byte("message 1"),
	}, 3)
	assert.Equal(t, 1, p1)

	p2 := router(&ProducerMessage{
		Key:     "my-key",
		Payload: []byte("message 2"),
	}, 3)
	assert.Equal(t, p1, p2)
}

func TestDefaultRouterNoRoutingBecauseOnlyOnePartition(t *testing.T) {

	router := NewDefaultRouter(internal.JavaStringHash, 1, 10, oneHourPublishMaxDelay, false)

	// partition index should not change regardless of the batching settings
	p1 := router(&ProducerMessage{
		Key: "",
	}, 1)
	p2 := router(&ProducerMessage{
		Key: "my-key",
	}, 1)
	p3 := router(&ProducerMessage{
		Payload: []byte("this payload is bigger than 10 bytes"),
	}, 1)

	// we send 2 messages to try trigger the max messages rule
	p4 := router(&ProducerMessage{}, 1)
	p5 := router(&ProducerMessage{}, 1)

	assert.Equal(t, 0, p1)
	assert.Equal(t, 0, p2)
	assert.Equal(t, 0, p3)
	assert.Equal(t, 0, p4)
	assert.Equal(t, 0, p5)
}
