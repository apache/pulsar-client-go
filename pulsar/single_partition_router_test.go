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

	"github.com/stretchr/testify/assert"
)

type topicMetaData struct {
	partition uint32
}

func (t topicMetaData) NumPartitions() uint32 {
	return t.partition
}

func TestNewSinglePartitionRouter(t *testing.T) {
	numPartitions := topicMetaData{2}
	router := NewSinglePartitionRouter()
	p := router(&ProducerMessage{
		Payload: []byte("message 2"),
	}, numPartitions)
	assert.GreaterOrEqual(t, p, 0)

	p2 := router(&ProducerMessage{
		Payload: []byte("message 2"),
	}, numPartitions)
	assert.Equal(t, p, p2)
}

func TestNewSinglePartitionRouterWithKey(t *testing.T) {
	router := NewSinglePartitionRouter()
	numPartitions := topicMetaData{3}
	p := router(&ProducerMessage{
		Payload: []byte("message 2"),
		Key:     "my-key",
	}, numPartitions)
	assert.Equal(t, 1, p)

	p2 := router(&ProducerMessage{
		Key:     "my-key",
		Payload: []byte("message 2"),
	}, numPartitions)
	assert.Equal(t, p, p2)
}
func TestNewSinglePartitionRouterWithOrderingKey(t *testing.T) {
	router := NewSinglePartitionRouter()
	numPartitions := topicMetaData{3}
	p := router(&ProducerMessage{
		Payload:     []byte("message 2"),
		OrderingKey: "my-key",
	}, numPartitions)
	assert.Equal(t, 1, p)

	p2 := router(&ProducerMessage{
		OrderingKey: "my-key",
		Payload:     []byte("message 2"),
	}, numPartitions)
	assert.Equal(t, p, p2)
}
