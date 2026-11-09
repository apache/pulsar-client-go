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

package internal

import (
	"bytes"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestBatchContainer_IsFull(t *testing.T) {
	batcher, err := NewBatchBuilder(
		10,
		1000,
		10000,
		"test",
		1,
		pb.CompressionType_NONE,
		compression.Level(0),
		&bufferPoolImpl{},
		NewMetricsProvider(2, map[string]string{}, prometheus.DefaultRegisterer),
		log.NewLoggerWithLogrus(logrus.StandardLogger()),
		&mockEncryptor{},
		NewSequenceIDGenerator(1),
	)
	if err != nil {
		assert.Fail(t, "Failed to create batcher")
	}

	f := func(payload []byte) bool {
		return batcher.Add(&pb.SingleMessageMetadata{
			PayloadSize: proto.Int32(123),
		}, payload, nil, nil, time.Now(),
			nil, false, false, 0, 0, nil)
	}

	// maxMessages
	for i := 0; i < 9; i++ {
		f([]byte("test"))
		assert.False(t, batcher.IsFull())
	}
	f([]byte("test"))
	assert.True(t, batcher.IsFull())

	batcher.Flush()
	assert.False(t, batcher.IsFull())

	// maxBatchSize
	f(bytes.Repeat([]byte("a"), 1000))
	assert.True(t, batcher.IsFull())
}
