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
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

type mockEncryptor struct {
}

func (m *mockEncryptor) Encrypt(_ []byte, _ *pb.MessageMetadata) ([]byte, error) {
	return []byte("test"), nil
}

func TestKeyBasedBatcherOrdering(t *testing.T) {
	keyBatcher, err := NewKeyBasedBatchBuilder(
		1000,
		1000,
		1000,
		"test",
		1,
		pb.CompressionType_NONE,
		compression.Level(0),
		&bufferPoolImpl{},
		log.NewLoggerWithLogrus(logrus.StandardLogger()),
		&mockEncryptor{},
	)
	if err != nil {
		assert.Fail(t, "Failed to create key based batcher")
	}

	sequenceID := uint64(0)
	for i := 0; i < 10; i++ {
		metadata := &pb.SingleMessageMetadata{
			OrderingKey: []byte(fmt.Sprintf("key-%d", i)),
			PayloadSize: proto.Int32(0),
		}
		assert.True(t, keyBatcher.Add(metadata, &sequenceID, []byte("test"), nil, nil, time.Now(),
			nil, false, false, 0, 0))
	}

	batches := keyBatcher.FlushBatches()
	for i := 1; i < len(batches); i++ {
		if batches[i].SequenceID <= batches[i-1].SequenceID {
			t.Errorf("Batch id is not incremental at index %d: %d <= %d", i, batches[i].SequenceID, batches[i-1].SequenceID)
		}
	}
}
