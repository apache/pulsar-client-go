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
	"github.com/prometheus/client_golang/prometheus"
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
		NewMetricsProvider(2, map[string]string{}, prometheus.DefaultRegisterer),
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

// TestBatchBuildersSplitOnSchemaVersion checks that the built-in batch builders refuse a message whose
// schema version differs from the current batch's, whether multi-schema is enabled or not.
func TestBatchBuildersSplitOnSchemaVersion(t *testing.T) {
	newBuilders := map[string]func() (BatchBuilder, error){
		"default": func() (BatchBuilder, error) {
			return NewBatchBuilder(1000, 1000, 1000, "test", 1, pb.CompressionType_NONE, compression.Level(0),
				&bufferPoolImpl{}, NewMetricsProvider(2, map[string]string{}, prometheus.NewRegistry()),
				log.NewLoggerWithLogrus(logrus.StandardLogger()), &mockEncryptor{})
		},
		"key-based": func() (BatchBuilder, error) {
			return NewKeyBasedBatchBuilder(1000, 1000, 1000, "test", 1, pb.CompressionType_NONE,
				compression.Level(0), &bufferPoolImpl{},
				NewMetricsProvider(2, map[string]string{}, prometheus.NewRegistry()),
				log.NewLoggerWithLogrus(logrus.StandardLogger()), &mockEncryptor{})
		},
	}
	versions := [][]byte{nil, {0, 0, 0, 0, 0, 0, 0, 0}}
	for name, newBuilder := range newBuilders {
		for _, multiSchemaEnabled := range []bool{true, false} {
			for _, untypedFirst := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/multiSchema=%v/untypedFirst=%v", name, multiSchemaEnabled, untypedFirst),
					func(t *testing.T) {
						bb, err := newBuilder()
						assert.NoError(t, err)
						first, second := versions[0], versions[1]
						if !untypedFirst {
							first, second = second, first
						}
						add := func(version []byte) bool {
							metadata := &pb.SingleMessageMetadata{PayloadSize: proto.Int32(4)}
							sequenceID := uint64(0)
							return bb.Add(metadata, &sequenceID, []byte("test"), nil, nil, time.Now(),
								version, multiSchemaEnabled, false, 0, 0)
						}
						assert.True(t, add(first))
						assert.True(t, add(first), "same schema version must share the batch")
						assert.False(t, add(second), "different schema version must not share the batch")
					})
			}
		}
	}
}
