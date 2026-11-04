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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

// readBatchFrame decodes the CommandSend and the batch MessageMetadata of a flushed batch.
func readBatchFrame(t *testing.T, buffer Buffer) (*pb.CommandSend, *pb.MessageMetadata) {
	t.Helper()
	frame := NewBufferWrapper(buffer.ReadableSlice())
	frame.ReadUint32() // total size
	var cmd pb.BaseCommand
	require.NoError(t, proto.Unmarshal(frame.Read(frame.ReadUint32()), &cmd))
	meta, err := NewMessageReader(frame).ReadMessageMetadata()
	require.NoError(t, err)
	return cmd.GetSend(), meta
}

func newTestBatchBuilder(t *testing.T, provider BatcherBuilderProvider) BatchBuilder {
	t.Helper()
	builder, err := provider(1000, 1<<20, 1<<20, "test", 1, pb.CompressionType_NONE, compression.Level(0),
		&bufferPoolImpl{}, NewMetricsProvider(2, map[string]string{}, prometheus.NewRegistry()),
		log.DefaultNopLogger(), &mockEncryptor{})
	require.NoError(t, err)
	return builder
}

func addWithSequenceID(t *testing.T, builder BatchBuilder, generator *uint64, key string, sequenceID *uint64) {
	t.Helper()
	metadata := &pb.SingleMessageMetadata{
		PartitionKey: proto.String(key),
		PayloadSize:  proto.Int32(4),
		SequenceId:   sequenceID,
	}
	require.True(t, builder.Add(metadata, generator, []byte("test"), nil, nil, time.Now(),
		nil, false, false, 0, 0))
}

func TestBatchContainerSetsHighestSequenceID(t *testing.T) {
	builder := newTestBatchBuilder(t, NewBatchBuilder)
	generator := uint64(0)

	for _, sequenceID := range []uint64{5, 6, 9} {
		addWithSequenceID(t, builder, &generator, "", proto.Uint64(sequenceID))
	}
	batch := builder.Flush()
	require.NotNil(t, batch)
	require.NoError(t, batch.Error)
	assert.Equal(t, uint64(5), batch.SequenceID)
	assert.Equal(t, uint64(9), batch.HighestSequenceID)

	send, meta := readBatchFrame(t, batch.BatchData)
	assert.Equal(t, uint64(5), send.GetSequenceId())
	assert.Equal(t, uint64(9), send.GetHighestSequenceId())
	assert.Equal(t, int32(3), send.GetNumMessages())
	assert.Equal(t, uint64(5), meta.GetSequenceId())
	assert.Equal(t, uint64(9), meta.GetHighestSequenceId())

	// The next batch must not inherit the previous highest sequence ID.
	addWithSequenceID(t, builder, &generator, "", proto.Uint64(10))
	batch = builder.Flush()
	require.NotNil(t, batch)
	assert.Equal(t, uint64(10), batch.SequenceID)
	assert.Equal(t, uint64(10), batch.HighestSequenceID)
	send, meta = readBatchFrame(t, batch.BatchData)
	assert.Equal(t, uint64(10), send.GetHighestSequenceId())
	assert.Equal(t, uint64(10), meta.GetHighestSequenceId())
}

func TestBatchContainerHighestSequenceIDWithGeneratedFirstID(t *testing.T) {
	builder := newTestBatchBuilder(t, NewBatchBuilder)
	generator := uint64(7)

	// The first message takes its sequence ID from the generator inside Add.
	addWithSequenceID(t, builder, &generator, "", nil)
	batch := builder.Flush()
	require.NotNil(t, batch)
	assert.Equal(t, uint64(7), batch.SequenceID)
	assert.Equal(t, uint64(7), batch.HighestSequenceID)
	assert.Equal(t, uint64(8), generator)
}

func TestKeyBasedBatchContainerSetsHighestSequenceIDPerKey(t *testing.T) {
	builder := newTestBatchBuilder(t, NewKeyBasedBatchBuilder)
	generator := uint64(0)

	sequenceID := uint64(0)
	for i := 0; i < 9; i++ {
		addWithSequenceID(t, builder, &generator, fmt.Sprintf("key-%d", i%3), proto.Uint64(sequenceID))
		sequenceID++
	}
	batches := builder.FlushBatches()
	require.Len(t, batches, 3)
	for i, batch := range batches {
		// key-i holds sequence IDs i, i+3 and i+6.
		assert.Equal(t, uint64(i), batch.SequenceID)
		assert.Equal(t, uint64(i+6), batch.HighestSequenceID)
		send, meta := readBatchFrame(t, batch.BatchData)
		assert.Equal(t, uint64(i), send.GetSequenceId())
		assert.Equal(t, uint64(i+6), send.GetHighestSequenceId())
		assert.Equal(t, uint64(i+6), meta.GetHighestSequenceId())
	}
}
