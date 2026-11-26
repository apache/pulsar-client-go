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
	"encoding/binary"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

// parseSendFrame decodes the CommandSend and the MessageMetadata of a serialized
// batch frame: [TOTAL_SIZE][CMD_SIZE][CMD][MAGIC][CHECKSUM][METADATA_SIZE][METADATA][PAYLOAD].
func parseSendFrame(t *testing.T, frame []byte) (*pb.CommandSend, *pb.MessageMetadata) {
	t.Helper()
	require.GreaterOrEqual(t, len(frame), 8)
	cmdSize := binary.BigEndian.Uint32(frame[4:8])
	var cmd pb.BaseCommand
	require.NoError(t, proto.Unmarshal(frame[8:8+cmdSize], &cmd))
	meta, err := NewMessageReaderFromArray(frame[8+cmdSize:]).ReadMessageMetadata()
	require.NoError(t, err)
	return cmd.GetSend(), meta
}

func newTestBatchBuilder(t *testing.T, batchingKeyed bool) BatchBuilder {
	t.Helper()
	metrics := NewMetricsProvider(2, map[string]string{}, prometheus.NewRegistry())
	logger := log.NewLoggerWithLogrus(logrus.StandardLogger())
	if batchingKeyed {
		b, err := NewKeyBasedBatchBuilder(1000, 1000, 1000, "test", 1, pb.CompressionType_NONE,
			compression.Level(0), &bufferPoolImpl{}, metrics, logger, &mockEncryptor{})
		require.NoError(t, err)
		return b
	}
	b, err := NewBatchBuilder(1000, 1000, 1000, "test", 1, pb.CompressionType_NONE,
		compression.Level(0), &bufferPoolImpl{}, metrics, logger, &mockEncryptor{})
	require.NoError(t, err)
	return b
}

func addWithSequenceID(b BatchBuilder, seq uint64, key string) {
	smm := &pb.SingleMessageMetadata{
		SequenceId:  proto.Uint64(seq),
		PayloadSize: proto.Int32(4),
	}
	if key != "" {
		smm.OrderingKey = []byte(key)
	}
	gen := uint64(0)
	b.Add(smm, &gen, []byte("test"), nil, nil, time.Now(), nil, false, false, 0, 0)
}

// A batch carries the sequence id of its first message in sequence_id and the
// sequence id of its last message in highest_sequence_id, both on CommandSend and
// on the message metadata.
func TestBatchBuilderFlushSetsHighestSequenceID(t *testing.T) {
	b := newTestBatchBuilder(t, false)
	defer b.Close()

	addWithSequenceID(b, 5, "")
	addWithSequenceID(b, 6, "")
	addWithSequenceID(b, 7, "")

	flushed := b.Flush()
	require.NotNil(t, flushed)
	require.NoError(t, flushed.Error)
	require.Equal(t, uint64(5), flushed.SequenceID)

	send, meta := parseSendFrame(t, flushed.BatchData.ReadableSlice())
	require.Equal(t, uint64(5), send.GetSequenceId())
	require.Equal(t, uint64(7), send.GetHighestSequenceId())
	require.Equal(t, uint64(5), meta.GetSequenceId())
	require.Equal(t, uint64(7), meta.GetHighestSequenceId())
}

// Each key-based sub-batch carries its own first and last sequence ids.
func TestKeyBasedBatchFlushSetsHighestSequenceID(t *testing.T) {
	b := newTestBatchBuilder(t, true)
	defer b.Close()

	addWithSequenceID(b, 1, "a")
	addWithSequenceID(b, 2, "b")
	addWithSequenceID(b, 3, "a")
	addWithSequenceID(b, 4, "b")

	batches := b.FlushBatches()
	require.Len(t, batches, 2)
	expected := []struct{ first, highest uint64 }{{1, 3}, {2, 4}}
	for i, fb := range batches {
		require.NoError(t, fb.Error)
		require.Equal(t, expected[i].first, fb.SequenceID)
		send, meta := parseSendFrame(t, fb.BatchData.ReadableSlice())
		require.Equal(t, expected[i].first, send.GetSequenceId())
		require.Equal(t, expected[i].highest, send.GetHighestSequenceId())
		require.Equal(t, expected[i].first, meta.GetSequenceId())
		require.Equal(t, expected[i].highest, meta.GetHighestSequenceId())
	}
}
