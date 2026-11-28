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
	"github.com/stretchr/testify/require"
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

func TestKeyBasedBatcherSetsTxnIDOnCommandAndMetadata(t *testing.T) {
	bb := newTestBatchBuilder(t, NewKeyBasedBatchBuilder)
	sequenceID := uint64(0)

	// Different keys batch separately, so each key may carry its own transaction.
	txns := []testTxn{txnA, noTxn, txnB}
	for i, txn := range txns {
		require.True(t, addToBatch(bb, &sequenceID, fmt.Sprintf("key-%d", i), txn))
	}

	batches := bb.FlushBatches()
	require.Len(t, batches, len(txns))
	for i, batch := range batches {
		require.NoError(t, batch.Error)
		cmd, metadata := parseSendFrame(t, batch.BatchData)
		assertTxnID(t, txns[i], cmd, metadata)
	}
}

func TestKeyBasedBatcherRefusesMessageFromDifferentTxn(t *testing.T) {
	cases := []struct {
		name  string
		first testTxn
		next  testTxn
	}{
		{"txn then other txn", txnA, txnB},
		{"txn then no txn", txnA, noTxn},
		{"no txn then txn", noTxn, txnA},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bb := newTestBatchBuilder(t, NewKeyBasedBatchBuilder)
			sequenceID := uint64(0)
			require.True(t, addToBatch(bb, &sequenceID, "k", c.first))
			assert.False(t, addToBatch(bb, &sequenceID, "k", c.next))

			batches := bb.FlushBatches()
			require.Len(t, batches, 1)
			assert.Len(t, batches[0].Callbacks, 1)
			assert.True(t, addToBatch(bb, &sequenceID, "k", c.next))
		})
	}
}

func TestKeyBasedBatcherAcceptsMessageFromSameTxn(t *testing.T) {
	for _, txn := range []testTxn{noTxn, txnA} {
		bb := newTestBatchBuilder(t, NewKeyBasedBatchBuilder)
		sequenceID := uint64(0)
		require.True(t, addToBatch(bb, &sequenceID, "k", txn))
		assert.True(t, addToBatch(bb, &sequenceID, "k", txn))
	}
}
