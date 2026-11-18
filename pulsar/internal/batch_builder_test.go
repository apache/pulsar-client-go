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
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	"github.com/apache/pulsar-client-go/pulsar/internal/crypto"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type testTxn struct {
	use          bool
	mostSigBits  uint64
	leastSigBits uint64
}

var (
	noTxn = testTxn{}
	txnA  = testTxn{use: true, mostSigBits: 1, leastSigBits: 2}
	txnB  = testTxn{use: true, mostSigBits: 1, leastSigBits: 3}
)

func newTestBatchBuilder(t *testing.T, provider BatcherBuilderProvider) BatchBuilder {
	bb, err := provider(
		1000, 1<<20, 1<<20, "test", 1,
		pb.CompressionType_NONE, compression.Level(0),
		NewBufferPool(),
		NewMetricsProvider(2, map[string]string{}, prometheus.NewRegistry()),
		log.NewLoggerWithLogrus(logrus.StandardLogger()),
		crypto.NewNoopEncryptor(),
	)
	require.NoError(t, err)
	return bb
}

func addToBatch(bb BatchBuilder, sequenceID *uint64, key string, txn testTxn) bool {
	metadata := &pb.SingleMessageMetadata{
		PartitionKey: proto.String(key),
		PayloadSize:  proto.Int32(4),
	}
	return bb.Add(metadata, sequenceID, []byte("test"), nil, nil, time.Now(),
		nil, false, txn.use, txn.mostSigBits, txn.leastSigBits)
}

// parseSendFrame decodes a frame written by serializeMessage.
func parseSendFrame(t *testing.T, frame Buffer) (*pb.CommandSend, *pb.MessageMetadata) {
	frame.ReadUint32() // total size
	cmdSize := frame.ReadUint32()
	var cmd pb.BaseCommand
	require.NoError(t, proto.Unmarshal(frame.Read(cmdSize), &cmd))
	metadata, err := NewMessageReader(frame).ReadMessageMetadata()
	require.NoError(t, err)
	return cmd.GetSend(), metadata
}

func assertTxnID(t *testing.T, txn testTxn, cmd *pb.CommandSend, metadata *pb.MessageMetadata) {
	if !txn.use {
		assert.Nil(t, cmd.TxnidMostBits)
		assert.Nil(t, cmd.TxnidLeastBits)
		assert.Nil(t, metadata.TxnidMostBits)
		assert.Nil(t, metadata.TxnidLeastBits)
		return
	}
	assert.Equal(t, txn.mostSigBits, cmd.GetTxnidMostBits())
	assert.Equal(t, txn.leastSigBits, cmd.GetTxnidLeastBits())
	assert.Equal(t, txn.mostSigBits, metadata.GetTxnidMostBits())
	assert.Equal(t, txn.leastSigBits, metadata.GetTxnidLeastBits())
}

func TestBatchContainerSetsTxnIDOnCommandAndMetadata(t *testing.T) {
	bb := newTestBatchBuilder(t, NewBatchBuilder)
	sequenceID := uint64(0)

	// A non-transactional batch following a transactional one must not inherit its txn id.
	for _, txn := range []testTxn{txnA, noTxn, txnB} {
		require.True(t, addToBatch(bb, &sequenceID, "k", txn))
		batch := bb.Flush()
		require.NoError(t, batch.Error)
		cmd, metadata := parseSendFrame(t, batch.BatchData)
		assertTxnID(t, txn, cmd, metadata)
	}
}

func TestBatchContainerRefusesMessageFromDifferentTxn(t *testing.T) {
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
			bb := newTestBatchBuilder(t, NewBatchBuilder)
			sequenceID := uint64(0)
			require.True(t, addToBatch(bb, &sequenceID, "k", c.first))
			assert.False(t, addToBatch(bb, &sequenceID, "k", c.next))

			batch := bb.Flush()
			require.NoError(t, batch.Error)
			assert.Len(t, batch.Callbacks, 1)
			assert.True(t, addToBatch(bb, &sequenceID, "k", c.next))
		})
	}
}

func TestBatchContainerAcceptsMessageFromSameTxn(t *testing.T) {
	for _, txn := range []testTxn{noTxn, txnA} {
		bb := newTestBatchBuilder(t, NewBatchBuilder)
		sequenceID := uint64(0)
		require.True(t, addToBatch(bb, &sequenceID, "k", txn))
		assert.True(t, addToBatch(bb, &sequenceID, "k", txn))
	}
}
