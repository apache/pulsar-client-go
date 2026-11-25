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
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestBatchBuilderAddsTxnIDToMessageMetadata(t *testing.T) {
	batcher, err := NewBatchBuilder(
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
	require.NoError(t, err)

	sequenceID := uint64(0)
	metadata := &pb.SingleMessageMetadata{
		PayloadSize: proto.Int32(0),
	}

	require.True(t, batcher.Add(metadata, &sequenceID, []byte("test"), nil, nil, time.Time{},
		nil, false, true, 12, 34))

	bc := batcher.(*batchContainer)
	assert.Equal(t, uint64(12), bc.msgMetadata.GetTxnidMostBits())
	assert.Equal(t, uint64(34), bc.msgMetadata.GetTxnidLeastBits())
}

func TestBatchBuilderClearsTxnIDAfterFlush(t *testing.T) {
	batcher, err := NewBatchBuilder(
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
	require.NoError(t, err)

	sequenceID := uint64(0)
	metadata := &pb.SingleMessageMetadata{
		PayloadSize: proto.Int32(0),
	}

	require.True(t, batcher.Add(metadata, &sequenceID, []byte("test"), nil, nil, time.Time{},
		nil, false, true, 12, 34))
	require.NotNil(t, batcher.Flush())

	bc := batcher.(*batchContainer)
	assert.Nil(t, bc.msgMetadata.TxnidMostBits)
	assert.Nil(t, bc.msgMetadata.TxnidLeastBits)
	assert.Nil(t, bc.cmdSend.Send.TxnidMostBits)
	assert.Nil(t, bc.cmdSend.Send.TxnidLeastBits)
}
