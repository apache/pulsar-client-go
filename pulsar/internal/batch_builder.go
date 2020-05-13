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
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	"github.com/apache/pulsar-client-go/pulsar/internal/pb"
	"github.com/golang/protobuf/proto"

	log "github.com/sirupsen/logrus"
)

const (
	// MaxMessageSize limit message size for transfer
	MaxMessageSize = 5 * 1024 * 1024
	// MaxBatchSize will be the largest size for a batch sent from this particular producer.
	// This is used as a baseline to allocate a new buffer that can hold the entire batch
	// without needing costly re-allocations.
	MaxBatchSize = 128 * 1024
	// DefaultMaxMessagesPerBatch init default num of entries in per batch.
	DefaultMaxMessagesPerBatch = 1000
)

// BatchBuilder wraps the objects needed to build a batch.
type BatchBuilder struct {
	buffer Buffer

	// Current number of messages in the batch
	numMessages uint

	// Max number of message allowed in the batch
	maxMessages uint

	producerName string
	producerID   uint64

	cmdSend     *pb.BaseCommand
	msgMetadata *pb.MessageMetadata
	callbacks   []interface{}

	compressionProvider compression.Provider
}

// NewBatchBuilder init batch builder and return BatchBuilder pointer. Build a new batch message container.
func NewBatchBuilder(maxMessages uint, producerName string, producerID uint64,
	compressionType pb.CompressionType) (*BatchBuilder, error) {
	if maxMessages == 0 {
		maxMessages = DefaultMaxMessagesPerBatch
	}
	bb := &BatchBuilder{
		buffer:       NewBuffer(4096),
		numMessages:  0,
		maxMessages:  maxMessages,
		producerName: producerName,
		producerID:   producerID,
		cmdSend: baseCommand(pb.BaseCommand_SEND,
			&pb.CommandSend{
				ProducerId: &producerID,
			}),
		msgMetadata: &pb.MessageMetadata{
			ProducerName: &producerName,
		},
		callbacks:           []interface{}{},
		compressionProvider: getCompressionProvider(compressionType),
	}

	if compressionType != pb.CompressionType_NONE {
		bb.msgMetadata.Compression = &compressionType
	}

	if !bb.compressionProvider.CanCompress() {
		return nil, fmt.Errorf("compression provider %d can only decompress data", compressionType)
	}

	return bb, nil
}

// IsFull check if the size in the current batch exceeds the maximum size allowed by the batch
func (bb *BatchBuilder) IsFull() bool {
	return bb.numMessages >= bb.maxMessages || bb.buffer.ReadableBytes() > MaxBatchSize
}

func (bb *BatchBuilder) hasSpace(payload []byte) bool {
	msgSize := uint32(len(payload))
	return bb.numMessages > 0 && (bb.buffer.ReadableBytes()+msgSize) > MaxBatchSize
}

// Add will add single message to batch.
func (bb *BatchBuilder) Add(metadata *pb.SingleMessageMetadata, sequenceID uint64, payload []byte,
	callback interface{}, replicateTo []string, deliverAt time.Time) bool {
	if replicateTo != nil && bb.numMessages != 0 {
		// If the current batch is not empty and we're trying to set the replication clusters,
		// then we need to force the current batch to flush and send the message individually
		return false
	} else if bb.msgMetadata.ReplicateTo != nil {
		// There's already a message with cluster replication list. need to flush before next
		// message can be sent
		return false
	} else if bb.hasSpace(payload) {
		// The current batch is full. Producer has to call Flush() to
		return false
	}

	if bb.numMessages == 0 {
		bb.msgMetadata.SequenceId = proto.Uint64(sequenceID)
		bb.msgMetadata.PublishTime = proto.Uint64(TimestampMillis(time.Now()))
		bb.msgMetadata.SequenceId = proto.Uint64(sequenceID)
		bb.msgMetadata.ProducerName = &bb.producerName
		bb.msgMetadata.ReplicateTo = replicateTo
		bb.msgMetadata.PartitionKey = metadata.PartitionKey

		if deliverAt.UnixNano() > 0 {
			bb.msgMetadata.DeliverAtTime = proto.Int64(int64(TimestampMillis(deliverAt)))
		}

		bb.cmdSend.Send.SequenceId = proto.Uint64(sequenceID)
	}
	addSingleMessageToBatch(bb.buffer, metadata, payload)

	bb.numMessages++
	bb.callbacks = append(bb.callbacks, callback)
	return true
}

func (bb *BatchBuilder) reset() {
	bb.numMessages = 0
	bb.buffer.Clear()
	bb.callbacks = []interface{}{}
	bb.msgMetadata.ReplicateTo = nil
}

// Flush all the messages buffered in the client and wait until all messages have been successfully persisted.
func (bb *BatchBuilder) Flush() (batchData []byte, sequenceID uint64, callbacks []interface{}) {
	log.Debug("BatchBuilder flush: messages: ", bb.numMessages)
	if bb.numMessages == 0 {
		// No-Op for empty batch
		return nil, 0, nil
	}

	bb.msgMetadata.NumMessagesInBatch = proto.Int32(int32(bb.numMessages))
	bb.cmdSend.Send.NumMessages = proto.Int32(int32(bb.numMessages))

	uncompressedSize := bb.buffer.ReadableBytes()
	compressed := bb.compressionProvider.Compress(bb.buffer.ReadableSlice())
	bb.msgMetadata.UncompressedSize = &uncompressedSize

	buffer := NewBuffer(4096)
	serializeBatch(buffer, bb.cmdSend, bb.msgMetadata, compressed)

	callbacks = bb.callbacks
	sequenceID = bb.cmdSend.Send.GetSequenceId()
	bb.reset()
	return buffer.ReadableSlice(), sequenceID, callbacks
}

func getCompressionProvider(compressionType pb.CompressionType) compression.Provider {
	switch compressionType {
	case pb.CompressionType_NONE:
		return compression.NoopProvider()
	case pb.CompressionType_LZ4:
		return compression.Lz4Provider()
	case pb.CompressionType_ZLIB:
		return compression.ZLibProvider()
	case pb.CompressionType_ZSTD:
		return compression.ZStdProvider()
	default:
		log.Panic("unsupported compression type")
		return nil
	}
}
