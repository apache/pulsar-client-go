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
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	"github.com/apache/pulsar-client-go/pulsar/internal/crypto"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

// BatcherBuilderProvider defines func which returns the BatchBuilder.
type BatcherBuilderProvider func(
	maxMessages uint, maxBatchSize uint, maxMessageSize uint32, producerName string, producerID uint64,
	compressionType pb.CompressionType, level compression.Level,
	bufferPool BuffersPool, metrics *Metrics, logger log.Logger, encryptor crypto.Encryptor, sequenceIDGenerator *SequenceIDGenerator,
) (BatchBuilder, error)

// BatchBuilder is a interface of batch builders
type BatchBuilder interface {
	// IsFull check if the size in the current batch exceeds the maximum size allowed by the batch
	IsFull() bool

	// Add will add single message to batch.
	Add(
		metadata *pb.SingleMessageMetadata,
		payload []byte,
		callback interface{}, replicateTo []string, deliverAt time.Time,
		schemaVersion []byte, multiSchemaEnabled bool,
		useTxn bool,
		mostSigBits uint64,
		leastSigBits uint64,
		customSequenceID *int64,
	) bool

	// Flush all the messages buffered in the client and wait until all messages have been successfully persisted.
	Flush() *FlushBatch

	// FlushBatches all the messages buffered in multiple batches and wait until all
	// messages have been successfully persisted.
	FlushBatches() []*FlushBatch

	// Return the batch container batch message in multiple batches.
	IsMultiBatches() bool

	reset()
	Close() error
}

type FlushBatch struct {
	BatchData  Buffer
	SequenceID uint64
	Callbacks  []interface{}
	Error      error
}

type messageEntry struct {
	smm              *pb.SingleMessageMetadata
	payload          []byte
	customSequenceID *int64
}

type messageBatch struct {
	entries       []messageEntry
	estimatedSize uint32
}

func (mb *messageBatch) Append(smm *pb.SingleMessageMetadata, payload []byte, customSequenceID *int64) {
	mb.entries = append(mb.entries, messageEntry{
		smm:              smm,
		payload:          payload,
		customSequenceID: customSequenceID,
	})
	mb.estimatedSize += uint32(len(payload) + proto.Size(smm) + 4 + 11) // metadataSize:4 sequenceID: [2,11]
}

func (mb *messageBatch) AssignSequenceIDs(sequenceIDGenerator *SequenceIDGenerator) {
	for k, entry := range mb.entries {
		if entry.smm.SequenceId != nil {
			continue
		}

		var sequenceID uint64
		if entry.customSequenceID != nil {
			sequenceID = uint64(*entry.customSequenceID)
		} else {
			sequenceID = sequenceIDGenerator.Next()
		}
		mb.entries[k].smm.SequenceId = proto.Uint64(sequenceID)
	}
}

func (mb *messageBatch) FlushTo(wb Buffer) {
	for _, entry := range mb.entries {
		addSingleMessageToBatch(wb, entry.smm, entry.payload)
	}
	mb.entries = mb.entries[:0]
	mb.estimatedSize = 0
}

// batchContainer wraps the objects needed to a batch.
// batchContainer implement BatchBuilder as a single batch container.
type batchContainer struct {
	buffer Buffer

	// Current number of messages in the batch
	numMessages uint

	// Max number of message allowed in the batch
	maxMessages uint

	// The largest size for a batch sent from this particular producer.
	// This is used as a baseline to allocate a new buffer that can hold the entire batch
	// without needing costly re-allocations.
	maxBatchSize uint

	maxMessageSize uint32

	producerName        string
	producerID          uint64
	sequenceIDGenerator *SequenceIDGenerator
	messageBatch

	cmdSend     *pb.BaseCommand
	msgMetadata *pb.MessageMetadata
	callbacks   []interface{}

	compressionProvider compression.Provider
	buffersPool         BuffersPool
	metrics             *Metrics

	log log.Logger

	encryptor crypto.Encryptor
}

// newBatchContainer init a batchContainer
func newBatchContainer(
	maxMessages uint, maxBatchSize uint, maxMessageSize uint32, producerName string, producerID uint64,
	compressionType pb.CompressionType, level compression.Level,
	bufferPool BuffersPool, metrics *Metrics, logger log.Logger, encryptor crypto.Encryptor, sequenceIDGenerator *SequenceIDGenerator,
) batchContainer {

	bc := batchContainer{
		buffer:              NewBuffer(4096),
		numMessages:         0,
		maxMessages:         maxMessages,
		maxBatchSize:        maxBatchSize,
		maxMessageSize:      maxMessageSize,
		producerName:        producerName,
		producerID:          producerID,
		sequenceIDGenerator: sequenceIDGenerator,
		cmdSend: baseCommand(
			pb.BaseCommand_SEND,
			&pb.CommandSend{
				ProducerId: &producerID,
			},
		),
		msgMetadata: &pb.MessageMetadata{
			ProducerName: &producerName,
		},
		callbacks:           []interface{}{},
		compressionProvider: GetCompressionProvider(compressionType, level),
		buffersPool:         bufferPool,
		metrics:             metrics,
		log:                 logger,
		encryptor:           encryptor,
	}

	if compressionType != pb.CompressionType_NONE {
		bc.msgMetadata.Compression = &compressionType
	}

	return bc
}

// NewBatchBuilder init batch builder and return BatchBuilder pointer. Build a new batch message container.
func NewBatchBuilder(
	maxMessages uint, maxBatchSize uint, maxMessageSize uint32, producerName string, producerID uint64,
	compressionType pb.CompressionType, level compression.Level,
	bufferPool BuffersPool, metrics *Metrics, logger log.Logger, encryptor crypto.Encryptor, sequenceIDGenerator *SequenceIDGenerator,
) (BatchBuilder, error) {

	bc := newBatchContainer(
		maxMessages, maxBatchSize, maxMessageSize, producerName, producerID, compressionType,
		level, bufferPool, metrics, logger, encryptor, sequenceIDGenerator,
	)

	return &bc, nil
}

// IsFull checks if the size in the current batch meets or exceeds the maximum size allowed by the batch
func (bc *batchContainer) IsFull() bool {
	return bc.numMessages >= bc.maxMessages || bc.buffer.ReadableBytes() >= uint32(bc.maxBatchSize) || bc.estimatedSize >= uint32(bc.maxBatchSize)
}

// hasSpace should return true if and only if the batch container can accommodate another message of length payload.
func (bc *batchContainer) hasSpace(payload []byte) bool {
	if bc.numMessages == 0 {
		// allow to add at least one message when batching is disabled
		return true
	}
	msgSize := uint32(len(payload))
	expectedSize := bc.buffer.ReadableBytes() + msgSize
	return bc.numMessages+1 <= bc.maxMessages &&
		expectedSize <= uint32(bc.maxBatchSize) && expectedSize <= bc.maxMessageSize &&
		bc.estimatedSize+msgSize <= uint32(bc.maxBatchSize) && bc.estimatedSize+msgSize <= bc.maxMessageSize
}

func (bc *batchContainer) hasSameSchema(schemaVersion []byte) bool {
	if bc.numMessages == 0 {
		return true
	}
	return bytes.Equal(bc.msgMetadata.SchemaVersion, schemaVersion)
}

// Add will add single message to batch.
func (bc *batchContainer) Add(
	metadata *pb.SingleMessageMetadata,
	payload []byte,
	callback interface{}, replicateTo []string, deliverAt time.Time,
	schemaVersion []byte, multiSchemaEnabled bool,
	useTxn bool, mostSigBits uint64, leastSigBits uint64, customSequenceID *int64,
) bool {

	if replicateTo != nil && bc.numMessages != 0 {
		// If the current batch is not empty and we're trying to set the replication clusters,
		// then we need to force the current batch to flush and send the message individually
		return false
	} else if bc.msgMetadata.ReplicateTo != nil {
		// There's already a message with cluster replication list. need to flush before next
		// message can be sent
		return false
	} else if !bc.hasSpace(payload) {
		// The current batch is full. Producer has to call Flush() to
		return false
	} else if multiSchemaEnabled && !bc.hasSameSchema(schemaVersion) {
		// The current batch has a different schema. Producer has to call Flush() to
		return false
	}

	if bc.numMessages == 0 {
		bc.msgMetadata.PublishTime = proto.Uint64(TimestampMillis(time.Now()))
		bc.msgMetadata.ProducerName = &bc.producerName
		bc.msgMetadata.ReplicateTo = replicateTo
		bc.msgMetadata.PartitionKey = metadata.PartitionKey
		bc.msgMetadata.SchemaVersion = schemaVersion
		bc.msgMetadata.Properties = metadata.Properties

		if deliverAt.UnixNano() > 0 {
			bc.msgMetadata.DeliverAtTime = proto.Int64(int64(TimestampMillis(deliverAt)))
		}

		if useTxn {
			bc.cmdSend.Send.TxnidMostBits = proto.Uint64(mostSigBits)
			bc.cmdSend.Send.TxnidLeastBits = proto.Uint64(leastSigBits)
		}
	}
	bc.messageBatch.Append(metadata, payload, customSequenceID)

	bc.numMessages++
	bc.callbacks = append(bc.callbacks, callback)
	return true
}

func (bc *batchContainer) reset() {
	bc.numMessages = 0
	bc.buffer.Clear()
	bc.callbacks = []interface{}{}
	bc.msgMetadata.ReplicateTo = nil
	bc.msgMetadata.DeliverAtTime = nil
	bc.msgMetadata.SchemaVersion = nil
	bc.msgMetadata.Properties = nil
}

// Flush all the messages buffered in the client and wait until all messages have been successfully persisted.
func (bc *batchContainer) Flush() *FlushBatch {
	if bc.numMessages == 0 {
		// No-Op for empty batch
		return nil
	}

	bc.log.Debug("BatchBuilder flush: messages: ", bc.numMessages)

	bc.msgMetadata.NumMessagesInBatch = proto.Int32(int32(bc.numMessages))
	bc.cmdSend.Send.NumMessages = proto.Int32(int32(bc.numMessages))

	uncompressedSize := bc.buffer.ReadableBytes()
	bc.msgMetadata.UncompressedSize = &uncompressedSize

	buffer := bc.buffersPool.GetBuffer(int(uncompressedSize * 3 / 2))
	bufferCount := bc.metrics.SendingBuffersCount
	bufferCount.Inc()
	buffer.SetReleaseCallback(func() { bufferCount.Dec() })

	bc.messageBatch.AssignSequenceIDs(bc.sequenceIDGenerator)
	sequenceID := *bc.messageBatch.entries[0].smm.SequenceId
	bc.msgMetadata.SequenceId = proto.Uint64(sequenceID)
	bc.cmdSend.Send.SequenceId = proto.Uint64(sequenceID)
	bc.messageBatch.FlushTo(bc.buffer)

	var err error
	if err = serializeMessage(
		buffer, bc.cmdSend, bc.msgMetadata, bc.buffer, bc.compressionProvider,
		bc.encryptor, bc.maxMessageSize, true,
	); err != nil {
		sequenceID = 0
	}

	callbacks := bc.callbacks
	bc.reset()
	return &FlushBatch{
		BatchData:  buffer,
		SequenceID: sequenceID,
		Callbacks:  callbacks,
		Error:      err,
	}
}

// FlushBatches only for multiple batches container
func (bc *batchContainer) FlushBatches() []*FlushBatch {
	panic("single batch container not support FlushBatches(), please use Flush() instead")
}

// batchContainer as a single batch container
func (bc *batchContainer) IsMultiBatches() bool {
	return false
}

func (bc *batchContainer) Close() error {
	return bc.compressionProvider.Close()
}

func GetCompressionProvider(
	compressionType pb.CompressionType,
	level compression.Level,
) compression.Provider {
	switch compressionType {
	case pb.CompressionType_NONE:
		return compression.NewNoopProvider()
	case pb.CompressionType_LZ4:
		return compression.NewLz4Provider()
	case pb.CompressionType_ZLIB:
		return compression.NewZLibProvider()
	case pb.CompressionType_ZSTD:
		return compression.NewZStdProvider(level)
	case pb.CompressionType_SNAPPY:
		return compression.NewSnappyProvider()
	default:
		panic("unsupported compression type")
	}
}
