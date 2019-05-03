package impl

import (
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"pulsar-client-go-native/pulsar/impl/compression"
	pb "pulsar-client-go-native/pulsar/pulsar_proto"
	"time"
)

const MaxMessageSize = 5 * 1024 * 1024

const MaxBatchSize = 128 * 1024

const DefaultMaxMessagesPerBatch = 1000

type BatchBuilder struct {
	buffer Buffer

	// Current number of messages in the batch
	numMessages uint

	// Max number of message allowed in the batch
	maxMessages uint

	producerName string
	producerId   uint64

	cmdSend     *pb.BaseCommand
	msgMetadata *pb.MessageMetadata
	callbacks   []interface{}

	compressionProvider compression.Provider
}

func NewBatchBuilder(maxMessages uint, producerName string, producerId uint64,
	compressionType pb.CompressionType) *BatchBuilder {
	if maxMessages == 0 {
		maxMessages = DefaultMaxMessagesPerBatch
	}
	bb := &BatchBuilder{
		buffer:       NewBuffer(4096),
		numMessages:  0,
		maxMessages:  maxMessages,
		producerName: producerName,
		producerId:   producerId,
		cmdSend: baseCommand(pb.BaseCommand_SEND,
			&pb.CommandSend{
				ProducerId: &producerId,
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

	return bb
}

func (bb *BatchBuilder) IsFull() bool {
	return bb.numMessages >= bb.maxMessages || bb.buffer.ReadableBytes() > MaxBatchSize
}

func (bb *BatchBuilder) hasSpace(payload []byte) bool {
	msgSize := uint32(len(payload))
	return bb.numMessages > 0 && (bb.buffer.ReadableBytes()+msgSize) > MaxBatchSize
}

func (bb *BatchBuilder) Add(metadata *pb.SingleMessageMetadata, sequenceId uint64, payload []byte,
	callback interface{}, replicateTo []string, ) bool {
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
		bb.msgMetadata.SequenceId = proto.Uint64(sequenceId)
		bb.msgMetadata.PublishTime = proto.Uint64(TimestampMillis(time.Now()))
		bb.msgMetadata.SequenceId = proto.Uint64(sequenceId)
		bb.msgMetadata.ProducerName = &bb.producerName
		bb.msgMetadata.ReplicateTo = replicateTo

		bb.cmdSend.Send.SequenceId = proto.Uint64(sequenceId)
	}
	addSingleMessageToBatch(bb.buffer, metadata, payload)

	bb.numMessages += 1
	bb.callbacks = append(bb.callbacks, callback)
	return true
}

func (bb *BatchBuilder) reset() {
	bb.numMessages = 0
	bb.buffer.Clear()
	bb.callbacks = []interface{}{}
	bb.msgMetadata.ReplicateTo = nil
}

func (bb *BatchBuilder) Flush() (batchData []byte, sequenceId uint64, callbacks []interface{}) {
	log.Debug("BatchBuilder flush: messages: ", bb.numMessages)
	if bb.numMessages == 0 {
		// No-Op for empty batch
		return nil, 0, nil
	}

	bb.msgMetadata.NumMessagesInBatch = proto.Int32(int32(bb.numMessages))
	bb.cmdSend.Send.NumMessages = proto.Int32(int32(bb.numMessages))

	compressed := bb.compressionProvider.Compress(bb.buffer.ReadableSlice())

	buffer := NewBuffer(4096)
	serializeBatch(buffer, bb.cmdSend, bb.msgMetadata, compressed)

	callbacks = bb.callbacks
	sequenceId = bb.cmdSend.Send.GetSequenceId()
	bb.reset()
	return buffer.ReadableSlice(), sequenceId, callbacks
}

func getCompressionProvider(compressionType pb.CompressionType) compression.Provider {
	switch compressionType {
	case pb.CompressionType_NONE:
		return compression.NoopProvider
	case pb.CompressionType_LZ4:
		return compression.Lz4Provider
	case pb.CompressionType_ZLIB:
		return compression.ZLibProvider
	case pb.CompressionType_ZSTD:
		return compression.ZStdProvider
	default:
		log.Panic("Unsupported compression type: ", compressionType)
		return nil
	}
}
