package impl

import (
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
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
}

func NewBatchBuilder(maxMessages uint, producerName string, producerId uint64) *BatchBuilder {
	if maxMessages == 0 {
		maxMessages = DefaultMaxMessagesPerBatch
	}
	return &BatchBuilder{
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
	}
}

func (bb *BatchBuilder) IsFull() bool {
	return bb.numMessages >= bb.maxMessages || bb.buffer.ReadableBytes() > MaxBatchSize
}

func (bb *BatchBuilder) hasSpace(payload []byte) bool {
	msgSize := uint32(len(payload))
	return bb.numMessages > 0 && (bb.buffer.ReadableBytes()+msgSize) > MaxBatchSize
}

func (bb *BatchBuilder) Add(metadata *pb.SingleMessageMetadata, sequenceId uint64, payload []byte) bool {
	if bb.hasSpace(payload) {
		// The current batch is full. Producer has to call Flush() to
		return false
	}

	if bb.numMessages == 0 {
		bb.msgMetadata.SequenceId = proto.Uint64(sequenceId)
		bb.msgMetadata.PublishTime = proto.Uint64(TimestampMillis(time.Now()))
		bb.msgMetadata.SequenceId = proto.Uint64(sequenceId)
		bb.msgMetadata.ProducerName = &bb.producerName

		bb.cmdSend.Send.SequenceId = proto.Uint64(sequenceId)
	}
	serializeSingleMessage(bb.buffer, metadata, payload)

	bb.numMessages += 1
	return true
}

func (bb *BatchBuilder) reset() {
	bb.numMessages = 0
	bb.buffer.Clear()
}

func (bb *BatchBuilder) Flush() []byte {
	log.Info("BatchBuilder flush: messages: ", bb.numMessages)
	if bb.numMessages == 0 {
		// No-Op for empty batch
		return nil
	}

	bb.msgMetadata.NumMessagesInBatch = proto.Int32(int32(bb.numMessages))
	bb.cmdSend.Send.NumMessages = proto.Int32(int32(bb.numMessages))

	buffer := NewBuffer(4096)
	serializeBatch(buffer, bb.cmdSend, bb.msgMetadata, bb.buffer.ReadableSlice())

	bb.reset()
	return buffer.ReadableSlice()
}
