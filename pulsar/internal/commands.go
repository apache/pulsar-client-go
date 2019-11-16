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
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"

	log "github.com/sirupsen/logrus"

	"github.com/apache/pulsar-client-go/pkg/pb"
)

const (
	// MaxFrameSize limit the maximum size that pulsar allows for messages to be sent.
	MaxFrameSize        = 5 * 1024 * 1024
	magicCrc32c  uint16 = 0x0e01
)

// ErrCorruptedMessage is the error returned by ReadMessageData when it has detected corrupted data.
// The data is considered corrupted if it's missing a header, a checksum mismatch or there
// was an error when unmarshalling the message metadata.
var ErrCorruptedMessage = errors.New("corrupted message")

// ErrEOM is the error returned by ReadMessage when no more input is available.
var ErrEOM = errors.New("EOF")

func NewMessageReader(headersAndPayload Buffer) *MessageReader {
	return &MessageReader{
		buffer: headersAndPayload,
	}
}

func NewMessageReaderFromArray(headersAndPayload []byte) *MessageReader {
	return NewMessageReader(NewBufferWrapper(headersAndPayload))
}

// MessageReader provides helper methods to parse
// the metadata and messages from the binary format
// Wire format for a messages
//
// Old format (single message)
// [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
//
// Batch format
// [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [METADATA_SIZE][METADATA][PAYLOAD] [METADATA_SIZE][METADATA][PAYLOAD]
//
type MessageReader struct {
	buffer Buffer
	// true if we are parsing a batched message - set after parsing the message metadata
	batched bool
}

// ReadChecksum
func (r *MessageReader) readChecksum() (uint32, error) {
	if r.buffer.ReadableBytes() < 6 {
		return 0, errors.New("missing message header")
	}
	// reader magic number
	magicNumber := r.buffer.ReadUint16()
	if magicNumber != magicCrc32c {
		return 0, ErrCorruptedMessage
	}
	checksum := r.buffer.ReadUint32()
	return checksum, nil
}

func (r *MessageReader) ReadMessageMetadata() (*pb.MessageMetadata, error) {
	// Wire format
	// [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA]

	// read checksum
	checksum, err := r.readChecksum()
	if err != nil {
		return nil, err
	}

	// validate checksum
	computedChecksum := Crc32cCheckSum(r.buffer.ReadableSlice())
	if checksum != computedChecksum {
		return nil, fmt.Errorf("checksum mismatch received: 0x%x computed: 0x%x", checksum, computedChecksum)
	}

	size := r.buffer.ReadUint32()
	data := r.buffer.Read(size)
	var meta pb.MessageMetadata
	if err := proto.Unmarshal(data, &meta); err != nil {
		return nil, ErrCorruptedMessage
	}

	if meta.NumMessagesInBatch != nil {
		r.batched = true
	}

	return &meta, nil
}

func (r *MessageReader) ReadMessage() (*pb.SingleMessageMetadata, []byte, error) {
	if r.buffer.ReadableBytes() == 0 {
		return nil, nil, ErrEOM
	}
	if !r.batched {
		return r.readMessage()
	}

	return r.readSingleMessage()
}

func (r *MessageReader) readMessage() (*pb.SingleMessageMetadata, []byte, error) {
	// Wire format
	// [PAYLOAD]

	return nil, r.buffer.Read(r.buffer.ReadableBytes()), nil
}

func (r *MessageReader) readSingleMessage() (*pb.SingleMessageMetadata, []byte, error) {
	// Wire format
	// [METADATA_SIZE][METADATA][PAYLOAD]

	size := r.buffer.ReadUint32()
	var meta pb.SingleMessageMetadata
	if err := proto.Unmarshal(r.buffer.Read(size), &meta); err != nil {
		return nil, nil, err
	}

	return &meta, r.buffer.Read(uint32(meta.GetPayloadSize())), nil
}

func (r *MessageReader) ResetBuffer(buffer Buffer) {
	r.buffer = buffer
}

func baseCommand(cmdType pb.BaseCommand_Type, msg proto.Message) *pb.BaseCommand {
	cmd := &pb.BaseCommand{
		Type: &cmdType,
	}
	switch cmdType {
	case pb.BaseCommand_CONNECT:
		cmd.Connect = msg.(*pb.CommandConnect)
	case pb.BaseCommand_LOOKUP:
		cmd.LookupTopic = msg.(*pb.CommandLookupTopic)
	case pb.BaseCommand_PARTITIONED_METADATA:
		cmd.PartitionMetadata = msg.(*pb.CommandPartitionedTopicMetadata)
	case pb.BaseCommand_PRODUCER:
		cmd.Producer = msg.(*pb.CommandProducer)
	case pb.BaseCommand_SUBSCRIBE:
		cmd.Subscribe = msg.(*pb.CommandSubscribe)
	case pb.BaseCommand_FLOW:
		cmd.Flow = msg.(*pb.CommandFlow)
	case pb.BaseCommand_PING:
		cmd.Ping = msg.(*pb.CommandPing)
	case pb.BaseCommand_PONG:
		cmd.Pong = msg.(*pb.CommandPong)
	case pb.BaseCommand_SEND:
		cmd.Send = msg.(*pb.CommandSend)
	case pb.BaseCommand_CLOSE_PRODUCER:
		cmd.CloseProducer = msg.(*pb.CommandCloseProducer)
	case pb.BaseCommand_CLOSE_CONSUMER:
		cmd.CloseConsumer = msg.(*pb.CommandCloseConsumer)
	case pb.BaseCommand_ACK:
		cmd.Ack = msg.(*pb.CommandAck)
	case pb.BaseCommand_SEEK:
		cmd.Seek = msg.(*pb.CommandSeek)
	case pb.BaseCommand_UNSUBSCRIBE:
		cmd.Unsubscribe = msg.(*pb.CommandUnsubscribe)
	case pb.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES:
		cmd.RedeliverUnacknowledgedMessages = msg.(*pb.CommandRedeliverUnacknowledgedMessages)
	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE:
		cmd.GetTopicsOfNamespace = msg.(*pb.CommandGetTopicsOfNamespace)
	default:
		log.Panic("Missing command type: ", cmdType)
	}

	return cmd
}

func addSingleMessageToBatch(wb Buffer, smm *pb.SingleMessageMetadata, payload []byte) {
	serialized, err := proto.Marshal(smm)
	if err != nil {
		log.WithError(err).Fatal("Protobuf serialization error")
	}

	wb.WriteUint32(uint32(len(serialized)))
	wb.Write(serialized)
	wb.Write(payload)
}

func serializeBatch(wb Buffer, cmdSend *pb.BaseCommand, msgMetadata *pb.MessageMetadata, payload []byte) {
	// Wire format
	// [TOTAL_SIZE] [CMD_SIZE][CMD] [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
	cmdSize := proto.Size(cmdSend)
	msgMetadataSize := proto.Size(msgMetadata)
	payloadSize := len(payload)

	magicAndChecksumLength := 2 + 4 /* magic + checksumLength */
	headerContentSize := 4 + cmdSize + magicAndChecksumLength + 4 + msgMetadataSize
	// cmdLength + cmdSize + magicLength + checksumSize + msgMetadataLength + msgMetadataSize
	totalSize := headerContentSize + payloadSize

	wb.WriteUint32(uint32(totalSize)) // External frame

	// Write cmd
	wb.WriteUint32(uint32(cmdSize))
	serialized, err := proto.Marshal(cmdSend)
	if err != nil {
		log.WithError(err).Fatal("Protobuf error when serializing cmdSend")
	}

	wb.Write(serialized)

	// Create checksum placeholder
	wb.WriteUint16(magicCrc32c)
	checksumIdx := wb.WriterIndex()
	wb.WriteUint32(0) // skip 4 bytes of checksum

	// Write metadata
	metadataStartIdx := wb.WriterIndex()
	wb.WriteUint32(uint32(msgMetadataSize))
	serialized, err = proto.Marshal(msgMetadata)
	if err != nil {
		log.WithError(err).Fatal("Protobuf error when serializing msgMetadata")
	}

	wb.Write(serialized)
	wb.Write(payload)

	// Write checksum at created checksum-placeholder
	endIdx := wb.WriterIndex()
	checksum := Crc32cCheckSum(wb.Get(metadataStartIdx, endIdx-metadataStartIdx))

	// set computed checksum
	wb.PutUint32(checksum, checksumIdx)
}

// ConvertFromStringMap convert a string map to a KeyValue []byte
func ConvertFromStringMap(m map[string]string) []*pb.KeyValue {
	list := make([]*pb.KeyValue, len(m))

	i := 0
	for k, v := range m {
		list[i] = &pb.KeyValue{
			Key:   proto.String(k),
			Value: proto.String(v),
		}

		i++
	}

	return list
}

// ConvertToStringMap convert a KeyValue []byte to string map
func ConvertToStringMap(pbb []*pb.KeyValue) map[string]string {
	m := make(map[string]string)

	for _, kv := range pbb {
		m[*kv.Key] = *kv.Value
	}

	return m
}
