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
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"

	"github.com/apache/pulsar-client-go/pkg/pb"
	log "github.com/sirupsen/logrus"
)

const (
	// MaxFrameSize limit the maximum size that pulsar allows for messages to be sent.
	MaxFrameSize        = 5 * 1024 * 1024
	magicCrc32c  uint16 = 0x0e01
)

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

func ParseMessage(headersAndPayload []byte) (msgMeta *pb.MessageMetadata, payloadList [][]byte, err error) {
	// reusable buffer for 4-byte uint32s
	buf32 := make([]byte, 4)
	r := bytes.NewReader(headersAndPayload)
	// Wrap our reader so that we can only read
	// bytes from our frame
	lr := &io.LimitedReader{
		N: int64(len(headersAndPayload)),
		R: r,
	}
	// There are 3 possibilities for the following fields:
	//  - EOF: If so, this is a "simple" command. No more parsing required.
	//  - 2-byte magic number: Indicates the following 4 bytes are a checksum
	//  - 4-byte metadata size

	// The message may optionally stop here. If so,
	// this is a "simple" command.
	if lr.N <= 0 {
		return nil, nil, nil
	}

	// Optionally, the next 2 bytes may be the magicNumber. If
	// so, it indicates that the following 4 bytes are a checksum.
	// If not, the following 2 bytes (plus the 2 bytes already read),
	// are the metadataSize, which is why a 4 byte buffer is used.
	if _, err = io.ReadFull(lr, buf32); err != nil {
		return nil, nil, err
	}

	// Check for magicNumber which indicates a checksum
	var chksum CheckSum
	var expectedChksum []byte

	magicNumber := make([]byte, 2)
	binary.BigEndian.PutUint16(magicNumber, magicCrc32c)
	if magicNumber[0] == buf32[0] && magicNumber[1] == buf32[1] {
		expectedChksum = make([]byte, 4)

		// We already read the 2-byte magicNumber and the
		// initial 2 bytes of the checksum
		expectedChksum[0] = buf32[2]
		expectedChksum[1] = buf32[3]

		// Read the remaining 2 bytes of the checksum
		if _, err = io.ReadFull(lr, expectedChksum[2:]); err != nil {
			return nil, nil, err
		}

		// Use a tee reader to compute the checksum
		// of everything consumed after this point
		lr.R = io.TeeReader(lr.R, &chksum)

		// Fill buffer with metadata size, which is what it
		// would already contain if there were no magic number / checksum
		if _, err = io.ReadFull(lr, buf32); err != nil {
			return nil, nil, err
		}
	}

	// Read metadataSize
	metadataSize := binary.BigEndian.Uint32(buf32)
	// guard against allocating large buffer
	if metadataSize > MaxFrameSize {
		return nil, nil, fmt.Errorf("frame metadata size (%d) "+
			"cannot b greater than max frame size (%d)", metadataSize, MaxFrameSize)
	}

	// Read protobuf encoded metadata
	metaBuf := make([]byte, metadataSize)
	if _, err = io.ReadFull(lr, metaBuf); err != nil {
		return nil, nil, err
	}
	msgMeta = new(pb.MessageMetadata)
	if err = proto.Unmarshal(metaBuf, msgMeta); err != nil {
		return nil, nil, err
	}

	numMsg := msgMeta.GetNumMessagesInBatch()

	if numMsg > 0 && msgMeta.NumMessagesInBatch != nil {
		payloads := make([]byte, lr.N)
		if _, err = io.ReadFull(lr, payloads); err != nil {
			return nil, nil, err
		}

		singleMessages, err := decodeBatchPayload(payloads, numMsg)
		if err != nil {
			return nil, nil, err
		}

		payloadList = make([][]byte, 0, numMsg)
		for _, singleMsg := range singleMessages {
			msgMeta.PartitionKey = singleMsg.SingleMeta.PartitionKey
			msgMeta.Properties = singleMsg.SingleMeta.Properties
			msgMeta.EventTime = singleMsg.SingleMeta.EventTime
			payloadList = append(payloadList, singleMsg.SinglePayload)
		}

		if err := computeChecksum(chksum, expectedChksum); err != nil {
			return nil, nil, err
		}
		return msgMeta, payloadList, nil
	}
	// Anything left in the frame is considered
	// the payload and can be any sequence of bytes.
	payloadList = make([][]byte, 0, 10)
	if lr.N > 0 {
		// guard against allocating large buffer
		if lr.N > MaxFrameSize {
			return nil, nil, fmt.Errorf("frame payload size (%d) cannot be greater than max frame size (%d)", lr.N, MaxFrameSize)
		}

		payload := make([]byte, lr.N)
		if _, err = io.ReadFull(lr, payload); err != nil {
			return nil, nil, err
		}

		payloadList = append(payloadList, payload)
	}

	if err := computeChecksum(chksum, expectedChksum); err != nil {
		return nil, nil, err
	}

	return msgMeta, payloadList, nil
}

func computeChecksum(chksum CheckSum, expectedChksum []byte) error {
	computed := chksum.compute()
	if !bytes.Equal(computed, expectedChksum) {
		return fmt.Errorf("checksum mismatch: computed (0x%X) does "+
			"not match given checksum (0x%X)", computed, expectedChksum)
	}
	return nil
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

// singleMessage represents one of the elements of the batch type payload
type singleMessage struct {
	SingleMetaSize uint32
	SingleMeta     *pb.SingleMessageMetadata
	SinglePayload  []byte
}

// decodeBatchPayload parses the payload of the batch type
// If the producer uses the batch function, msg.Payload will be a singleMessage array structure.
func decodeBatchPayload(bp []byte, batchNum int32) ([]*singleMessage, error) {
	buf32 := make([]byte, 4)
	rdBuf := bytes.NewReader(bp)
	singleMsgList := make([]*singleMessage, 0, batchNum)
	for i := int32(0); i < batchNum; i++ {
		// singleMetaSize
		if _, err := io.ReadFull(rdBuf, buf32); err != nil {
			return nil, err
		}
		singleMetaSize := binary.BigEndian.Uint32(buf32)

		// singleMeta
		singleMetaBuf := make([]byte, singleMetaSize)
		if _, err := io.ReadFull(rdBuf, singleMetaBuf); err != nil {
			return nil, err
		}
		singleMeta := new(pb.SingleMessageMetadata)
		if err := proto.Unmarshal(singleMetaBuf, singleMeta); err != nil {
			return nil, err
		}
		// payload
		singlePayload := make([]byte, singleMeta.GetPayloadSize())
		if _, err := io.ReadFull(rdBuf, singlePayload); err != nil {
			return nil, err
		}
		singleMsg := &singleMessage{
			SingleMetaSize: singleMetaSize,
			SingleMeta:     singleMeta,
			SinglePayload:  singlePayload,
		}

		singleMsgList = append(singleMsgList, singleMsg)
	}
	return singleMsgList, nil
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
