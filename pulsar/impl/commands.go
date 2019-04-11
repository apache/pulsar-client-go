package impl

import (
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	pb "pulsar-client-go-native/pulsar/pulsar_proto"
)

const MaxFrameSize = 5 * 1024 * 1024

const magicCrc32c uint16 = 0x0e01

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
	case pb.BaseCommand_PING:
		cmd.Ping = msg.(*pb.CommandPing)
	case pb.BaseCommand_PONG:
		cmd.Pong = msg.(*pb.CommandPong)
	case pb.BaseCommand_SEND:
		cmd.Send = msg.(*pb.CommandSend)
	case pb.BaseCommand_CLOSE_PRODUCER:
		cmd.CloseProducer = msg.(*pb.CommandCloseProducer)
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

	wb.WriteUint32(uint32(totalSize))  // External frame

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

func ConvertFromStringMap(m map[string]string) []*pb.KeyValue {
	list := make([]*pb.KeyValue, len(m))

	i := 0
	for k, v := range m {
		list[i] = &pb.KeyValue{
			Key:   proto.String(k),
			Value: proto.String(v),
		}

		i += 1
	}

	return list
}

func ConvertToStringMap(pbb []*pb.KeyValue) map[string]string {
	m := make(map[string]string)

	for _, kv := range pbb {
		m[*kv.Key] = *kv.Value
	}

	return m
}
