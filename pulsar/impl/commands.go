package impl

import (
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	pb "pulsar-client-go-native/pulsar/pulsar_proto"
)

const MaxFrameSize = 5 * 1024 * 1024

func baseCommand(cmdType pb.BaseCommand_Type, msg proto.Message) *pb.BaseCommand {
	cmd := &pb.BaseCommand{
		Type: &cmdType,
	}
	switch cmdType {
	case pb.BaseCommand_CONNECT:
		cmd.Connect = msg.(*pb.CommandConnect)
		break
	case pb.BaseCommand_LOOKUP:
		cmd.LookupTopic = msg.(*pb.CommandLookupTopic)
		break

	case pb.BaseCommand_PARTITIONED_METADATA:
		cmd.PartitionMetadata = msg.(*pb.CommandPartitionedTopicMetadata)
		break

	default:
		log.Panic("Missing command type: ", cmdType)
	}

	return cmd
}

