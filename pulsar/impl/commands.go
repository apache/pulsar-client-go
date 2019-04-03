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
	default:
		log.Panic("Missing command type: ", cmdType)
	}

	return cmd
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
