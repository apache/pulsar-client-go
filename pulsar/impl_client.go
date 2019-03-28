package pulsar

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/url"
	"pulsar-client-go-native/pulsar/impl"
	pb "pulsar-client-go-native/pulsar/pulsar_proto"
)

type client struct {
	options ClientOptions

	cnxPool       impl.ConnectionPool
	rpcClient     impl.RpcClient
	lookupService impl.LookupService

	handlers map[impl.Closable]bool
}

func newClient(options ClientOptions) (Client, error) {
	if options.URL == "" {
		return nil, newError(ResultInvalidConfiguration, "URL is required for client")
	}

	url, err := url.Parse(options.URL)
	if err != nil {
		log.WithError(err).Error("Failed to parse service URL")
		return nil, newError(ResultInvalidConfiguration, "Invalid service URL")
	}

	if url.Scheme != "pulsar" {
		return nil, newError(ResultInvalidConfiguration, fmt.Sprintf("Invalid URL scheme '%s'", url.Scheme))
	}

	c := &client{
		cnxPool: impl.NewConnectionPool(),
	}
	c.rpcClient = impl.NewRpcClient(url, c.cnxPool)
	c.lookupService = impl.NewLookupService(c.rpcClient)
	return c, nil
}

func (client *client) CreateProducer(options ProducerOptions) (Producer, error) {
	producer, err := newProducer(client, options)
	if err == nil {
		client.handlers[producer] = true
	}
	return producer, err
}

func (client *client) Subscribe(options ConsumerOptions) (Consumer, error) {
	// TODO: Implement consumer
	return nil, nil
}

func (client *client) CreateReader(options ReaderOptions) (Reader, error) {
	// TODO: Implement reader
	return nil, nil
}

func (client *client) TopicPartitions(topic string) ([]string, error) {
	topicName, err := impl.ParseTopicName(topic)
	if err != nil {
		return nil, err
	}

	id := client.rpcClient.NewRequestId()
	res, err := client.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_PARTITIONED_METADATA,
		&pb.CommandPartitionedTopicMetadata{
			RequestId: &id,
			Topic:     &topicName.Name,
		})
	if err != nil {
		return nil, err
	}

	r := res.Response.PartitionMetadataResponse
	if r.Error != nil {
		return nil, newError(ResultLookupError, r.GetError().String())
	}

	if r.GetPartitions() > 0 {
		partitions := make([]string, r.GetPartitions())
		for i := 0; i < int(r.GetPartitions()); i++ {
			partitions[i] = fmt.Sprintf("%s-partition-%d", topic, i)
		}
		return partitions, nil
	} else {
		// Non-partitioned topic
		return []string{topicName.Name}, nil
	}
}

func (client *client) Close() error {
	for handler := range client.handlers {
		if err := handler.Close(); err != nil {
			return err
		}
	}

	return nil
}
