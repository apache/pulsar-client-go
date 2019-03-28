package pulsar

import (
	"context"
	log "github.com/sirupsen/logrus"
	//"pulsar-client-go-native/pulsar/impl"
	//pb "pulsar-client-go-native/pulsar/pulsar_proto"
	"sync"
)

type partitionProducer struct {
	client *client
	topic  string
	log    *log.Entry
	mutex  sync.Mutex
	cond   *sync.Cond
}

func newPartitionProducer(client *client, topic string, options *ProducerOptions) (*partitionProducer, error) {

	p := &partitionProducer{
		log: log.WithField("topic", topic),
	}

	err := p.grabCnx()
	if err != nil {
		log.WithError(err).Errorf("Failed to create producer")
		return nil, err
	} else {
		log.Info("Created producer")
		return p, nil
	}
}

func (p *partitionProducer) grabCnx() error {
	//lr, err := p.client.lookupService.Lookup(p.topic)
	//if err != nil {
	//	p.log.WithError(err).Warn("Failed to lookup topic")
	//	return err
	//}

	//id := p.client.rpcClient.NewRequestId()
	//p.client.rpcClient.Request(lr.LogicalAddr.Host, lr.PhysicalAddr.Host, id, pb.BaseCommand_PRODUCER, *pb.CommandProducer{
	//
	//})
	//
	//var cnx impl.Connection
	//cnx, err = p.client.cnxPool.GetConnection(lr.LogicalAddr.Host, lr.PhysicalAddr.Host)
	//if err != nil {
	//	p.log.WithError(err).Warn("Failed to get connection")
	//	return err
	//}

	//cnx.

	return nil
}

func (p *partitionProducer) run() {

}

func (p *partitionProducer) Topic() string {
	return ""
}

func (p *partitionProducer) Name() string {
	return ""
}

func (p *partitionProducer) Send(context.Context, ProducerMessage) error {
	return nil
}

func (p *partitionProducer) SendAsync(context.Context, ProducerMessage, func(ProducerMessage, error)) {
}

func (p *partitionProducer) LastSequenceID() int64 {
	return -1
}

func (p *partitionProducer) Flush() error {
	return nil
}

func (p *partitionProducer) Close() error {
	return nil
}
