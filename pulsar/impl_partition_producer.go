package pulsar

import (
	"context"
	log "github.com/sirupsen/logrus"
	"pulsar-client-go-native/pulsar/impl"
	pb "pulsar-client-go-native/pulsar/pulsar_proto"
	"sync"
)

type partitionProducer struct {
	client *client
	topic  string
	log    *log.Entry
	mutex  sync.Mutex
	cond   *sync.Cond
	cnx    impl.Connection

	producerName *string
	producerId   uint64

	// Channel where app is posting messages to be published
	eventsChan chan interface{}
}

func newPartitionProducer(client *client, topic string, options *ProducerOptions) (*partitionProducer, error) {

	p := &partitionProducer{
		log:        log.WithField("topic", topic),
		client:     client,
		topic:      topic,
		producerId: client.rpcClient.NewProducerId(),
		eventsChan: make(chan interface{}),
	}

	if options.Name != "" {
		p.producerName = &options.Name
	}

	err := p.grabCnx()
	if err != nil {
		log.WithError(err).Errorf("Failed to create producer")
		return nil, err
	} else {
		log.Info("Created producer on cnx: ")
		go p.run()
		return p, nil
	}
}

func (p *partitionProducer) grabCnx() error {
	lr, err := p.client.lookupService.Lookup(p.topic)
	if err != nil {
		p.log.WithError(err).Warn("Failed to lookup topic")
		return err
	}

	p.log.Info("Lookup result: ", lr)
	id := p.client.rpcClient.NewRequestId()
	res, err := p.client.rpcClient.Request(lr.LogicalAddr, lr.PhysicalAddr, id, pb.BaseCommand_PRODUCER, &pb.CommandProducer{
		RequestId:    &id,
		Topic:        &p.topic,
		Encrypted:    nil,
		Metadata:     nil,
		ProducerId:   &p.producerId,
		ProducerName: p.producerName,
		Schema:       nil,
	})

	if err != nil {
		p.log.WithError(err).Error("Failed to create producer")
		return err
	}

	p.producerName = res.Response.ProducerSuccess.ProducerName
	p.cnx = res.Cnx
	p.log.WithField("cnx", res.Cnx).Info("Created producer")
	return nil
}

func (p *partitionProducer) run() {
	for {
		i := <-p.eventsChan
		switch v := i.(type) {
		case *sendRequest:
			p.log.Info("Received send request: ", v)
			v.callback(nil, v.msg, nil)
		}
	}
}

func (p *partitionProducer) Topic() string {
	return p.topic
}

func (p *partitionProducer) Name() string {
	return *p.producerName
}

func (p *partitionProducer) Send(ctx context.Context, msg *ProducerMessage) error {
	wg := sync.WaitGroup{}
	wg.Add(1)

	var err error

	p.SendAsync(ctx, msg, func(ID MessageID, message *ProducerMessage, e error) {
		err = e
		wg.Done()
	})

	// When sending synchronously we flush immediately to avoid
	// the increased latency and reduced throughput of batching
	if err = p.Flush(); err != nil {
		return err
	}

	wg.Wait()
	return err
	return nil
}

type sendRequest struct {
	ctx      context.Context
	msg      *ProducerMessage
	callback func(MessageID, *ProducerMessage, error)
}

func (p *partitionProducer) SendAsync(ctx context.Context, msg *ProducerMessage, callback func(MessageID, *ProducerMessage, error)) {
	p.eventsChan <- &sendRequest{ctx, msg, callback}
}

func (p *partitionProducer) LastSequenceID() int64 {
	// TODO: return real last sequence id
	return -1
}

func (p *partitionProducer) Flush() error {
	return nil
}

func (p *partitionProducer) Close() error {
	p.log.Info("Closing producer")
	return nil
}
