package pulsar

import (
	"context"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"pulsar-client-go-native/pulsar/impl"
	pb "pulsar-client-go-native/pulsar/pulsar_proto"
	"sync"
	"time"
)

type partitionProducer struct {
	client *client
	topic  string
	log    *log.Entry
	mutex  sync.Mutex
	cond   *sync.Cond
	cnx    impl.Connection

	options             *ProducerOptions
	producerName        *string
	producerId          uint64
	batchBuilder        *impl.BatchBuilder
	sequenceIdGenerator *uint64
	batchFlushTicker    *time.Ticker

	// Channel where app is posting messages to be published
	eventsChan chan interface{}
}

const defaultBatchingMaxPublishDelay = 10 * time.Millisecond

func newPartitionProducer(client *client, topic string, options *ProducerOptions) (*partitionProducer, error) {

	var batchingMaxPublishDelay time.Duration
	if options.BatchingMaxPublishDelay != 0 {
		batchingMaxPublishDelay = options.BatchingMaxPublishDelay
	} else {
		batchingMaxPublishDelay = defaultBatchingMaxPublishDelay
	}

	p := &partitionProducer{
		log:              log.WithField("topic", topic),
		client:           client,
		topic:            topic,
		options:          options,
		producerId:       client.rpcClient.NewProducerId(),
		eventsChan:       make(chan interface{}),
		batchFlushTicker: time.NewTicker(batchingMaxPublishDelay),
	}

	if options.Name != "" {
		p.producerName = &options.Name
	}

	err := p.grabCnx()
	if err != nil {
		log.WithError(err).Errorf("Failed to create producer")
		return nil, err
	} else {
		p.log = p.log.WithField("name", *p.producerName)
		p.log.Info("Created producer")
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

	p.log.Debug("Lookup result: ", lr)
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
	if p.batchBuilder == nil {
		p.batchBuilder = impl.NewBatchBuilder(p.options.BatchingMaxMessages, *p.producerName, p.producerId)
	}
	if p.sequenceIdGenerator == nil {
		nextSequenceId := uint64(res.Response.ProducerSuccess.GetLastSequenceId() + 1)
		p.sequenceIdGenerator = &nextSequenceId
	}
	p.cnx = res.Cnx
	p.log.WithField("cnx", res.Cnx).Debug("Connected producer")
	return nil
}

func (p *partitionProducer) run() {
	for {
		select {
		case i := <-p.eventsChan:
			switch v := i.(type) {
			case *sendRequest:
				p.internalSend(v)
			}

		case _ = <-p.batchFlushTicker.C:
			p.internalFlush()
		}
	}
}

func (p *partitionProducer) Topic() string {
	return p.topic
}

func (p *partitionProducer) Name() string {
	return *p.producerName
}

func (p *partitionProducer) internalSend(request *sendRequest) {
	p.log.Debug("Received send request: ", *request)

	msg := request.msg

	if msg.ReplicationClusters == nil {
		smm := &pb.SingleMessageMetadata{
			PayloadSize: proto.Int(len(msg.Payload)),
		}

		if msg.EventTime != nil {
			smm.EventTime = proto.Uint64(impl.TimestampMillis(*msg.EventTime))
		}

		if msg.Key != "" {
			smm.PartitionKey = &msg.Key
		}

		if msg.Properties != nil {
			smm.Properties = impl.ConvertFromStringMap(msg.Properties)
		}

		sequenceId := impl.GetAndAdd(p.sequenceIdGenerator, 1)
		for ; p.batchBuilder.Add(smm, sequenceId, msg.Payload) == false; {
			// The current batch is full.. flush it and retry
			p.internalFlush()
		}
	} else {
		p.log.Panic("TODO: serialize into single message")
	}
}

func (p *partitionProducer) internalFlush() {
	batchData := p.batchBuilder.Flush()
	if batchData == nil {
		return
	}

	p.cnx.WriteData(batchData)
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
