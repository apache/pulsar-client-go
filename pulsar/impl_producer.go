package pulsar

import (
	"context"
	"pulsar-client-go-native/pulsar/impl"
)

type producer struct {
	topic         string
	producers     []Producer
	messageRouter func(*ProducerMessage, TopicMetadata) int
}

func newProducer(client *client, options *ProducerOptions) (*producer, error) {
	if options.Topic == "" {
		return nil, newError(ResultInvalidTopicName, "Topic name is required for producer")
	}

	p := &producer{
		topic: options.Topic,
	}

	if options.MessageRouter == nil {
		internalRouter := impl.NewDefaultRouter(options.BatchingMaxPublishDelay)
		p.messageRouter = func(message *ProducerMessage, metadata TopicMetadata) int {
			return internalRouter(metadata.NumPartitions())
		}
	}

	partitions, err := client.TopicPartitions(options.Topic)
	if err != nil {
		return nil, err
	}

	numPartitions := len(partitions)
	p.producers = make([]Producer, numPartitions)

	type ProducerError struct {
		partition int
		Producer
		error
	}

	c := make(chan ProducerError, numPartitions)

	for partitionIdx, partition := range partitions {
		go func() {
			prod, err := newPartitionProducer(client, partition, options)
			c <- ProducerError{partitionIdx, prod, err}
		}()
	}

	for i := 0; i < numPartitions; i++ {
		pe := <-c
		err = pe.error
		p.producers[pe.partition] = pe.Producer
	}

	if err != nil {
		// Since there were some failures, cleanup all the partitions that succeeded in creating the producers
		for _, producer := range p.producers {
			if producer != nil {
				_ = producer.Close()
			}
		}
		return nil, err
	} else {
		return p, nil
	}
}

func (p *producer) Topic() string {
	return p.topic
}

func (p *producer) Name() string {
	return p.producers[0].Name()
}

func (p *producer) NumPartitions() uint32 {
	return uint32(len(p.producers))
}

func (p *producer) Send(ctx context.Context, msg *ProducerMessage) error {
	partition := p.messageRouter(msg, p)
	return p.producers[partition].Send(ctx, msg)
}

func (p *producer) SendAsync(ctx context.Context, msg *ProducerMessage, callback func(MessageID, *ProducerMessage, error)) {
	partition := p.messageRouter(msg, p)
	p.producers[partition].SendAsync(ctx, msg, callback)
}

func (p *producer) LastSequenceID() int64 {
	var maxSeq int64 = -1
	for _, pp := range p.producers {
		s := pp.LastSequenceID()
		if s > maxSeq {
			maxSeq = s
		}
	}
	return maxSeq
}

func (p *producer) Flush() error {
	var err error = nil
	for _, pp := range p.producers {
		if e := pp.Flush(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (p *producer) Close() error {
	var err error = nil
	for _, pp := range p.producers {
		if e := pp.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}
