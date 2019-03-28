package pulsar

import (
	"context"
	"fmt"
	"pulsar-client-go-native/pulsar/impl"
	"sync"
)

type producer struct {
	topic         string
	producers     []Producer
	messageRouter func(ProducerMessage, TopicMetadata) int
}

func newProducer(client *client, options ProducerOptions) (*producer, error) {
	if options.Topic == "" {
		return nil, newError(ResultInvalidTopicName, "Topic name is required for producer")
	}

	p := &producer{
		topic: options.Topic,
	}

	if options.MessageRouter == nil {
		internalRouter := impl.NewDefaultRouter(options.BatchingMaxPublishDelay)
		p.messageRouter = func(message ProducerMessage, metadata TopicMetadata) int {
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

	for i := 0; i < numPartitions; i++ {
		partition := i
		go func() {
			partitionName := fmt.Sprintf("%s-partition-%d", options.Topic, partition)
			prod, err := newPartitionProducer(client, partitionName, &options)
			c <- ProducerError{partition, prod, err}
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

func (p *producer) Send(ctx context.Context, msg ProducerMessage) error {
	wg := sync.WaitGroup{}
	wg.Add(1)

	var err error

	p.SendAsync(ctx, msg, func(message ProducerMessage, e error) {
		err = e
		wg.Done()
	})

	wg.Wait()
	return err
}

func (p *producer) SendAsync(ctx context.Context, msg ProducerMessage, callback func(ProducerMessage, error)) {
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
	return nil
}

func (p *producer) Close() error {
	return nil
}
