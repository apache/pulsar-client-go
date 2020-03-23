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

package pulsar

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
)

type producer struct {
	client        *client
	topic         string
	producers     []Producer
	messageRouter func(*ProducerMessage, TopicMetadata) int
}

const defaultBatchingMaxPublishDelay = 10 * time.Millisecond

func getHashingFunction(s HashingScheme) func(string) uint32 {
	switch s {
	case JavaStringHash:
		return internal.JavaStringHash
	case Murmur3_32Hash:
		return internal.Murmur3_32Hash
	default:
		return internal.JavaStringHash
	}
}

func newProducer(client *client, options *ProducerOptions) (*producer, error) {
	if options.Topic == "" {
		return nil, newError(ResultInvalidTopicName, "Topic name is required for producer")
	}

	p := &producer{
		topic:  options.Topic,
		client: client,
	}

	var batchingMaxPublishDelay time.Duration
	if options.BatchingMaxPublishDelay != 0 {
		batchingMaxPublishDelay = options.BatchingMaxPublishDelay
	} else {
		batchingMaxPublishDelay = defaultBatchingMaxPublishDelay
	}

	if options.MessageRouter == nil {
		internalRouter := internal.NewDefaultRouter(
			internal.NewSystemClock(),
			getHashingFunction(options.HashingScheme),
			batchingMaxPublishDelay)
		p.messageRouter = func(message *ProducerMessage, metadata TopicMetadata) int {
			return internalRouter(message.Key, metadata.NumPartitions())
		}
	} else {
		p.messageRouter = options.MessageRouter
	}

	partitions, err := client.TopicPartitions(options.Topic)
	if err != nil {
		return nil, err
	}

	numPartitions := len(partitions)
	p.producers = make([]Producer, numPartitions)

	type ProducerError struct {
		partition int
		prod      Producer
		err       error
	}

	c := make(chan ProducerError, numPartitions)

	for partitionIdx, partition := range partitions {
		go func(partitionIdx int, partition string) {
			prod, e := newPartitionProducer(client, partition, options, partitionIdx)
			c <- ProducerError{
				partition: partitionIdx,
				prod:      prod,
				err:       e,
			}
		}(partitionIdx, partition)
	}

	for i := 0; i < numPartitions; i++ {
		pe, ok := <-c
		if ok {
			if pe.err != nil {
				err = pe.err
			} else {
				p.producers[pe.partition] = pe.prod
			}
		}
	}

	if err != nil {
		// Since there were some failures, cleanup all the partitions that succeeded in creating the producers
		for _, producer := range p.producers {
			if producer != nil {
				producer.Close()
			}
		}
		return nil, err
	}

	return p, nil
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

func (p *producer) Send(ctx context.Context, msg *ProducerMessage) (MessageID, error) {
	partition := p.messageRouter(msg, p)
	return p.producers[partition].Send(ctx, msg)
}

func (p *producer) SendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error)) {
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
	for _, pp := range p.producers {
		if err := pp.Flush(); err != nil {
			return err
		}

	}
	return nil
}

func (p *producer) Close() {
	for _, pp := range p.producers {
		pp.Close()
	}
	p.client.handlers.Del(p)
}
