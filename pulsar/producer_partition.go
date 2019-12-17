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
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	log "github.com/sirupsen/logrus"

	"github.com/apache/pulsar-client-go/pkg/pb"
	"github.com/apache/pulsar-client-go/pulsar/internal"
)

type producerState int

const (
	producerInit producerState = iota
	producerReady
	producerClosing
	producerClosed
)

type partitionProducer struct {
	state  producerState
	client *client
	topic  string
	log    *log.Entry
	cnx    internal.Connection

	options             *ProducerOptions
	producerName        *string
	producerID          uint64
	batchBuilder        *internal.BatchBuilder
	sequenceIDGenerator *uint64
	batchFlushTicker    *time.Ticker

	// Channel where app is posting messages to be published
	eventsChan chan interface{}

	publishSemaphore internal.Semaphore
	pendingQueue     internal.BlockingQueue
	lastSequenceID   int64

	partitionIdx int
}

const defaultBatchingMaxPublishDelay = 10 * time.Millisecond

func newPartitionProducer(client *client, topic string, options *ProducerOptions, partitionIdx int) (*partitionProducer, error) {

	var batchingMaxPublishDelay time.Duration
	if options.BatchingMaxPublishDelay != 0 {
		batchingMaxPublishDelay = options.BatchingMaxPublishDelay
	} else {
		batchingMaxPublishDelay = defaultBatchingMaxPublishDelay
	}

	var maxPendingMessages int
	if options.MaxPendingMessages == 0 {
		maxPendingMessages = 1000
	} else {
		maxPendingMessages = options.MaxPendingMessages
	}

	p := &partitionProducer{
		state:            producerInit,
		log:              log.WithField("topic", topic),
		client:           client,
		topic:            topic,
		options:          options,
		producerID:       client.rpcClient.NewProducerID(),
		eventsChan:       make(chan interface{}, 1),
		batchFlushTicker: time.NewTicker(batchingMaxPublishDelay),
		publishSemaphore: make(internal.Semaphore, maxPendingMessages),
		pendingQueue:     internal.NewBlockingQueue(maxPendingMessages),
		lastSequenceID:   -1,
		partitionIdx:     partitionIdx,
	}

	if options.Name != "" {
		p.producerName = &options.Name
	}

	err := p.grabCnx()
	if err != nil {
		log.WithError(err).Errorf("Failed to create producer")
		return nil, err
	}

	p.log = p.log.WithField("name", p.producerName)
	p.log.Info("Created producer")
	p.state = producerReady

	go p.runEventsLoop()

	return p, nil
}

func (p *partitionProducer) grabCnx() error {
	lr, err := p.client.lookupService.Lookup(p.topic)
	if err != nil {
		p.log.WithError(err).Warn("Failed to lookup topic")
		return err
	}

	p.log.Debug("Lookup result: ", lr)
	id := p.client.rpcClient.NewRequestID()
	cmdProducer := &pb.CommandProducer{
		RequestId:    proto.Uint64(id),
		Topic:        proto.String(p.topic),
		Encrypted:    nil,
		ProducerId:   proto.Uint64(p.producerID),
		ProducerName: p.producerName,
		Schema:       nil,
	}
	if len(p.options.Properties) > 0 {
		cmdProducer.Metadata = toKeyValues(p.options.Properties)
	}

	res, err := p.client.rpcClient.Request(lr.LogicalAddr, lr.PhysicalAddr, id, pb.BaseCommand_PRODUCER, cmdProducer)
	if err != nil {
		p.log.WithError(err).Error("Failed to create producer")
		return err
	}

	p.producerName = res.Response.ProducerSuccess.ProducerName
	if p.batchBuilder == nil {
		p.batchBuilder, err = internal.NewBatchBuilder(p.options.BatchingMaxMessages, *p.producerName,
			p.producerID, pb.CompressionType(p.options.CompressionType))
		if err != nil {
			return err
		}
	}
	if p.sequenceIDGenerator == nil {
		nextSequenceID := uint64(res.Response.ProducerSuccess.GetLastSequenceId() + 1)
		p.sequenceIDGenerator = &nextSequenceID
	}
	p.cnx = res.Cnx
	p.cnx.RegisterListener(p.producerID, p)
	p.log.WithField("cnx", res.Cnx).Debug("Connected producer")

	if p.pendingQueue.Size() > 0 {
		p.log.Infof("Resending %d pending batches", p.pendingQueue.Size())
		for it := p.pendingQueue.Iterator(); it.HasNext(); {
			p.cnx.WriteData(it.Next().(*pendingItem).batchData)
		}
	}
	return nil
}

type connectionClosed struct{}

func (p *partitionProducer) ConnectionClosed() {
	// Trigger reconnection in the produce goroutine
	p.eventsChan <- &connectionClosed{}
}

func (p *partitionProducer) reconnectToBroker() {
	backoff := internal.Backoff{}
	for {
		if p.state != producerReady {
			// Producer is already closing
			return
		}

		d := backoff.Next()
		p.log.Info("Reconnecting to broker in ", d)
		time.Sleep(d)

		err := p.grabCnx()
		if err == nil {
			// Successfully reconnected
			p.log.Info("Reconnected producer to broker")
			return
		}
	}
}

func (p *partitionProducer) runEventsLoop() {
	for {
		select {
		case i := <-p.eventsChan:
			switch v := i.(type) {
			case *sendRequest:
				p.internalSend(v)
			case *connectionClosed:
				p.reconnectToBroker()
			case *flushRequest:
				p.internalFlush(v)
			case *closeProducer:
				p.internalClose(v)
				return
			}

		case _ = <-p.batchFlushTicker.C:
			p.internalFlushCurrentBatch()
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

	sendAsBatch := !p.options.DisableBatching && msg.ReplicationClusters == nil
	smm := &pb.SingleMessageMetadata{
		PayloadSize: proto.Int(len(msg.Payload)),
	}

	if msg.EventTime != nil {
		smm.EventTime = proto.Uint64(internal.TimestampMillis(*msg.EventTime))
	}

	if msg.Key != "" {
		smm.PartitionKey = &msg.Key
	}

	if msg.Properties != nil {
		smm.Properties = internal.ConvertFromStringMap(msg.Properties)
	}

	var sequenceID uint64
	if msg.SequenceID != nil {
		sequenceID = uint64(*msg.SequenceID)
	} else {
		sequenceID = internal.GetAndAdd(p.sequenceIDGenerator, 1)
	}

	if sendAsBatch {
		added := p.batchBuilder.Add(smm, sequenceID, msg.Payload, request, msg.ReplicationClusters)
		if !added {
			// The current batch is full.. flush it and retry
			p.internalFlushCurrentBatch()

			// after flushing try again to add the current payload
			if ok := p.batchBuilder.Add(smm, sequenceID, msg.Payload, request, msg.ReplicationClusters); !ok {
				p.log.WithField("size", len(msg.Payload)).
					WithField("sequenceID", sequenceID).
					WithField("properties", msg.Properties).
					Error("unable to add message to batch")
			}
		}
	} else {
		// Send individually
		if added := p.batchBuilder.Add(smm, sequenceID, msg.Payload, request, msg.ReplicationClusters); !added {
			p.log.WithField("size", len(msg.Payload)).
				WithField("sequenceID", sequenceID).
				WithField("properties", msg.Properties).
				Error("unable to send single message")
		}
		p.internalFlushCurrentBatch()
	}

	if request.flushImmediately {
		p.internalFlushCurrentBatch()
	}
}

type pendingItem struct {
	sync.Mutex
	batchData    []byte
	sequenceID   uint64
	sendRequests []interface{}
}

func (p *partitionProducer) internalFlushCurrentBatch() {
	batchData, sequenceID, callbacks := p.batchBuilder.Flush()
	if batchData == nil {
		return
	}

	p.pendingQueue.Put(&pendingItem{
		batchData:    batchData,
		sequenceID:   sequenceID,
		sendRequests: callbacks,
	})
	p.cnx.WriteData(batchData)
}

func (p *partitionProducer) internalFlush(fr *flushRequest) {
	p.internalFlushCurrentBatch()

	pi, ok := p.pendingQueue.PeekLast().(*pendingItem)
	if !ok {
		fr.waitGroup.Done()
		return
	}

	sendReq := &sendRequest{
		msg: nil,
		callback: func(id MessageID, message *ProducerMessage, e error) {
			fr.err = e
			fr.waitGroup.Done()
		},
	}

	// lock the pending request while adding requests
	// since the ReceivedSendReceipt func iterates over this list
	pi.Lock()
	pi.sendRequests = append(pi.sendRequests, sendReq)
	pi.Unlock()
}

func (p *partitionProducer) Send(ctx context.Context, msg *ProducerMessage) error {
	wg := sync.WaitGroup{}
	wg.Add(1)

	var err error

	p.internalSendAsync(ctx, msg, func(ID MessageID, message *ProducerMessage, e error) {
		err = e
		wg.Done()
	}, true)

	// When sending synchronously we flush immediately to avoid
	// the increased latency and reduced throughput of batching
	if err = p.Flush(); err != nil {
		return err
	}

	wg.Wait()
	return err
}

func (p *partitionProducer) SendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error)) {
	p.publishSemaphore.Acquire()
	sr := &sendRequest{
		ctx:              ctx,
		msg:              msg,
		callback:         callback,
		flushImmediately: false,
	}
	p.eventsChan <- sr
}

func (p *partitionProducer) internalSendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error), flushImmediately bool) {
	p.publishSemaphore.Acquire()
	sr := &sendRequest{
		ctx:              ctx,
		msg:              msg,
		callback:         callback,
		flushImmediately: flushImmediately,
	}
	p.eventsChan <- sr
}

func (p *partitionProducer) ReceivedSendReceipt(response *pb.CommandSendReceipt) {
	pi, ok := p.pendingQueue.Peek().(*pendingItem)

	if !ok {
		p.log.Warnf("Received ack for %v although the pending queue is empty", response.GetMessageId())
		return
	}

	if pi.sequenceID != response.GetSequenceId() {
		p.log.Warnf("Received ack for %v on sequenceId %v - expected: %v", response.GetMessageId(),
			response.GetSequenceId(), pi.sequenceID)
		return
	}

	// The ack was indeed for the expected item in the queue, we can remove it and trigger the callback
	p.pendingQueue.Poll()

	// lock the pending item while sending the requests
	pi.Lock()
	defer pi.Unlock()
	for idx, i := range pi.sendRequests {
		sr := i.(*sendRequest)
		if sr.msg != nil {
			atomic.StoreInt64(&p.lastSequenceID, int64(pi.sequenceID))
			p.publishSemaphore.Release()
		}

		if sr.callback != nil {
			msgID := newMessageID(
				int64(response.MessageId.GetLedgerId()),
				int64(response.MessageId.GetEntryId()),
				idx,
				p.partitionIdx,
			)
			sr.callback(msgID, sr.msg, nil)
		}
	}
}

func (p *partitionProducer) internalClose(req *closeProducer) {
	defer req.waitGroup.Done()
	if p.state != producerReady {
		return
	}

	p.state = producerClosing
	p.log.Info("Closing producer")

	id := p.client.rpcClient.NewRequestID()
	_, err := p.client.rpcClient.RequestOnCnx(p.cnx, id, pb.BaseCommand_CLOSE_PRODUCER, &pb.CommandCloseProducer{
		ProducerId: &p.producerID,
		RequestId:  &id,
	})

	if err != nil {
		p.log.WithError(err).Warn("Failed to close producer")
	} else {
		p.log.Info("Closed producer")
	}

	p.state = producerClosed
	p.cnx.UnregisterListener(p.producerID)
	p.batchFlushTicker.Stop()
}

func (p *partitionProducer) LastSequenceID() int64 {
	return atomic.LoadInt64(&p.lastSequenceID)
}

func (p *partitionProducer) Flush() error {
	wg := sync.WaitGroup{}
	wg.Add(1)

	cp := &flushRequest{&wg, nil}
	p.eventsChan <- cp

	wg.Wait()
	return cp.err
}

func (p *partitionProducer) Close() {
	if p.state != producerReady {
		// Producer is closing
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	cp := &closeProducer{&wg}
	p.eventsChan <- cp

	wg.Wait()
}

type sendRequest struct {
	ctx              context.Context
	msg              *ProducerMessage
	callback         func(MessageID, *ProducerMessage, error)
	flushImmediately bool
}

type closeProducer struct {
	waitGroup *sync.WaitGroup
}

type flushRequest struct {
	waitGroup *sync.WaitGroup
	err       error
}
