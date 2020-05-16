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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	log "github.com/sirupsen/logrus"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/internal/pb"
)

type producerState int

const (
	producerInit producerState = iota
	producerReady
	producerClosing
	producerClosed
)

var ErrSendTimeout = errors.New("send time out")
var SendTimeoutNotSet = time.Duration(0)

type partitionProducer struct {
	state  producerState
	client *client
	topic  string
	log    *log.Entry
	cnx    internal.Connection

	options                *ProducerOptions
	producerName           string
	producerID             uint64
	batchBuilder           *internal.BatchBuilder
	sequenceIDGenerator    *uint64
	batchFlushTicker       *time.Ticker
	checkSendTimeoutTicker *time.Ticker

	// Channel where app is posting messages to be published
	eventsChan chan interface{}

	publishSemaphore           internal.Semaphore
	pendingQueue               internal.BlockingQueue
	sendTimeoutSequenceIDQueue internal.BlockingQueue
	lastSequenceID             int64

	partitionIdx int
}

func newPartitionProducer(client *client, topic string, options *ProducerOptions, partitionIdx int) (
	*partitionProducer, error) {
	var batchingMaxPublishDelay time.Duration
	if options.BatchingMaxPublishDelay != 0 {
		batchingMaxPublishDelay = options.BatchingMaxPublishDelay
	} else {
		batchingMaxPublishDelay = defaultBatchingMaxPublishDelay
	}

	var sendTimeoutCheckInterval time.Duration
	if options.SendTimeoutCheckInterval != 0 {
		sendTimeoutCheckInterval = options.SendTimeoutCheckInterval
	} else {
		sendTimeoutCheckInterval = defaultSendTimeoutCheckInterval
	}

	var maxPendingMessages int
	if options.MaxPendingMessages == 0 {
		maxPendingMessages = 1000
	} else {
		maxPendingMessages = options.MaxPendingMessages
	}

	p := &partitionProducer{
		state:                      producerInit,
		log:                        log.WithField("topic", topic),
		client:                     client,
		topic:                      topic,
		options:                    options,
		producerID:                 client.rpcClient.NewProducerID(),
		eventsChan:                 make(chan interface{}, 10),
		batchFlushTicker:           time.NewTicker(batchingMaxPublishDelay),
		checkSendTimeoutTicker:     time.NewTicker(sendTimeoutCheckInterval),
		publishSemaphore:           make(internal.Semaphore, maxPendingMessages),
		pendingQueue:               internal.NewBlockingQueue(maxPendingMessages),
		sendTimeoutSequenceIDQueue: internal.NewBlockingQueue(maxPendingMessages),
		lastSequenceID:             -1,
		partitionIdx:               partitionIdx,
	}

	if options.Name != "" {
		p.producerName = options.Name
	}

	err := p.grabCnx()
	if err != nil {
		log.WithError(err).Errorf("Failed to create producer")
		return nil, err
	}

	p.log = p.log.WithField("producer_name", p.producerName)
	p.log.WithField("cnx", p.cnx.ID()).Info("Created producer")
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
		RequestId:  proto.Uint64(id),
		Topic:      proto.String(p.topic),
		Encrypted:  nil,
		ProducerId: proto.Uint64(p.producerID),
		Schema:     nil,
	}

	if p.producerName != "" {
		cmdProducer.ProducerName = proto.String(p.producerName)
	}

	if len(p.options.Properties) > 0 {
		cmdProducer.Metadata = toKeyValues(p.options.Properties)
	}
	res, err := p.client.rpcClient.Request(lr.LogicalAddr, lr.PhysicalAddr, id, pb.BaseCommand_PRODUCER, cmdProducer)
	if err != nil {
		p.log.WithError(err).Error("Failed to create producer")
		return err
	}

	p.producerName = res.Response.ProducerSuccess.GetProducerName()
	if p.batchBuilder == nil {
		p.batchBuilder, err = internal.NewBatchBuilder(p.options.BatchingMaxMessages, p.producerName,
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
	p.log.WithField("cnx", res.Cnx.ID()).Debug("Connected producer")

	p.log.Infof("Resending %d pending batches", p.pendingQueue.Size())
	p.pendingQueue.IterateIfNonEmpty(
		func(item interface{}) {
			p.cnx.WriteData(item.(*pendingItem).batchData)
		})

	return nil
}

type connectionClosed struct{}

func (p *partitionProducer) ConnectionClosed() {
	// Trigger reconnection in the produce goroutine
	p.log.WithField("cnx", p.cnx.ID()).Warn("Connection was closed")
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
			p.log.WithField("cnx", p.cnx.ID()).Info("Reconnected producer to broker")
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

		case <-p.batchFlushTicker.C:
			p.internalFlushCurrentBatch()
		case <-p.checkSendTimeoutTicker.C:
			p.internalCheckSendTimeout()
		}
	}
}

func (p *partitionProducer) Topic() string {
	return p.topic
}

func (p *partitionProducer) Name() string {
	return p.producerName
}

func (p *partitionProducer) internalSend(request *sendRequest) {
	p.log.Debug("Received send request: ", *request)

	msg := request.msg

	deliverAt := msg.DeliverAt
	if msg.DeliverAfter.Nanoseconds() > 0 {
		deliverAt = time.Now().Add(msg.DeliverAfter)
	}

	sendAsBatch := !p.options.DisableBatching &&
		msg.ReplicationClusters == nil &&
		deliverAt.UnixNano() < 0

	smm := &pb.SingleMessageMetadata{
		PayloadSize: proto.Int(len(msg.Payload)),
	}

	if msg.EventTime.UnixNano() != 0 {
		smm.EventTime = proto.Uint64(internal.TimestampMillis(msg.EventTime))
	}

	if msg.Key != "" {
		smm.PartitionKey = proto.String(msg.Key)
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
		added := p.batchBuilder.Add(smm, sequenceID, msg.Payload, request,
			msg.ReplicationClusters, deliverAt, request.createAt, request.sendTimeout)
		if !added {
			// The current batch is full.. flush it and retry
			p.internalFlushCurrentBatch()

			// check after flush
			if request.Timeout() {
				request.callback(nil, request.msg, ErrSendTimeout)
				p.publishSemaphore.Release()
			}

			// after flushing try again to add the current payload
			if ok := p.batchBuilder.Add(smm, sequenceID, msg.Payload, request,
				msg.ReplicationClusters, deliverAt, request.createAt, request.sendTimeout); !ok {
				p.log.WithField("size", len(msg.Payload)).
					WithField("sequenceID", sequenceID).
					WithField("properties", msg.Properties).
					Error("unable to add message to batch")
			}
		}
	} else {
		// Send individually
		if added := p.batchBuilder.Add(smm, sequenceID, msg.Payload, request,
			msg.ReplicationClusters, deliverAt, request.createAt, request.sendTimeout); !added {
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

func (p *partitionProducer) internalCheckSendTimeout() {
	if p.options.SendTimeout == SendTimeoutNotSet {
		return
	}

	// poll if pendingItem expire
	item, empty, satisfy := p.pendingQueue.PollIfSatisfy(func(item interface{}) bool {
		pi := item.(*pendingItem)
		var now = time.Now()
		expire := pi.createAt.Add(pi.sendTimeout).Before(now)
		if expire {
			p.sendTimeoutSequenceIDQueue.Put(pi.sequenceID)
			p.log.Infof("Ticker check send timeout (%v). Ignored ack on sequenceId %v.", pi.sendTimeout, pi.sequenceID)
		}
		return expire
	})

	if empty || !satisfy {
		return
	}

	pi, ok := item.(*pendingItem)
	if ok {
		pi.Lock()
		defer pi.Unlock()

		for _, i := range pi.sendRequests {
			sr := i.(*sendRequest)
			if sr.msg != nil {
				atomic.StoreInt64(&p.lastSequenceID, int64(pi.sequenceID))
				p.publishSemaphore.Release()
			}

			if sr.callback != nil {
				sr.callback(nil, sr.msg, ErrSendTimeout)
			}
		}

		// Mark this pendingItem as done
		pi.completed = true
	} else {
		p.log.Errorf("error when poll from p.pendingQueue expect pendingItem type")
	}
}

type pendingItem struct {
	sync.Mutex
	batchData    []byte
	sequenceID   uint64
	sendRequests []interface{}
	completed    bool
	createAt     time.Time
	sendTimeout  time.Duration
}

func (p *partitionProducer) internalFlushCurrentBatch() {
	batchData, sequenceID, callbacks, createAt, sendTimeout := p.batchBuilder.Flush()
	if batchData == nil {
		return
	}

	p.pendingQueue.Put(&pendingItem{
		batchData:    batchData,
		sequenceID:   sequenceID,
		sendRequests: callbacks,
		createAt:     createAt,
		sendTimeout:  sendTimeout,
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

	// lock the pending request while adding requests
	// since the ReceivedSendReceipt func iterates over this list
	pi.Lock()
	defer pi.Unlock()

	if pi.completed {
		// The last item in the queue has been completed while we were
		// looking at it. It's safe at this point to assume that every
		// message enqueued before Flush() was called are now persisted
		fr.waitGroup.Done()
		return
	}

	sendReq := &sendRequest{
		msg: nil,
		callback: func(id MessageID, message *ProducerMessage, e error) {
			fr.err = e
			fr.waitGroup.Done()
		},
		flushImmediately: false,
		createAt:         time.Now(),
		// dummy value set here. because we never check this value
		sendTimeout: time.Hour * 96,
	}

	pi.sendRequests = append(pi.sendRequests, sendReq)
}

func (p *partitionProducer) Send(ctx context.Context, msg *ProducerMessage) (MessageID, error) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	var err error
	var msgID MessageID

	p.internalSendAsync(ctx, msg, func(ID MessageID, message *ProducerMessage, e error) {
		err = e
		msgID = ID
		wg.Done()
	}, true)

	wg.Wait()
	return msgID, err
}

func (p *partitionProducer) SendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error)) {
	p.publishSemaphore.Acquire()
	sr := &sendRequest{
		ctx:              ctx,
		msg:              msg,
		callback:         callback,
		flushImmediately: false,
		createAt:         time.Now(),
		sendTimeout:      p.options.SendTimeout,
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
		createAt:         time.Now(),
		sendTimeout:      p.options.SendTimeout,
	}
	p.eventsChan <- sr
}

func (p *partitionProducer) maybeIgnoreTimeoutResponse(responseSequenceID uint64) (item interface{}, ignored bool) {
	item, empty, satisfy := p.sendTimeoutSequenceIDQueue.PollIfSatisfy(func(item interface{}) bool {
		id := item.(uint64)
		return id == responseSequenceID
	})

	if empty {
		return item, false
	}

	return item, satisfy
}

func (p *partitionProducer) ReceivedSendReceipt(response *pb.CommandSendReceipt) {

	// inject point for mock slow response
	p.beforeReceiveResponse(response)

	// maybe internalCheckSendTimeout has removed pendingItem from pendingQueue
	if _, ignored := p.maybeIgnoreTimeoutResponse(response.GetSequenceId()); ignored {
		p.log.Infof("Ignored ack for %v on sequenceId %v because of send timeout.",
			response.GetMessageId(), response.GetSequenceId())
		return
	}
	item, empty, satisfy := p.pendingQueue.PollIfSatisfy(func(item interface{}) bool {
		pi := item.(*pendingItem)
		if pi.sequenceID != response.GetSequenceId() {
			p.log.Warnf("Received ack for %v on sequenceId %v - expected: %v", response.GetMessageId(),
				response.GetSequenceId(), pi.sequenceID)
			return false
		}
		return true
	})

	if empty {
		p.log.Warnf("Received ack for %v although the pending queue is empty", response.GetMessageId())
		return
	} else if !satisfy {
		return
	}

	pi := item.(*pendingItem)

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

	// Mark this pending item as done
	pi.completed = true
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
	p.checkSendTimeoutTicker.Stop()
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

func (p *partitionProducer) beforeReceiveResponse(receipt *pb.CommandSendReceipt) {
	if p.options.beforeReceiveResponseCallback != nil {
		p.options.beforeReceiveResponseCallback(receipt)
	}
}

type sendRequest struct {
	ctx              context.Context
	msg              *ProducerMessage
	callback         func(MessageID, *ProducerMessage, error)
	flushImmediately bool
	sendTimeout      time.Duration
	createAt         time.Time
}

func (request sendRequest) Timeout() bool {
	if request.sendTimeout != SendTimeoutNotSet {
		return request.createAt.Add(request.sendTimeout).Before(time.Now())
	}

	return false
}

type closeProducer struct {
	waitGroup *sync.WaitGroup
}

type flushRequest struct {
	waitGroup *sync.WaitGroup
	err       error
}
