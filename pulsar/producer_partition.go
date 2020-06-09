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

const (
	// producer states
	producerInit int32 = iota
	producerReady
	producerClosing
	producerClosed
)

var (
	errFailAddBatch    = errors.New("message send failed")
	errMessageTooLarge = errors.New("message size exceeds MaxMessageSize")
	ErrNilContextPass  = errors.New("can't pass an nil context")
)

type partitionProducer struct {
	state  int32
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

	publishSemaphore     internal.Semaphore
	pendingQueue         internal.BlockingQueue
	checkTimeoutQueue    internal.BlockingQueue
	stopCheckTimeoutChan chan interface{}
	lastSequenceID       int64

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
		state:                  producerInit,
		log:                    log.WithField("topic", topic),
		client:                 client,
		topic:                  topic,
		options:                options,
		producerID:             client.rpcClient.NewProducerID(),
		eventsChan:             make(chan interface{}, 10),
		batchFlushTicker:       time.NewTicker(batchingMaxPublishDelay),
		checkSendTimeoutTicker: time.NewTicker(sendTimeoutCheckInterval),
		publishSemaphore:       make(internal.Semaphore, maxPendingMessages),
		pendingQueue:           internal.NewBlockingQueue(maxPendingMessages),
		checkTimeoutQueue:      internal.NewBlockingQueue(maxPendingMessages),
		stopCheckTimeoutChan:   make(chan interface{}, 1),
		lastSequenceID:         -1,
		partitionIdx:           partitionIdx,
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
	atomic.StoreInt32(&p.state, producerReady)

	go p.runEventsLoop()
	go p.checkTimeoutEventsLoop()

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
		internal.IterateFunc(func(item interface{}) bool {
			p.cnx.WriteData(item.(*pendingItem).batchData)
			return true
		}))

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
		if atomic.LoadInt32(&p.state) != producerReady {
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

func (p *partitionProducer) checkTimeoutEventsLoop() {
	for {
		select {
		case <-p.checkSendTimeoutTicker.C:
			p.internalCheckSendTimeout()
		case <-p.stopCheckTimeoutChan:
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

	// if msg is too large
	if len(msg.Payload) > int(p.cnx.GetMaxMessageSize()) {
		p.publishSemaphore.Release()
		request.CallBack(nil, request.msg, errMessageTooLarge)
		p.log.WithField("size", len(msg.Payload)).
			WithField("properties", msg.Properties).
			WithError(errMessageTooLarge).Error()
		return
	}

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

	// check after flush before add to batch
	if _, done := request.responseCallBackIfCtxDone(); done {
		p.publishSemaphore.Release()
		return
	}

	added := p.batchBuilder.Add(smm, sequenceID, msg.Payload, request,
		msg.ReplicationClusters, deliverAt)
	if !added {
		// The current batch is full.. flush it and retry
		p.internalFlushCurrentBatch()

		// check after flush before add to batch
		if _, done := request.responseCallBackIfCtxDone(); done {
			p.publishSemaphore.Release()
			return
		}
		// after flushing try again to add the current payload
		if ok := p.batchBuilder.Add(smm, sequenceID, msg.Payload, request,
			msg.ReplicationClusters, deliverAt); !ok {
			p.publishSemaphore.Release()
			request.CallBack(nil, request.msg, errFailAddBatch)
			p.log.WithField("size", len(msg.Payload)).
				WithField("sequenceID", sequenceID).
				WithField("properties", msg.Properties).
				Error("unable to add message to batch")
			return
		}
	}

	if !sendAsBatch || request.flushImmediately {
		p.internalFlushCurrentBatch()
	}
}

func (p *partitionProducer) internalCheckSendTimeout() {
	// check items in pendingQueue until one not all expire
	var checkNext = true
	for checkNext {
		_, empty, satisfy := p.checkTimeoutQueue.PollIfSatisfy(func(item interface{}) bool {
			sr := item.(*sendRequest)
			_, done := sr.responseCallBackIfCtxDone()
			return done
		})
		checkNext = !empty && satisfy
	}
}

type pendingItem struct {
	sync.Mutex
	batchData    []byte
	sequenceID   uint64
	sendRequests []interface{}
	completed    bool
}

// need Lock() before call this func
func (pi *pendingItem) checkRequestsContextDone() (allRequestDone bool) {
	allRequestDone = true
	for _, i := range pi.sendRequests {
		sr := i.(*sendRequest)
		if _, callBacked := sr.responseCallBackIfCtxDone(); !callBacked {
			allRequestDone = false
		}
	}

	return allRequestDone
}

func (p *partitionProducer) internalFlushCurrentBatch() {
	batchData, sequenceID, callbacks := p.batchBuilder.Flush()
	if batchData == nil {
		return
	}

	pi := &pendingItem{
		batchData:    batchData,
		sequenceID:   sequenceID,
		sendRequests: callbacks,
	}

	// check send timeout before we publish message
	var allContextDone bool
	pi.Lock()
	allContextDone = pi.checkRequestsContextDone()
	pi.Unlock()

	if !allContextDone {
		p.pendingQueue.Put(pi)
		p.cnx.WriteData(batchData)
	} else {
		log.Debugf("all request context Done while waiting for flush. sequenceID %v", pi.sequenceID)
		for _, i := range pi.sendRequests {
			sr := i.(*sendRequest)
			// still set lastSequenceId here
			if sr.msg != nil {
				atomic.StoreInt64(&p.lastSequenceID, int64(pi.sequenceID))
				p.publishSemaphore.Release()
			}
		}
	}
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

	sendReq := newsendRequest(context.Background(),
		nil,
		func(id MessageID, message *ProducerMessage, e error) {
			fr.err = e
			fr.waitGroup.Done()
		},
		false)

	pi.sendRequests = append(pi.sendRequests, sendReq)
}

func (p *partitionProducer) Send(ctx context.Context, msg *ProducerMessage) (MessageID, error) {
	if ctx == nil {
		return nil, ErrNilContextPass
	}
	chanWaitGroup := make(chan struct{}, 1)

	var err error
	var msgID MessageID

	p.internalSendAsync(ctx, msg, func(ID MessageID, message *ProducerMessage, e error) {
		err = e
		msgID = ID
		chanWaitGroup <- struct{}{}
	}, true)

	for {
		select {
		case <-chanWaitGroup:
			close(chanWaitGroup)
			return msgID, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *partitionProducer) SendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error)) {
	if ctx == nil {
		callback(nil, msg, ErrNilContextPass)
	}

	sr := newsendRequest(ctx, msg, callback, false)

	if sr.needCheckTimeoutOrCancel() {
		p.checkTimeoutQueue.Put(sr)
	}

	// may be message will block on eventChan publish event
	checkContextIfBlockOnEventChan := func() (ctxDone bool) {
		for {
			select {
			case p.eventsChan <- sr:
				return false
			case <-ctx.Done():
				log.Debugf("send timeout because for eventChan Block %v", msg)
				sr.CallBack(nil, msg, ctx.Err())
				return true
			}
		}
	}

	// may be message will block on acquire semaphore
	for {
		select {
		case p.publishSemaphore <- true:
			checkContextIfBlockOnEventChan()
			return
		case <-ctx.Done():
			log.Debugf("send timeout because for publishSemaphore %v", msg)
			sr.CallBack(nil, msg, ctx.Err())
			return
		}
	}
}

func (p *partitionProducer) internalSendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error), flushImmediately bool) {
	sr := newsendRequest(ctx, msg, callback, flushImmediately)

	if sr.needCheckTimeoutOrCancel() {
		p.checkTimeoutQueue.Put(sr)
	}

	// may be message will block on eventChan publish event
	checkContextIfBlockOnEventChan := func() (ctxDone bool) {
		for {
			select {
			case p.eventsChan <- sr:
				return false
			case <-ctx.Done():
				log.Debugf("send timeout because for eventChan Block %v", msg)
				sr.CallBack(nil, msg, ctx.Err())
				return true
			}
		}
	}

	// may be message will block on acquire semaphore
	for {
		select {
		case p.publishSemaphore <- true:
			checkContextIfBlockOnEventChan()
			return
		case <-ctx.Done():
			log.Debugf("send timeout because for publishSemaphore %v", msg)
			sr.CallBack(nil, msg, ctx.Err())
			return
		}
	}
}

func (p *partitionProducer) ReceivedSendReceipt(response *pb.CommandSendReceipt) {

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

		// maybe sendRequest.ctx has already timeout or canceled
		// still set lastSequenceId here
		if sr.msg != nil {
			atomic.StoreInt64(&p.lastSequenceID, int64(pi.sequenceID))
			p.publishSemaphore.Release()
		}

		msgID := newMessageID(
			int64(response.MessageId.GetLedgerId()),
			int64(response.MessageId.GetEntryId()),
			idx,
			p.partitionIdx,
		)

		sr.CallBack(msgID, sr.msg, nil)
	}

	// Mark this pending item as done
	pi.completed = true
}

func (p *partitionProducer) internalClose(req *closeProducer) {
	defer req.waitGroup.Done()
	if !atomic.CompareAndSwapInt32(&p.state, producerReady, producerClosing) {
		return
	}

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

	atomic.StoreInt32(&p.state, producerClosed)
	p.cnx.UnregisterListener(p.producerID)
	p.batchFlushTicker.Stop()
	p.checkSendTimeoutTicker.Stop()
	p.stopCheckTimeoutChan <- struct{}{}
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
	if atomic.LoadInt32(&p.state) != producerReady {
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
	callback         *onceCallback
	flushImmediately bool

	sync.Mutex
	callbackCalled bool
}

type onceCallback struct {
	once     sync.Once
	callback func(MessageID, *ProducerMessage, error)
}

func (request *sendRequest) CallBack(messageID MessageID, msg *ProducerMessage, err error) {
	request.callback.once.Do(func() {
		request.callback.callback(messageID, msg, err)
	})
}

var defaultCallback = func(MessageID, *ProducerMessage, error) {}

func newsendRequest(ctx context.Context,
	msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error),
	flushImmediately bool,
) *sendRequest {

	var cb func(MessageID, *ProducerMessage, error)
	if callback == nil {
		cb = defaultCallback
	} else {
		cb = callback
	}

	return &sendRequest{
		ctx: ctx,
		msg: msg,
		callback: &onceCallback{
			callback: cb,
		},
		flushImmediately: flushImmediately,
	}
}

func (request *sendRequest) responseCallBackIfCtxDone() (callbackCalledByThisCall bool, callBackCalled bool) {
	request.Lock()
	defer request.Unlock()

	if request.callbackCalled {
		return false, request.callbackCalled
	}

	select {
	case <-request.ctx.Done():
		request.CallBack(nil, request.msg, request.ctx.Err())
		request.callbackCalled = true
		return true, request.callbackCalled
	default:
		return false, request.callbackCalled
	}
}

func (request *sendRequest) needCheckTimeoutOrCancel() bool {
	ctx := request.ctx
	if ctx == nil {
		return false
	}
	// if ctx won't be cancel
	return ctx.Done() != nil
}

type closeProducer struct {
	waitGroup *sync.WaitGroup
}

type flushRequest struct {
	waitGroup *sync.WaitGroup
	err       error
}
