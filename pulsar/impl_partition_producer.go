package pulsar

import (
	"context"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"pulsar-client-go-native/pulsar/impl"
	"pulsar-client-go-native/pulsar/impl/util"
	pb "pulsar-client-go-native/pulsar/pulsar_proto"
	"sync"
	"sync/atomic"
	"time"
)

type producerState int

const (
	producerInit = iota
	producerReady
	producerClosing
	producerClosed
)

type partitionProducer struct {
	state  producerState
	client *client
	topic  string
	log    *log.Entry
	cnx    impl.Connection

	options             *ProducerOptions
	producerName        *string
	producerId          uint64
	batchBuilder        *impl.BatchBuilder
	sequenceIdGenerator *uint64
	batchFlushTicker    *time.Ticker

	// Channel where app is posting messages to be published
	eventsChan chan interface{}

	publishSemaphore util.Semaphore
	pendingQueue     util.BlockingQueue
	lastSequenceID   int64
}

const defaultBatchingMaxPublishDelay = 10 * time.Millisecond

func newPartitionProducer(client *client, topic string, options *ProducerOptions) (*partitionProducer, error) {

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
		producerId:       client.rpcClient.NewProducerId(),
		eventsChan:       make(chan interface{}),
		batchFlushTicker: time.NewTicker(batchingMaxPublishDelay),
		publishSemaphore: make(util.Semaphore, maxPendingMessages),
		pendingQueue:     util.NewBlockingQueue(maxPendingMessages),
		lastSequenceID:   -1,
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
		p.state = producerReady
		go p.runEventsLoop()
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
		p.batchBuilder = impl.NewBatchBuilder(p.options.BatchingMaxMessages, *p.producerName,
			p.producerId, pb.CompressionType(p.options.CompressionType))
	}
	if p.sequenceIdGenerator == nil {
		nextSequenceId := uint64(res.Response.ProducerSuccess.GetLastSequenceId() + 1)
		p.sequenceIdGenerator = &nextSequenceId
	}
	p.cnx = res.Cnx
	p.cnx.RegisterListener(p.producerId, p)
	p.log.WithField("cnx", res.Cnx).Debug("Connected producer")

	if p.pendingQueue.Size() > 0 {
		p.log.Infof("Resending %v pending batches", p.pendingQueue.Size())
		for it := p.pendingQueue.Iterator(); it.HasNext(); {
			p.cnx.WriteData(it.Next().(*pendingItem).batchData)
		}
	}
	return nil
}

type connectionClosed struct {
}

func (p *partitionProducer) ConnectionClosed() {
	// Trigger reconnection in the produce goroutine
	p.eventsChan <- &connectionClosed{}
}

func (p *partitionProducer) reconnectToBroker() {
	p.log.Info("Reconnecting to broker")
	backoff := impl.Backoff{}
	for {
		if p.state != producerReady {
			// Producer is already closing
			return
		}

		err := p.grabCnx()
		if err == nil {
			// Successfully reconnected
			return
		}

		d := backoff.Next()
		p.log.Info("Retrying reconnection after ", d)

		time.Sleep(d)
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

	sendAsBatch := !p.options.DisableBatching && request.msg.ReplicationClusters == nil
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

	if sendAsBatch {
		for ; p.batchBuilder.Add(smm, sequenceId, msg.Payload, request, msg.ReplicationClusters) == false; {
			// The current batch is full.. flush it and retry
			p.internalFlushCurrentBatch()
		}
	} else {
		// Send individually
		p.batchBuilder.Add(smm, sequenceId, msg.Payload, request, msg.ReplicationClusters)
		p.internalFlushCurrentBatch()
	}

	if request.flushImmediately {
		p.internalFlushCurrentBatch()
	}
}

type pendingItem struct {
	batchData    []byte
	sequenceId   uint64
	sendRequests []interface{}
}

func (p *partitionProducer) internalFlushCurrentBatch() {
	batchData, sequenceId, callbacks := p.batchBuilder.Flush()
	if batchData == nil {
		return
	}

	p.pendingQueue.Put(&pendingItem{batchData, sequenceId, callbacks})
	p.cnx.WriteData(batchData)
}

func (p *partitionProducer) internalFlush(fr *flushRequest) {
	p.internalFlushCurrentBatch()

	pi := p.pendingQueue.PeekLast().(*pendingItem)
	pi.sendRequests = append(pi.sendRequests, &sendRequest{
		callback: func(id MessageID, message *ProducerMessage, e error) {
			fr.err = e
			fr.waitGroup.Done()
		},
	})
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
	p.eventsChan <- &sendRequest{ctx, msg, callback, false}
}

func (p *partitionProducer) internalSendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error), flushImmediately bool) {
	p.publishSemaphore.Acquire()
	p.eventsChan <- &sendRequest{ctx, msg, callback, flushImmediately}
}

func (p *partitionProducer) ReceivedSendReceipt(response *pb.CommandSendReceipt) {
	pi := p.pendingQueue.Peek().(*pendingItem)

	if pi == nil {
		p.log.Warnf("Received ack for %v although the pending queue is empty", response.GetMessageId())
		return
	} else if pi.sequenceId != response.GetSequenceId() {
		p.log.Warnf("Received ack for %v on sequenceId %v - expected: %v", response.GetMessageId(),
			response.GetSequenceId(), pi.sequenceId)
		return
	}

	// The ack was indeed for the expected item in the queue, we can remove it and trigger the callback
	p.pendingQueue.Poll()
	for _, i := range pi.sendRequests {
		sr := i.(*sendRequest)
		atomic.StoreInt64(&p.lastSequenceID, int64(pi.sequenceId))
		if sr.callback != nil {
			p.publishSemaphore.Release()
			sr.callback(nil, sr.msg, nil)
		}
	}
}

func (p *partitionProducer) internalClose(req *closeProducer) {
	if p.state != producerReady {
		req.waitGroup.Done()
		return
	}

	p.state = producerClosing
	p.log.Info("Closing producer")

	id := p.client.rpcClient.NewRequestId()
	_, err := p.client.rpcClient.RequestOnCnx(p.cnx, id, pb.BaseCommand_CLOSE_PRODUCER, &pb.CommandCloseProducer{
		ProducerId: &p.producerId,
		RequestId:  &id,
	})

	if err != nil {
		req.err = err
	} else {
		p.log.Info("Closed producer")
		p.state = producerClosed
		p.cnx.UnregisterListener(p.producerId)
		p.batchFlushTicker.Stop()
	}

	req.waitGroup.Done()
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

func (p *partitionProducer) Close() error {
	if p.state != producerReady {
		// Producer is closing
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	cp := &closeProducer{&wg, nil}
	p.eventsChan <- cp

	wg.Wait()
	return cp.err
}

type sendRequest struct {
	ctx              context.Context
	msg              *ProducerMessage
	callback         func(MessageID, *ProducerMessage, error)
	flushImmediately bool
}

type closeProducer struct {
	waitGroup *sync.WaitGroup
	err       error
}

type flushRequest struct {
	waitGroup *sync.WaitGroup
	err       error
}
