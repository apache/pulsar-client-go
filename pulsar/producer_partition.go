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
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	internalcrypto "github.com/apache/pulsar-client-go/pulsar/internal/crypto"

	"google.golang.org/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"

	uAtomic "go.uber.org/atomic"
)

type producerState int32

const (
	// producer states
	producerInit = iota
	producerReady
	producerClosing
	producerClosed
)

var (
	errFailAddToBatch     = newError(AddToBatchFailed, "message add to batch failed")
	errSendTimeout        = newError(TimeoutError, "message send timeout")
	errSendQueueIsFull    = newError(ProducerQueueIsFull, "producer send queue is full")
	errContextExpired     = newError(TimeoutError, "message send context expired")
	errMessageTooLarge    = newError(MessageTooBig, "message size exceeds MaxMessageSize")
	errMetaTooLarge       = newError(InvalidMessage, "message metadata size exceeds MaxMessageSize")
	errProducerClosed     = newError(ProducerClosed, "producer already been closed")
	errMemoryBufferIsFull = newError(ClientMemoryBufferIsFull, "client memory buffer is full")

	buffersPool sync.Pool
)

var errTopicNotFount = "TopicNotFound"

type partitionProducer struct {
	state  uAtomic.Int32
	client *client
	topic  string
	log    log.Logger

	conn uAtomic.Value

	options                  *ProducerOptions
	producerName             string
	userProvidedProducerName bool
	producerID               uint64
	batchBuilder             internal.BatchBuilder
	sequenceIDGenerator      *uint64
	batchFlushTicker         *time.Ticker
	encryptor                internalcrypto.Encryptor
	compressionProvider      compression.Provider

	// Channel where app is posting messages to be published
	dataChan        chan *sendRequest
	cmdChan         chan interface{}
	connectClosedCh chan connectionClosed

	publishSemaphore internal.Semaphore
	pendingQueue     internal.BlockingQueue
	lastSequenceID   int64
	schemaInfo       *SchemaInfo
	partitionIdx     int32
	metrics          *internal.LeveledMetrics
	epoch            uint64
	schemaCache      *schemaCache
	topicEpoch       *uint64
}

type schemaCache struct {
	schemas sync.Map
}

func newSchemaCache() *schemaCache {
	return &schemaCache{}
}

func (s *schemaCache) Put(schema *SchemaInfo, schemaVersion []byte) {
	key := schema.hash()
	s.schemas.Store(key, schemaVersion)
}

func (s *schemaCache) Get(schema *SchemaInfo) (schemaVersion []byte) {
	val, ok := s.schemas.Load(schema.hash())
	if !ok {
		return nil
	}
	return val.([]byte)
}

func newPartitionProducer(client *client, topic string, options *ProducerOptions, partitionIdx int,
	metrics *internal.LeveledMetrics) (
	*partitionProducer, error) {
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

	logger := client.log.SubLogger(log.Fields{"topic": topic})

	p := &partitionProducer{
		client:           client,
		topic:            topic,
		log:              logger,
		options:          options,
		producerID:       client.rpcClient.NewProducerID(),
		dataChan:         make(chan *sendRequest, maxPendingMessages),
		cmdChan:          make(chan interface{}, 10),
		connectClosedCh:  make(chan connectionClosed, 10),
		batchFlushTicker: time.NewTicker(batchingMaxPublishDelay),
		compressionProvider: internal.GetCompressionProvider(pb.CompressionType(options.CompressionType),
			compression.Level(options.CompressionLevel)),
		publishSemaphore: internal.NewSemaphore(int32(maxPendingMessages)),
		pendingQueue:     internal.NewBlockingQueue(maxPendingMessages),
		lastSequenceID:   -1,
		partitionIdx:     int32(partitionIdx),
		metrics:          metrics,
		epoch:            0,
		schemaCache:      newSchemaCache(),
	}
	if p.options.DisableBatching {
		p.batchFlushTicker.Stop()
	}
	p.setProducerState(producerInit)

	if options.Schema != nil && options.Schema.GetSchemaInfo() != nil {
		p.schemaInfo = options.Schema.GetSchemaInfo()
	} else {
		p.schemaInfo = nil
	}

	if options.Name != "" {
		p.producerName = options.Name
		p.userProvidedProducerName = true
	} else {
		p.userProvidedProducerName = false
	}
	err := p.grabCnx()
	if err != nil {
		p.batchFlushTicker.Stop()
		logger.WithError(err).Error("Failed to create producer at newPartitionProducer")
		return nil, err
	}

	p.log = p.log.SubLogger(log.Fields{
		"producer_name": p.producerName,
		"producerID":    p.producerID,
	})

	p.log.WithField("cnx", p._getConn().ID()).Info("Created producer")
	p.setProducerState(producerReady)

	if p.options.SendTimeout > 0 {
		go p.failTimeoutMessages()
	}

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

	// set schema info for producer

	var pbSchema *pb.Schema
	if p.schemaInfo != nil {
		tmpSchemaType := pb.Schema_Type(int32(p.schemaInfo.Type))
		pbSchema = &pb.Schema{
			Name:       proto.String(p.schemaInfo.Name),
			Type:       &tmpSchemaType,
			SchemaData: []byte(p.schemaInfo.Schema),
			Properties: internal.ConvertFromStringMap(p.schemaInfo.Properties),
		}
		p.log.Debugf("The partition producer schema name is: %s", pbSchema.Name)
	} else {
		p.log.Debug("The partition producer schema is nil")
	}

	cmdProducer := &pb.CommandProducer{
		RequestId:                proto.Uint64(id),
		Topic:                    proto.String(p.topic),
		Encrypted:                nil,
		ProducerId:               proto.Uint64(p.producerID),
		Schema:                   pbSchema,
		Epoch:                    proto.Uint64(atomic.LoadUint64(&p.epoch)),
		UserProvidedProducerName: proto.Bool(p.userProvidedProducerName),
		ProducerAccessMode:       toProtoProducerAccessMode(p.options.ProducerAccessMode).Enum(),
	}

	if p.topicEpoch != nil {
		cmdProducer.TopicEpoch = proto.Uint64(*p.topicEpoch)
	}

	if p.producerName != "" {
		cmdProducer.ProducerName = proto.String(p.producerName)
	}

	if len(p.options.Properties) > 0 {
		cmdProducer.Metadata = toKeyValues(p.options.Properties)
	}
	res, err := p.client.rpcClient.Request(lr.LogicalAddr, lr.PhysicalAddr, id, pb.BaseCommand_PRODUCER, cmdProducer)
	if err != nil {
		p.log.WithError(err).Error("Failed to create producer at send PRODUCER request")
		if errors.Is(err, internal.ErrRequestTimeOut) {
			id := p.client.rpcClient.NewRequestID()
			_, _ = p.client.rpcClient.Request(lr.LogicalAddr, lr.PhysicalAddr, id, pb.BaseCommand_CLOSE_PRODUCER,
				&pb.CommandCloseProducer{
					ProducerId: &p.producerID,
					RequestId:  &id,
				})
		}
		return err
	}

	p.producerName = res.Response.ProducerSuccess.GetProducerName()
	nextTopicEpoch := res.Response.ProducerSuccess.GetTopicEpoch()
	p.topicEpoch = &nextTopicEpoch

	if p.options.Encryption != nil {
		p.encryptor = internalcrypto.NewProducerEncryptor(p.options.Encryption.Keys,
			p.options.Encryption.KeyReader,
			p.options.Encryption.MessageCrypto,
			p.options.Encryption.ProducerCryptoFailureAction, p.log)
	} else {
		p.encryptor = internalcrypto.NewNoopEncryptor()
	}

	if p.sequenceIDGenerator == nil {
		nextSequenceID := uint64(res.Response.ProducerSuccess.GetLastSequenceId() + 1)
		p.sequenceIDGenerator = &nextSequenceID
	}

	schemaVersion := res.Response.ProducerSuccess.GetSchemaVersion()
	if len(schemaVersion) != 0 {
		p.schemaCache.Put(p.schemaInfo, schemaVersion)
	}

	p._setConn(res.Cnx)
	err = p._getConn().RegisterListener(p.producerID, p)
	if err != nil {
		return err
	}

	if !p.options.DisableBatching && p.batchBuilder == nil {
		provider, err := GetBatcherBuilderProvider(p.options.BatcherBuilderType)
		if err != nil {
			return err
		}
		maxMessageSize := uint32(p._getConn().GetMaxMessageSize())
		p.batchBuilder, err = provider(p.options.BatchingMaxMessages, p.options.BatchingMaxSize,
			maxMessageSize, p.producerName, p.producerID, pb.CompressionType(p.options.CompressionType),
			compression.Level(p.options.CompressionLevel),
			p,
			p.log,
			p.encryptor)
		if err != nil {
			return err
		}
	}

	p.log.WithFields(log.Fields{
		"cnx":   res.Cnx.ID(),
		"epoch": atomic.LoadUint64(&p.epoch),
	}).Info("Connected producer")

	pendingItems := p.pendingQueue.ReadableSlice()
	viewSize := len(pendingItems)
	if viewSize > 0 {
		p.log.Infof("Resending %d pending batches", viewSize)
		lastViewItem := pendingItems[viewSize-1].(*pendingItem)

		// iterate at most pending items
		for i := 0; i < viewSize; i++ {
			item := p.pendingQueue.Poll()
			if item == nil {
				continue
			}
			pi := item.(*pendingItem)
			// when resending pending batches, we update the sendAt timestamp and put to the back of queue
			// to avoid pending item been removed by failTimeoutMessages and cause race condition
			pi.Lock()
			pi.sentAt = time.Now()
			pi.Unlock()
			p.pendingQueue.Put(pi)
			p._getConn().WriteData(pi.buffer)

			if pi == lastViewItem {
				break
			}
		}
	}
	return nil
}

type connectionClosed struct{}

func (p *partitionProducer) GetBuffer() internal.Buffer {
	b, ok := buffersPool.Get().(internal.Buffer)
	if ok {
		b.Clear()
	}
	return b
}

func (p *partitionProducer) ConnectionClosed() {
	// Trigger reconnection in the produce goroutine
	p.log.WithField("cnx", p._getConn().ID()).Warn("Connection was closed")
	p.connectClosedCh <- connectionClosed{}
}

func (p *partitionProducer) getOrCreateSchema(schemaInfo *SchemaInfo) (schemaVersion []byte, err error) {

	tmpSchemaType := pb.Schema_Type(int32(schemaInfo.Type))
	pbSchema := &pb.Schema{
		Name:       proto.String(schemaInfo.Name),
		Type:       &tmpSchemaType,
		SchemaData: []byte(schemaInfo.Schema),
		Properties: internal.ConvertFromStringMap(schemaInfo.Properties),
	}
	id := p.client.rpcClient.NewRequestID()
	req := &pb.CommandGetOrCreateSchema{
		RequestId: proto.Uint64(id),
		Topic:     proto.String(p.topic),
		Schema:    pbSchema,
	}
	res, err := p.client.rpcClient.RequestOnCnx(p._getConn(), id, pb.BaseCommand_GET_OR_CREATE_SCHEMA, req)
	if err != nil {
		return
	}
	if res.Response.Error != nil {
		err = errors.New(res.Response.GetError().String())
		return
	}
	if res.Response.GetOrCreateSchemaResponse.ErrorCode != nil {
		err = errors.New(*res.Response.GetOrCreateSchemaResponse.ErrorMessage)
		return
	}
	return res.Response.GetOrCreateSchemaResponse.SchemaVersion, nil
}

func (p *partitionProducer) reconnectToBroker() {
	var maxRetry int
	if p.options.MaxReconnectToBroker == nil {
		maxRetry = -1
	} else {
		maxRetry = int(*p.options.MaxReconnectToBroker)
	}

	var (
		delayReconnectTime time.Duration
		defaultBackoff     = internal.DefaultBackoff{}
	)

	for maxRetry != 0 {
		if p.getProducerState() != producerReady {
			// Producer is already closing
			p.log.Info("producer state not ready, exit reconnect")
			return
		}

		if p.options.BackoffPolicy == nil {
			delayReconnectTime = defaultBackoff.Next()
		} else {
			delayReconnectTime = p.options.BackoffPolicy.Next()
		}
		p.log.Info("Reconnecting to broker in ", delayReconnectTime)
		time.Sleep(delayReconnectTime)
		atomic.AddUint64(&p.epoch, 1)
		err := p.grabCnx()
		if err == nil {
			// Successfully reconnected
			p.log.WithField("cnx", p._getConn().ID()).Info("Reconnected producer to broker")
			return
		}
		p.log.WithError(err).Error("Failed to create producer at reconnect")
		errMsg := err.Error()
		if strings.Contains(errMsg, errTopicNotFount) {
			// when topic is deleted, we should give up reconnection.
			p.log.Warn("Topic Not Found.")
			break
		}

		if maxRetry > 0 {
			maxRetry--
		}
		p.metrics.ProducersReconnectFailure.Inc()
		if maxRetry == 0 || defaultBackoff.IsMaxBackoffReached() {
			p.metrics.ProducersReconnectMaxRetry.Inc()
		}
	}
}

func (p *partitionProducer) runEventsLoop() {
	for {
		select {
		case data := <-p.dataChan:
			p.internalSend(data)
		case i := <-p.cmdChan:
			switch v := i.(type) {
			case *flushRequest:
				p.internalFlush(v)
			case *closeProducer:
				p.internalClose(v)
				return
			}
		case <-p.connectClosedCh:
			p.log.Info("runEventsLoop will reconnect in producer")
			p.reconnectToBroker()
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

func runCallback(cb func(MessageID, *ProducerMessage, error), id MessageID, msg *ProducerMessage, err error) {
	if cb == nil {
		return
	}

	cb(id, msg, err)
}

func (p *partitionProducer) internalSend(sr *sendRequest) {
	p.log.Debug("Received send request: ", *sr.msg)

	if sr.sendAsBatch {
		smm := p.genSingleMessageMetadataInBatch(sr.msg, int(sr.uncompressedSize))
		multiSchemaEnabled := !p.options.DisableMultiSchema

		added := addRequestToBatch(
			smm, p, sr.uncompressedPayload, sr, sr.msg, sr.deliverAt, sr.schemaVersion, multiSchemaEnabled)
		if !added {
			// The current batch is full. flush it and retry
			p.internalFlushCurrentBatch()

			// after flushing try again to add the current payload
			ok := addRequestToBatch(
				smm, p, sr.uncompressedPayload, sr, sr.msg, sr.deliverAt, sr.schemaVersion, multiSchemaEnabled)
			if !ok {
				p.log.WithField("size", sr.uncompressedSize).
					WithField("properties", sr.msg.Properties).
					Error("unable to add message to batch")
				sr.done(nil, errFailAddToBatch)
				return
			}
		}

		if sr.flushImmediately {
			p.internalFlushCurrentBatch()
		}
		return
	}

	if sr.totalChunks <= 1 {
		p.internalSingleSend(sr.mm, sr.compressedPayload, sr, uint32(sr.maxMessageSize))
		return
	}

	var lhs, rhs int
	uuid := fmt.Sprintf("%s-%s", p.producerName, strconv.FormatUint(*sr.mm.SequenceId, 10))
	sr.mm.Uuid = proto.String(uuid)
	sr.mm.NumChunksFromMsg = proto.Int32(int32(sr.totalChunks))
	sr.mm.TotalChunkMsgSize = proto.Int32(int32(sr.compressedSize))
	cr := newChunkRecorder()
	for chunkID := 0; chunkID < sr.totalChunks; chunkID++ {
		lhs = chunkID * sr.payloadChunkSize
		if rhs = lhs + sr.payloadChunkSize; rhs > sr.compressedSize {
			rhs = sr.compressedSize
		}
		// update chunk id
		sr.mm.ChunkId = proto.Int32(int32(chunkID))
		nsr := &sendRequest{
			ctx:                 sr.ctx,
			msg:                 sr.msg,
			producer:            sr.producer,
			callback:            sr.callback,
			callbackOnce:        sr.callbackOnce,
			publishTime:         sr.publishTime,
			flushImmediately:    sr.flushImmediately,
			totalChunks:         sr.totalChunks,
			chunkID:             chunkID,
			uuid:                uuid,
			chunkRecorder:       cr,
			transaction:         sr.transaction,
			memLimit:            sr.memLimit,
			semaphore:           sr.semaphore,
			reservedMem:         int64(rhs - lhs),
			sendAsBatch:         sr.sendAsBatch,
			schema:              sr.schema,
			schemaVersion:       sr.schemaVersion,
			uncompressedPayload: sr.uncompressedPayload,
			uncompressedSize:    sr.uncompressedSize,
			compressedPayload:   sr.compressedPayload,
			compressedSize:      sr.compressedSize,
			payloadChunkSize:    sr.payloadChunkSize,
			mm:                  sr.mm,
			deliverAt:           sr.deliverAt,
			maxMessageSize:      sr.maxMessageSize,
		}

		p.internalSingleSend(nsr.mm, nsr.compressedPayload[lhs:rhs], nsr, uint32(nsr.maxMessageSize))
	}
}

func addRequestToBatch(smm *pb.SingleMessageMetadata, p *partitionProducer,
	uncompressedPayload []byte,
	request *sendRequest, msg *ProducerMessage, deliverAt time.Time,
	schemaVersion []byte, multiSchemaEnabled bool) bool {
	var useTxn bool
	var mostSigBits uint64
	var leastSigBits uint64
	if request.transaction != nil {
		txnID := request.transaction.GetTxnID()
		useTxn = true
		mostSigBits = txnID.MostSigBits
		leastSigBits = txnID.LeastSigBits
	}

	return p.batchBuilder.Add(smm, p.sequenceIDGenerator, uncompressedPayload, request,
		msg.ReplicationClusters, deliverAt, schemaVersion, multiSchemaEnabled, useTxn, mostSigBits,
		leastSigBits)
}

func (p *partitionProducer) genMetadata(msg *ProducerMessage,
	uncompressedSize int,
	deliverAt time.Time) (mm *pb.MessageMetadata) {
	mm = &pb.MessageMetadata{
		ProducerName:     &p.producerName,
		PublishTime:      proto.Uint64(internal.TimestampMillis(time.Now())),
		ReplicateTo:      msg.ReplicationClusters,
		UncompressedSize: proto.Uint32(uint32(uncompressedSize)),
	}

	if !msg.EventTime.IsZero() {
		mm.EventTime = proto.Uint64(internal.TimestampMillis(msg.EventTime))
	}

	if msg.Key != "" {
		mm.PartitionKey = proto.String(msg.Key)
	}

	if len(msg.OrderingKey) != 0 {
		mm.OrderingKey = []byte(msg.OrderingKey)
	}

	if msg.Properties != nil {
		mm.Properties = internal.ConvertFromStringMap(msg.Properties)
	}

	if deliverAt.UnixNano() > 0 {
		mm.DeliverAtTime = proto.Int64(int64(internal.TimestampMillis(deliverAt)))
	}

	return
}

func (p *partitionProducer) updateMetadataSeqID(mm *pb.MessageMetadata, msg *ProducerMessage) {
	if msg.SequenceID != nil {
		mm.SequenceId = proto.Uint64(uint64(*msg.SequenceID))
	} else {
		mm.SequenceId = proto.Uint64(internal.GetAndAdd(p.sequenceIDGenerator, 1))
	}
}

func (p *partitionProducer) updateSingleMessageMetadataSeqID(smm *pb.SingleMessageMetadata, msg *ProducerMessage) {
	if msg.SequenceID != nil {
		smm.SequenceId = proto.Uint64(uint64(*msg.SequenceID))
	} else {
		smm.SequenceId = proto.Uint64(internal.GetAndAdd(p.sequenceIDGenerator, 1))
	}
}

func (p *partitionProducer) genSingleMessageMetadataInBatch(
	msg *ProducerMessage,
	uncompressedSize int,
) (smm *pb.SingleMessageMetadata) {
	smm = &pb.SingleMessageMetadata{
		PayloadSize: proto.Int32(int32(uncompressedSize)),
	}

	if !msg.EventTime.IsZero() {
		smm.EventTime = proto.Uint64(internal.TimestampMillis(msg.EventTime))
	}

	if msg.Key != "" {
		smm.PartitionKey = proto.String(msg.Key)
	}

	if len(msg.OrderingKey) != 0 {
		smm.OrderingKey = []byte(msg.OrderingKey)
	}

	if msg.Properties != nil {
		smm.Properties = internal.ConvertFromStringMap(msg.Properties)
	}

	p.updateSingleMessageMetadataSeqID(smm, msg)

	return
}

func (p *partitionProducer) internalSingleSend(
	mm *pb.MessageMetadata,
	compressedPayload []byte,
	sr *sendRequest,
	maxMessageSize uint32,
) {
	msg := sr.msg

	payloadBuf := internal.NewBuffer(len(compressedPayload))
	payloadBuf.Write(compressedPayload)

	buffer := p.GetBuffer()
	if buffer == nil {
		buffer = internal.NewBuffer(int(payloadBuf.ReadableBytes() * 3 / 2))
	}

	sid := *mm.SequenceId
	var useTxn bool
	var mostSigBits uint64
	var leastSigBits uint64

	if sr.transaction != nil {
		txnID := sr.transaction.GetTxnID()
		useTxn = true
		mostSigBits = txnID.MostSigBits
		leastSigBits = txnID.LeastSigBits
	}

	err := internal.SingleSend(
		buffer,
		p.producerID,
		sid,
		mm,
		payloadBuf,
		p.encryptor,
		maxMessageSize,
		useTxn,
		mostSigBits,
		leastSigBits,
	)

	if err != nil {
		sr.done(nil, err)
		p.log.WithError(err).Errorf("Single message serialize failed %s", msg.Value)
		return
	}

	p.pendingQueue.Put(&pendingItem{
		sentAt:       time.Now(),
		buffer:       buffer,
		sequenceID:   sid,
		sendRequests: []interface{}{sr},
	})
	p._getConn().WriteData(buffer)
}

type pendingItem struct {
	sync.Mutex
	buffer        internal.Buffer
	sequenceID    uint64
	sentAt        time.Time
	sendRequests  []interface{}
	isDone        bool
	flushCallback func(err error)
}

func (p *partitionProducer) internalFlushCurrentBatch() {
	if p.batchBuilder == nil {
		// batch is not enabled
		// the batch flush ticker should be stopped but it might still called once
		// depends on when stop() is called concurrently
		// so we add check to prevent the flow continues on a nil batchBuilder
		return
	}
	if p.batchBuilder.IsMultiBatches() {
		p.internalFlushCurrentBatches()
		return
	}

	batchData, sequenceID, callbacks, err := p.batchBuilder.Flush()
	if batchData == nil {
		return
	}

	// error occurred in batch flush
	// report it using callback
	if err != nil {
		for _, cb := range callbacks {
			if sr, ok := cb.(*sendRequest); ok {
				sr.done(nil, err)
			}
		}

		if errors.Is(err, internal.ErrExceedMaxMessageSize) {
			p.log.WithError(errMessageTooLarge).Errorf("internal err: %s", err)
		}

		return
	}

	p.pendingQueue.Put(&pendingItem{
		sentAt:       time.Now(),
		buffer:       batchData,
		sequenceID:   sequenceID,
		sendRequests: callbacks,
	})
	p._getConn().WriteData(batchData)
}

func (p *partitionProducer) failTimeoutMessages() {
	diff := func(sentAt time.Time) time.Duration {
		return p.options.SendTimeout - time.Since(sentAt)
	}

	t := time.NewTimer(p.options.SendTimeout)
	defer t.Stop()

	for range t.C {
		state := p.getProducerState()
		if state == producerClosing || state == producerClosed {
			return
		}

		item := p.pendingQueue.Peek()
		if item == nil {
			// pending queue is empty
			t.Reset(p.options.SendTimeout)
			continue
		}
		oldestItem := item.(*pendingItem)
		if nextWaiting := diff(oldestItem.sentAt); nextWaiting > 0 {
			// none of these pending messages have timed out, wait and retry
			t.Reset(nextWaiting)
			continue
		}

		// since pending queue is not thread safe because of there is no global iteration lock
		// to control poll from pending queue, current goroutine and connection receipt handler
		// iterate pending queue at the same time, this maybe a performance trade-off
		// see https://github.com/apache/pulsar-client-go/pull/301
		curViewItems := p.pendingQueue.ReadableSlice()
		viewSize := len(curViewItems)
		if viewSize <= 0 {
			// double check
			t.Reset(p.options.SendTimeout)
			continue
		}
		p.log.Infof("Failing %d messages on timeout %s", viewSize, p.options.SendTimeout)
		lastViewItem := curViewItems[viewSize-1].(*pendingItem)

		// iterate at most viewSize items
		for i := 0; i < viewSize; i++ {
			tickerNeedWaiting := time.Duration(0)
			item := p.pendingQueue.CompareAndPoll(
				func(m interface{}) bool {
					if m == nil {
						return false
					}

					pi := m.(*pendingItem)
					pi.Lock()
					defer pi.Unlock()
					if nextWaiting := diff(pi.sentAt); nextWaiting > 0 {
						// current and subsequent items not timeout yet, stop iterating
						tickerNeedWaiting = nextWaiting
						return false
					}
					return true
				})

			if item == nil {
				t.Reset(p.options.SendTimeout)
				break
			}

			if tickerNeedWaiting > 0 {
				t.Reset(tickerNeedWaiting)
				break
			}

			pi := item.(*pendingItem)
			pi.Lock()

			for _, i := range pi.sendRequests {
				sr := i.(*sendRequest)
				sr.done(nil, errSendTimeout)
			}

			// flag the sending has completed with error, flush make no effect
			pi.done(errSendTimeout)
			pi.Unlock()

			// finally reached the last view item, current iteration ends
			if pi == lastViewItem {
				t.Reset(p.options.SendTimeout)
				break
			}
		}
	}
}

func (p *partitionProducer) internalFlushCurrentBatches() {
	batchesData, sequenceIDs, callbacks, errs := p.batchBuilder.FlushBatches()
	if batchesData == nil {
		return
	}

	for i := range batchesData {
		// error occurred in processing batch
		// report it using callback
		if errs[i] != nil {
			for _, cb := range callbacks[i] {
				if sr, ok := cb.(*sendRequest); ok {
					sr.done(nil, errs[i])
				}
			}

			if errors.Is(errs[i], internal.ErrExceedMaxMessageSize) {
				p.log.WithError(errMessageTooLarge).Errorf("internal err: %s", errs[i])
				return
			}

			continue
		}
		if batchesData[i] == nil {
			continue
		}
		p.pendingQueue.Put(&pendingItem{
			sentAt:       time.Now(),
			buffer:       batchesData[i],
			sequenceID:   sequenceIDs[i],
			sendRequests: callbacks[i],
		})
		p._getConn().WriteData(batchesData[i])
	}

}

func (p *partitionProducer) internalFlush(fr *flushRequest) {
	// clear all the messages which have sent to dataChan before flush
	if len(p.dataChan) != 0 {
		oldDataChan := p.dataChan
		p.dataChan = make(chan *sendRequest, p.options.MaxPendingMessages)
		for len(oldDataChan) != 0 {
			pendingData := <-oldDataChan
			p.internalSend(pendingData)
		}
	}

	if !p.options.DisableBatching {
		p.internalFlushCurrentBatch()
	}

	pi, ok := p.pendingQueue.PeekLast().(*pendingItem)
	if !ok {
		close(fr.doneCh)
		return
	}

	// lock the pending request while adding requests
	// since the ReceivedSendReceipt func iterates over this list
	pi.Lock()
	defer pi.Unlock()

	if pi.isDone {
		// The last item in the queue has been completed while we were
		// looking at it. It's safe at this point to assume that every
		// message enqueued before Flush() was called are now persisted
		close(fr.doneCh)
		return
	}

	pi.flushCallback = func(err error) {
		fr.err = err
		close(fr.doneCh)
	}
}

func (p *partitionProducer) Send(ctx context.Context, msg *ProducerMessage) (MessageID, error) {
	var err error
	var msgID MessageID

	// use atomic bool to avoid race
	isDone := uAtomic.NewBool(false)
	doneCh := make(chan struct{})

	p.internalSendAsync(ctx, msg, func(ID MessageID, message *ProducerMessage, e error) {
		if isDone.CAS(false, true) {
			err = e
			msgID = ID
			close(doneCh)
		}
	}, true)

	// wait for send request to finish
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-doneCh:
		// send request has been finished
		return msgID, err
	}
}

func (p *partitionProducer) SendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error)) {
	p.internalSendAsync(ctx, msg, callback, false)
}

func (p *partitionProducer) validateMsg(msg *ProducerMessage) error {
	if msg == nil {
		return newError(InvalidMessage, "Message is nil")
	}

	if msg.Value != nil && msg.Payload != nil {
		return newError(InvalidMessage, "Can not set Value and Payload both")
	}

	if p.options.DisableMultiSchema {
		if msg.Schema != nil && p.options.Schema != nil &&
			msg.Schema.GetSchemaInfo().hash() != p.options.Schema.GetSchemaInfo().hash() {
			p.log.Errorf("The producer %s of the topic %s is disabled the `MultiSchema`", p.producerName, p.topic)
			return fmt.Errorf("msg schema can not match with producer schema")
		}
	}

	return nil
}

func (p *partitionProducer) prepareTransaction(sr *sendRequest) error {
	if sr.msg.Transaction == nil {
		return nil
	}

	txn := (sr.msg.Transaction).(*transaction)
	if txn.state != TxnOpen {
		p.log.WithField("state", txn.state).Error("Failed to send message" +
			" by a non-open transaction.")
		return newError(InvalidStatus, "Failed to send message by a non-open transaction.")
	}

	if err := txn.registerProducerTopic(p.topic); err != nil {
		return err
	}

	if err := txn.registerSendOrAckOp(); err != nil {
		return err
	}

	sr.transaction = txn
	return nil
}

func (p *partitionProducer) updateSchema(sr *sendRequest) error {
	var schema Schema
	var schemaVersion []byte
	var err error

	if sr.msg.Schema != nil {
		schema = sr.msg.Schema
	} else if p.options.Schema != nil {
		schema = p.options.Schema
	}

	if schema == nil {
		return nil
	}

	schemaVersion = p.schemaCache.Get(schema.GetSchemaInfo())
	if schemaVersion == nil {
		schemaVersion, err = p.getOrCreateSchema(schema.GetSchemaInfo())
		if err != nil {
			return fmt.Errorf("get schema version fail, err: %w", err)
		}
		p.schemaCache.Put(schema.GetSchemaInfo(), schemaVersion)
	}

	sr.schema = schema
	sr.schemaVersion = schemaVersion
	return nil
}

func (p *partitionProducer) updateUncompressedPayload(sr *sendRequest) error {
	// read payload from message
	sr.uncompressedPayload = sr.msg.Payload

	if sr.msg.Value != nil {
		if sr.schema == nil {
			p.log.Errorf("Schema encode message failed %s", sr.msg.Value)
			return newError(SchemaFailure, "set schema value without setting schema")
		}

		// payload and schema are mutually exclusive
		// try to get payload from schema value only if payload is not set
		schemaPayload, err := sr.schema.Encode(sr.msg.Value)
		if err != nil {
			p.log.WithError(err).Errorf("Schema encode message failed %s", sr.msg.Value)
			return newError(SchemaFailure, err.Error())
		}

		sr.uncompressedPayload = schemaPayload
	}

	sr.uncompressedSize = int64(len(sr.uncompressedPayload))
	return nil
}

func (p *partitionProducer) updateMetaData(sr *sendRequest) {
	deliverAt := sr.msg.DeliverAt
	if sr.msg.DeliverAfter.Nanoseconds() > 0 {
		deliverAt = time.Now().Add(sr.msg.DeliverAfter)
	}

	// set default ReplicationClusters when DisableReplication
	if sr.msg.DisableReplication {
		sr.msg.ReplicationClusters = []string{"__local__"}
	}

	sr.mm = p.genMetadata(sr.msg, int(sr.uncompressedSize), deliverAt)

	sr.sendAsBatch = !p.options.DisableBatching &&
		sr.msg.ReplicationClusters == nil &&
		deliverAt.UnixNano() < 0

	if !sr.sendAsBatch {
		// update sequence id for metadata, make the size of msgMetadata more accurate
		// batch sending will update sequence ID in the BatchBuilder
		p.updateMetadataSeqID(sr.mm, sr.msg)
	}

	sr.deliverAt = deliverAt
}

func (p *partitionProducer) updateChunkInfo(sr *sendRequest) error {
	checkSize := sr.uncompressedSize
	if !sr.sendAsBatch {
		sr.compressedPayload = p.compressionProvider.Compress(nil, sr.uncompressedPayload)
		sr.compressedSize = len(sr.compressedPayload)

		// set the compress type in msgMetaData
		compressionType := pb.CompressionType(p.options.CompressionType)
		if compressionType != pb.CompressionType_NONE {
			sr.mm.Compression = &compressionType
		}

		checkSize = int64(sr.compressedSize)
	}

	sr.maxMessageSize = p._getConn().GetMaxMessageSize()

	// if msg is too large and chunking is disabled
	if checkSize > int64(sr.maxMessageSize) && !p.options.EnableChunking {
		p.log.WithError(errMessageTooLarge).
			WithField("size", checkSize).
			WithField("properties", sr.msg.Properties).
			Errorf("MaxMessageSize %d", sr.maxMessageSize)
		return errMessageTooLarge
	}

	if sr.sendAsBatch || !p.options.EnableChunking {
		sr.totalChunks = 1
		sr.payloadChunkSize = int(sr.maxMessageSize)
	} else {
		sr.payloadChunkSize = int(sr.maxMessageSize) - proto.Size(sr.mm)
		if sr.payloadChunkSize <= 0 {
			p.log.WithError(errMetaTooLarge).
				WithField("metadata size", proto.Size(sr.mm)).
				WithField("properties", sr.msg.Properties).
				Errorf("MaxMessageSize %d", int(p._getConn().GetMaxMessageSize()))
			return errMetaTooLarge
		}
		// set ChunkMaxMessageSize
		if p.options.ChunkMaxMessageSize != 0 {
			sr.payloadChunkSize = int(math.Min(float64(sr.payloadChunkSize), float64(p.options.ChunkMaxMessageSize)))
		}
		sr.totalChunks = int(math.Max(1, math.Ceil(float64(sr.compressedSize)/float64(sr.payloadChunkSize))))
	}

	return nil
}

func (p *partitionProducer) internalSendAsync(
	ctx context.Context,
	msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error),
	flushImmediately bool,
) {
	if err := p.validateMsg(msg); err != nil {
		p.log.Error(err)
		runCallback(callback, nil, msg, err)
		return
	}

	sr := &sendRequest{
		ctx:              ctx,
		msg:              msg,
		producer:         p,
		callback:         callback,
		callbackOnce:     &sync.Once{},
		flushImmediately: flushImmediately,
		publishTime:      time.Now(),
		chunkID:          -1,
	}

	if err := p.prepareTransaction(sr); err != nil {
		sr.done(nil, err)
		return
	}

	if p.getProducerState() != producerReady {
		sr.done(nil, errProducerClosed)
		return
	}

	p.options.Interceptors.BeforeSend(p, msg)

	if err := p.updateSchema(sr); err != nil {
		p.log.Error(err)
		sr.done(nil, err)
		return
	}

	if err := p.updateUncompressedPayload(sr); err != nil {
		p.log.Error(err)
		sr.done(nil, err)
		return
	}

	p.updateMetaData(sr)

	if err := p.updateChunkInfo(sr); err != nil {
		p.log.Error(err)
		sr.done(nil, err)
		return
	}

	if err := p.reserveResources(sr); err != nil {
		p.log.Error(err)
		sr.done(nil, err)
		return
	}

	p.dataChan <- sr
}

func (p *partitionProducer) ReceivedSendReceipt(response *pb.CommandSendReceipt) {
	pi, ok := p.pendingQueue.Peek().(*pendingItem)

	if !ok {
		// if we receive a receipt although the pending queue is empty, the state of the broker and the producer differs.
		p.log.Warnf("Got ack %v for timed out msg", response.GetMessageId())
		return
	}

	if pi.sequenceID < response.GetSequenceId() {
		// Force connection closing so that messages can be re-transmitted in a new connection
		p.log.Warnf("Received ack for %v on sequenceId %v - expected: %v, local < remote, closing connection",
			response.GetMessageId(), response.GetSequenceId(), pi.sequenceID)
		p._getConn().Close()
		return
	} else if pi.sequenceID > response.GetSequenceId() {
		// Ignoring the ack since it's referring to a message that has already timed out.
		p.log.Warnf("Received ack for %v on sequenceId %v - expected: %v, local > remote, ignore it",
			response.GetMessageId(), response.GetSequenceId(), pi.sequenceID)
		return
	} else {
		// The ack was indeed for the expected item in the queue, we can remove it and trigger the callback
		p.pendingQueue.Poll()

		now := time.Now().UnixNano()

		// lock the pending item while sending the requests
		pi.Lock()
		defer pi.Unlock()
		p.metrics.PublishRPCLatency.Observe(float64(now-pi.sentAt.UnixNano()) / 1.0e9)
		batchSize := int32(len(pi.sendRequests))
		for idx, i := range pi.sendRequests {
			sr := i.(*sendRequest)
			atomic.StoreInt64(&p.lastSequenceID, int64(pi.sequenceID))

			msgID := newMessageID(
				int64(response.MessageId.GetLedgerId()),
				int64(response.MessageId.GetEntryId()),
				int32(idx),
				p.partitionIdx,
				batchSize,
			)

			if sr.totalChunks > 1 {
				if sr.chunkID == 0 {
					sr.chunkRecorder.setFirstChunkID(
						&messageID{
							int64(response.MessageId.GetLedgerId()),
							int64(response.MessageId.GetEntryId()),
							-1,
							p.partitionIdx,
							0,
						})
				} else if sr.chunkID == sr.totalChunks-1 {
					sr.chunkRecorder.setLastChunkID(
						&messageID{
							int64(response.MessageId.GetLedgerId()),
							int64(response.MessageId.GetEntryId()),
							-1,
							p.partitionIdx,
							0,
						})
					// use chunkMsgID to set msgID
					msgID = &sr.chunkRecorder.chunkedMsgID
				}
			}

			sr.done(msgID, nil)
		}

		// Mark this pending item as done
		pi.done(nil)
	}
}

func (p *partitionProducer) internalClose(req *closeProducer) {
	defer close(req.doneCh)
	if !p.casProducerState(producerReady, producerClosing) {
		return
	}

	defer close(p.dataChan)
	defer close(p.cmdChan)
	p.log.Info("Closing producer")

	id := p.client.rpcClient.NewRequestID()
	_, err := p.client.rpcClient.RequestOnCnx(p._getConn(), id, pb.BaseCommand_CLOSE_PRODUCER, &pb.CommandCloseProducer{
		ProducerId: &p.producerID,
		RequestId:  &id,
	})

	if err != nil {
		p.log.WithError(err).Warn("Failed to close producer")
	} else {
		p.log.Info("Closed producer")
	}
	p.failPendingMessages()

	if p.batchBuilder != nil {
		if err = p.batchBuilder.Close(); err != nil {
			p.log.WithError(err).Warn("Failed to close batch builder")
		}
	}

	p.setProducerState(producerClosed)
	p._getConn().UnregisterListener(p.producerID)
	p.batchFlushTicker.Stop()
}

func (p *partitionProducer) failPendingMessages() {
	curViewItems := p.pendingQueue.ReadableSlice()
	viewSize := len(curViewItems)
	if viewSize <= 0 {
		return
	}
	p.log.Infof("Failing %d messages on closing producer", viewSize)
	lastViewItem := curViewItems[viewSize-1].(*pendingItem)

	// iterate at most viewSize items
	for i := 0; i < viewSize; i++ {
		item := p.pendingQueue.CompareAndPoll(
			func(m interface{}) bool {
				return m != nil
			})

		if item == nil {
			return
		}

		pi := item.(*pendingItem)
		pi.Lock()

		for _, i := range pi.sendRequests {
			sr := i.(*sendRequest)
			sr.done(nil, errProducerClosed)
		}

		// flag the sending has completed with error, flush make no effect
		pi.done(errProducerClosed)
		pi.Unlock()

		// finally reached the last view item, current iteration ends
		if pi == lastViewItem {
			p.log.Infof("%d messages complete failed", viewSize)
			return
		}
	}
}

func (p *partitionProducer) LastSequenceID() int64 {
	return atomic.LoadInt64(&p.lastSequenceID)
}

func (p *partitionProducer) Flush() error {
	flushReq := &flushRequest{
		doneCh: make(chan struct{}),
		err:    nil,
	}
	p.cmdChan <- flushReq

	// wait for the flush request to complete
	<-flushReq.doneCh
	return flushReq.err
}

func (p *partitionProducer) getProducerState() producerState {
	return producerState(p.state.Load())
}

func (p *partitionProducer) setProducerState(state producerState) {
	p.state.Swap(int32(state))
}

// set a new producerState and return the last state
// returns bool if the new state has been set or not
func (p *partitionProducer) casProducerState(oldState, newState producerState) bool {
	return p.state.CAS(int32(oldState), int32(newState))
}

func (p *partitionProducer) Close() {
	if p.getProducerState() != producerReady {
		// Producer is closing
		return
	}

	cp := &closeProducer{doneCh: make(chan struct{})}
	p.cmdChan <- cp

	// wait for close producer request to complete
	<-cp.doneCh
}

type sendRequest struct {
	ctx              context.Context
	msg              *ProducerMessage
	producer         *partitionProducer
	callback         func(MessageID, *ProducerMessage, error)
	callbackOnce     *sync.Once
	publishTime      time.Time
	flushImmediately bool
	totalChunks      int
	chunkID          int
	uuid             string
	chunkRecorder    *chunkRecorder

	/// resource management

	memLimit          internal.MemoryLimitController
	reservedMem       int64
	semaphore         internal.Semaphore
	reservedSemaphore int

	/// convey settable state

	sendAsBatch         bool
	transaction         *transaction
	schema              Schema
	schemaVersion       []byte
	uncompressedPayload []byte
	uncompressedSize    int64
	compressedPayload   []byte
	compressedSize      int
	payloadChunkSize    int
	mm                  *pb.MessageMetadata
	deliverAt           time.Time
	maxMessageSize      int32
}

func (sr *sendRequest) done(msgID MessageID, err error) {
	if err == nil {
		sr.producer.metrics.PublishLatency.Observe(float64(time.Now().UnixNano()-sr.publishTime.UnixNano()) / 1.0e9)
		sr.producer.metrics.MessagesPublished.Inc()
		sr.producer.metrics.BytesPublished.Add(float64(sr.reservedMem))

		if sr.totalChunks <= 1 || sr.chunkID == sr.totalChunks-1 {
			if sr.producer.options.Interceptors != nil {
				sr.producer.options.Interceptors.OnSendAcknowledgement(sr.producer, sr.msg, msgID)
			}
		}
	}

	if err != nil {
		sr.producer.log.WithError(err).
			WithField("size", sr.reservedMem).
			WithField("properties", sr.msg.Properties)
	}

	if errors.Is(err, errSendTimeout) {
		sr.producer.metrics.PublishErrorsTimeout.Inc()
	}

	if errors.Is(err, errMessageTooLarge) {
		sr.producer.metrics.PublishErrorsMsgTooLarge.Inc()
	}

	if sr.semaphore != nil {
		sr.semaphore.Release()
		sr.producer.metrics.MessagesPending.Dec()
	}

	if sr.memLimit != nil {
		sr.memLimit.ReleaseMemory(sr.reservedMem)
		sr.producer.metrics.BytesPending.Sub(float64(sr.reservedMem))
	}

	// sr.chunkID == -1 means a chunked message is not yet prepared, so that we should fail it immediately
	if sr.totalChunks <= 1 || sr.chunkID == -1 || sr.chunkID == sr.totalChunks-1 {
		sr.callbackOnce.Do(func() {
			runCallback(sr.callback, msgID, sr.msg, err)
		})

		if sr.transaction != nil {
			sr.transaction.endSendOrAckOp(err)
		}
	}
}

func (p *partitionProducer) blockIfQueueFull() bool {
	//DisableBlockIfQueueFull == false means enable block
	return !p.options.DisableBlockIfQueueFull
}

func (p *partitionProducer) reserveSemaphore(sr *sendRequest) error {
	for i := 0; i < sr.totalChunks; i++ {
		if p.blockIfQueueFull() {
			if !p.publishSemaphore.Acquire(sr.ctx) {
				return errContextExpired
			}

			// update sr.semaphore and sr.reservedSemaphore here so that we can release semaphore in the case
			// of that only a part of the chunks acquire succeed
			sr.semaphore = p.publishSemaphore
			sr.reservedSemaphore++
			p.metrics.MessagesPending.Inc()
		} else {
			if !p.publishSemaphore.TryAcquire() {
				return errSendQueueIsFull
			}

			// update sr.semaphore and sr.reservedSemaphore here so that we can release semaphore in the case
			// of that only a part of the chunks acquire succeed
			sr.semaphore = p.publishSemaphore
			sr.reservedSemaphore++
			p.metrics.MessagesPending.Inc()
		}
	}

	return nil
}

func (p *partitionProducer) reserveMem(sr *sendRequest) error {
	requiredMem := sr.uncompressedSize
	if !sr.sendAsBatch {
		requiredMem = int64(sr.compressedSize)
	}

	if p.blockIfQueueFull() {
		if !p.client.memLimit.ReserveMemory(sr.ctx, requiredMem) {
			return errContextExpired
		}
	} else {
		if !p.client.memLimit.TryReserveMemory(requiredMem) {
			return errMemoryBufferIsFull
		}
	}

	sr.memLimit = p.client.memLimit
	sr.reservedMem += requiredMem
	p.metrics.BytesPending.Add(float64(requiredMem))
	return nil
}

func (p *partitionProducer) reserveResources(sr *sendRequest) error {
	if err := p.reserveSemaphore(sr); err != nil {
		return err
	}
	if err := p.reserveMem(sr); err != nil {
		return err
	}
	return nil
}

type closeProducer struct {
	doneCh chan struct{}
}

type flushRequest struct {
	doneCh chan struct{}
	err    error
}

func (i *pendingItem) done(err error) {
	if i.isDone {
		return
	}
	i.isDone = true
	buffersPool.Put(i.buffer)
	if i.flushCallback != nil {
		i.flushCallback(err)
	}
}

// _setConn sets the internal connection field of this partition producer atomically.
// Note: should only be called by this partition producer when a new connection is available.
func (p *partitionProducer) _setConn(conn internal.Connection) {
	p.conn.Store(conn)
}

// _getConn returns internal connection field of this partition producer atomically.
// Note: should only be called by this partition producer before attempting to use the connection
func (p *partitionProducer) _getConn() internal.Connection {
	// Invariant: p.conn must be non-nil for the lifetime of the partitionProducer.
	//            For this reason we leave this cast unchecked and panic() if the
	//            invariant is broken
	return p.conn.Load().(internal.Connection)
}

type chunkRecorder struct {
	chunkedMsgID chunkMessageID
}

func newChunkRecorder() *chunkRecorder {
	return &chunkRecorder{
		chunkedMsgID: chunkMessageID{},
	}
}

func (c *chunkRecorder) setFirstChunkID(msgID *messageID) {
	c.chunkedMsgID.firstChunkID = msgID
}

func (c *chunkRecorder) setLastChunkID(msgID *messageID) {
	c.chunkedMsgID.messageID = msgID
}

func toProtoProducerAccessMode(accessMode ProducerAccessMode) pb.ProducerAccessMode {
	switch accessMode {
	case ProducerAccessModeShared:
		return pb.ProducerAccessMode_Shared
	case ProducerAccessModeExclusive:
		return pb.ProducerAccessMode_Exclusive
	case ProducerAccessModeWaitForExclusive:
		return pb.ProducerAccessMode_WaitForExclusive
	}

	return pb.ProducerAccessMode_Shared
}
