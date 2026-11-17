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
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	internalcrypto "github.com/apache/pulsar-client-go/pulsar/internal/crypto"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
)

// receiptRaceQueue lets another owner claim the item after the receipt has
// observed it, without introducing hooks into production code.
type receiptRaceQueue struct {
	internal.BlockingQueue
	afterPeek func()
}

func (q *receiptRaceQueue) Peek() interface{} {
	item := q.BlockingQueue.Peek()
	if hook := q.afterPeek; hook != nil {
		q.afterPeek = nil
		hook()
	}
	return item
}

func TestReceivedSendReceiptOwnership(t *testing.T) {
	for _, owner := range []string{"receipt", "timeout", "close", "another receipt"} {
		t.Run(owner, func(t *testing.T) {
			q := &receiptRaceQueue{BlockingQueue: internal.NewBlockingQueue(2)}
			p := &partitionProducer{
				pendingQueue: q,
				options:      &ProducerOptions{SendTimeout: time.Millisecond},
				log:          plog.DefaultNopLogger(),
				metrics:      internal.NewMetricsProvider(4, nil, prometheus.NewRegistry()).GetLeveledMetrics("topic"),
			}
			p.setProducerState(producerReady)
			memory := internal.NewMemoryLimitController(1024, 0)
			semaphore := internal.NewSemaphore(2)
			calls := [2]int{}
			results := [2]error{}
			items := [2]*pendingItem{}
			requests := [2]*sendRequest{}
			for idx := range items {
				ctx, cancel := context.WithCancel(context.Background())
				require.True(t, semaphore.TryAcquire())
				memory.ForceReserveMemory(10)
				p.metrics.MessagesPending.Inc()
				p.metrics.BytesPending.Add(10)
				requests[idx] = &sendRequest{
					producer: p, pool: &sync.Pool{}, msg: &ProducerMessage{},
					callbackOnce: &sync.Once{}, publishTime: time.Now(),
					semaphore: semaphore, memLimit: memory, reservedMem: 10,
					callback: func(_ MessageID, _ *ProducerMessage, err error) {
						calls[idx]++
						results[idx] = err
						if owner == "timeout" && idx == 0 {
							p.setProducerState(producerClosing)
						}
					},
				}
				items[idx] = &pendingItem{
					ctx: ctx, cancel: cancel, buffer: internal.NewBuffer(1),
					sequenceID: uint64(idx + 1), createdAt: time.Now().Add(time.Hour),
					sentAt: time.Now(), sendRequests: []interface{}{requests[idx]},
				}
			}
			for _, item := range items {
				item.buffer.Retain()
			}
			items[0].createdAt = time.Now().Add(-time.Hour)
			q.Put(items[0])
			receipt := func(sequence uint64) *pb.CommandSendReceipt {
				return &pb.CommandSendReceipt{
					SequenceId: proto.Uint64(sequence),
					MessageId:  &pb.MessageIdData{LedgerId: proto.Uint64(1), EntryId: proto.Uint64(sequence)},
				}
			}
			// Keep both messages in flight when racing timeout or another receipt.
			// Close drains its snapshot, so its next item is enqueued after the drain.
			if owner != "close" {
				q.Put(items[1])
			}
			if owner != "receipt" {
				q.afterPeek = func() {
					switch owner {
					case "timeout":
						// Wait for the actual timeout worker to claim and complete the head.
						stopped := make(chan struct{})
						go func() {
							defer close(stopped)
							p.failTimeoutMessages()
						}()
						select {
						case <-stopped:
						case <-time.After(5 * time.Second):
							p.setProducerState(producerClosing)
							t.Fatal("timeout worker did not finish")
						}
					case "close":
						p.failPendingMessages(ErrProducerClosed)
					case "another receipt":
						p.ReceivedSendReceipt(receipt(1))
					}
					if owner == "close" {
						q.Put(items[1])
					}
				}
			}
			require.NotPanics(t, func() { p.ReceivedSendReceipt(receipt(1)) })
			require.Equal(t, [2]int{1, 0}, calls)
			switch owner {
			case "timeout":
				require.ErrorIs(t, results[0], ErrSendTimeout)
			case "close":
				require.ErrorIs(t, results[0], ErrProducerClosed)
			default:
				require.NoError(t, results[0])
			}
			require.Nil(t, requests[0].producer, "completed request should be reset for pooling")
			require.Equal(t, 1, q.Size())
			require.Same(t, items[1], q.Peek())
			require.EqualValues(t, 10, memory.CurrentUsage())
			require.True(t, items[0].isDone)
			require.ErrorIs(t, items[0].ctx.Err(), context.Canceled)
			require.Zero(t, items[0].buffer.RefCnt())

			// The next receipt must still complete and release all remaining resources.
			p.ReceivedSendReceipt(receipt(2))
			p.ReceivedSendReceipt(receipt(2)) // A duplicate receipt is harmless.
			require.Equal(t, [2]int{1, 1}, calls)
			require.NoError(t, results[1])
			require.Zero(t, q.Size())
			require.Zero(t, memory.CurrentUsage())
			require.ErrorIs(t, items[1].ctx.Err(), context.Canceled)
			require.Zero(t, items[1].buffer.RefCnt())
			require.True(t, semaphore.TryAcquire())
			require.True(t, semaphore.TryAcquire())
			require.False(t, semaphore.TryAcquire())
		})
	}
}

func TestReceivedSendReceiptAlreadyDone(t *testing.T) {
	q := internal.NewBlockingQueue(1)
	ctx, cancel := context.WithCancel(context.Background())
	pi := &pendingItem{
		ctx: ctx, cancel: cancel, buffer: internal.NewBuffer(1), sequenceID: 1,
		// A completed item may still refer to requests already reset for pooling.
		sendRequests: []interface{}{&sendRequest{}},
	}
	pi.buffer.Retain()
	pi.done(ErrSendTimeout)
	q.Put(pi)
	p := &partitionProducer{pendingQueue: q}
	require.NotPanics(t, func() {
		p.ReceivedSendReceipt(&pb.CommandSendReceipt{SequenceId: proto.Uint64(1)})
	})
	require.Zero(t, q.Size())
	require.Zero(t, pi.buffer.RefCnt())
}

// newBatchingTestProducer returns a ready, batching partitionProducer whose
// connection only records the written frames.
func newBatchingTestProducer(t *testing.T, provider internal.BatcherBuilderProvider) *partitionProducer {
	t.Helper()
	metrics := internal.NewMetricsProvider(4, nil, prometheus.NewRegistry())
	builder, err := provider(1000, 1<<20, 1<<20, "test", 1, pb.CompressionType_NONE, 0,
		internal.NewBufferPool(), metrics, plog.DefaultNopLogger(),
		internalcrypto.NewNoopEncryptor())
	require.NoError(t, err)
	generator := uint64(0)
	p := &partitionProducer{
		ctx:                  context.Background(),
		options:              &ProducerOptions{},
		log:                  plog.DefaultNopLogger(),
		metrics:              metrics.GetLeveledMetrics("topic"),
		pendingQueue:         internal.NewBlockingQueue(100),
		batchBuilder:         builder,
		sequenceIDGenerator:  &generator,
		lastSequenceID:       -1,
		lastSequenceIDPushed: -1,
	}
	p._setConn(&mockConn{dummyConnection: &dummyConnection{}})
	p.setProducerState(producerReady)
	return p
}

// sendBatched pushes a message through the event loop path. A negative
// sequenceID lets the producer generate one.
func sendBatched(p *partitionProducer, sequenceID int64, key string, acked map[string]error) {
	msg := &ProducerMessage{Payload: []byte(fmt.Sprintf("seq-%d-%d", sequenceID, len(acked))), Key: key}
	if sequenceID >= 0 {
		msg.SequenceID = &sequenceID
	}
	p.internalSend(&sendRequest{
		producer:            p,
		msg:                 msg,
		callbackOnce:        &sync.Once{},
		publishTime:         time.Now(),
		sendAsBatch:         true,
		uncompressedPayload: msg.Payload,
		uncompressedSize:    int64(len(msg.Payload)),
		callback: func(_ MessageID, m *ProducerMessage, err error) {
			acked[string(m.Payload)] = err
		},
	})
	acked[string(msg.Payload)] = errors.New("not acknowledged")
}

// sentBatches flushes the producer and returns the sequence IDs carried by each
// pending item, plus the item itself.
func sentBatches(t *testing.T, p *partitionProducer) ([][]int64, []*pendingItem) {
	t.Helper()
	p.internalFlushCurrentBatch()
	pending := p.pendingQueue.ReadableSlice()
	batches := make([][]int64, 0, len(pending))
	items := make([]*pendingItem, 0, len(pending))
	for _, item := range pending {
		pi := item.(*pendingItem)
		var ids []int64
		for _, r := range pi.sendRequests {
			ids = append(ids, *r.(*sendRequest).msg.SequenceID)
		}
		batches = append(batches, ids)
		items = append(items, pi)
	}
	return batches, items
}

func ackAll(p *partitionProducer, items []*pendingItem) {
	for _, pi := range items {
		p.ReceivedSendReceipt(&pb.CommandSendReceipt{
			SequenceId:        proto.Uint64(pi.sequenceID),
			HighestSequenceId: proto.Uint64(pi.highestSequenceID),
			MessageId:         &pb.MessageIdData{LedgerId: proto.Uint64(1), EntryId: proto.Uint64(pi.sequenceID)},
		})
	}
}

// See https://github.com/apache/pulsar-client-go/issues/1395
func TestBatchNeverMixesPotentialDuplicatesWithNewMessages(t *testing.T) {
	for name, provider := range map[string]internal.BatcherBuilderProvider{
		"default":   internal.NewBatchBuilder,
		"key-based": internal.NewKeyBasedBatchBuilder,
	} {
		t.Run(name, func(t *testing.T) {
			p := newBatchingTestProducer(t, provider)
			acked := map[string]error{}

			sendBatched(p, 0, "k", acked)
			sendBatched(p, 1, "k", acked)
			batches, items := sentBatches(t, p)
			require.Equal(t, [][]int64{{0, 1}}, batches)
			require.Equal(t, uint64(0), items[0].sequenceID)
			require.Equal(t, uint64(1), items[0].highestSequenceID)
			ackAll(p, items)
			require.Equal(t, int64(1), p.LastSequenceID())

			// The application resends 0 and 1 and then publishes 2 and 3 before the
			// batch window expires. Each resend gets its own batch and the new
			// messages are batched together again.
			for _, id := range []int64{0, 1, 2, 3} {
				sendBatched(p, id, "k", acked)
			}
			batches, items = sentBatches(t, p)
			require.Equal(t, [][]int64{{0}, {1}, {2, 3}}, batches)
			require.Equal(t, uint64(2), items[2].sequenceID)
			require.Equal(t, uint64(3), items[2].highestSequenceID)
			ackAll(p, items)
			require.Equal(t, int64(3), p.LastSequenceID())
			require.Len(t, acked, 6)
			for payload, err := range acked {
				require.NoError(t, err, payload)
			}
		})
	}
}

func TestBatchIsolatesResendOfAMessageInsideAnEarlierBatch(t *testing.T) {
	p := newBatchingTestProducer(t, internal.NewBatchBuilder)
	acked := map[string]error{}

	for id := int64(0); id < 5; id++ {
		sendBatched(p, id, "", acked)
	}
	// 3 was in the middle of the batch that is still in flight, so it is only a
	// potential duplicate until that batch is acknowledged.
	sendBatched(p, 3, "", acked)
	sendBatched(p, 5, "", acked)
	sendBatched(p, 6, "", acked)
	batches, items := sentBatches(t, p)
	require.Equal(t, [][]int64{{0, 1, 2, 3, 4}, {3}, {5, 6}}, batches)
	require.Equal(t, uint64(4), items[0].highestSequenceID)
	require.Equal(t, uint64(3), items[1].highestSequenceID)
	require.Equal(t, uint64(6), items[2].highestSequenceID)
}

func TestBatchWithGeneratedSequenceIDsIsNotSplit(t *testing.T) {
	p := newBatchingTestProducer(t, internal.NewBatchBuilder)
	acked := map[string]error{}

	for i := 0; i < 10; i++ {
		sendBatched(p, -1, "", acked)
	}
	p.internalFlushCurrentBatch()
	require.Equal(t, 1, p.pendingQueue.Size())
	pi := p.pendingQueue.Peek().(*pendingItem)
	require.Len(t, pi.sendRequests, 10)
	require.Equal(t, uint64(0), pi.sequenceID)
	require.Equal(t, uint64(9), pi.highestSequenceID)
	require.Equal(t, int64(9), p.lastSequenceIDPushed)
	ackAll(p, []*pendingItem{pi})
	require.Equal(t, int64(9), p.LastSequenceID())
	require.Len(t, acked, 10)
}
