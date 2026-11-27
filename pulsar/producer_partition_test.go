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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal"
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

// A send blocked on a full semaphore or memory limit must report the context
// error regardless of which resource blocked it, while staying a *Error whose
// Result() is TimeoutError, as ErrContextExpired was before.
func TestReserveResourcesContextErrorPreservesResult(t *testing.T) {
	for _, blocked := range []string{"semaphore", "memory"} {
		for _, tc := range []struct {
			name    string
			ctx     func() (context.Context, context.CancelFunc)
			wantErr error
		}{
			{
				name: "canceled",
				ctx: func() (context.Context, context.CancelFunc) {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx, cancel
				},
				wantErr: context.Canceled,
			},
			{
				name: "deadline",
				ctx: func() (context.Context, context.CancelFunc) {
					return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				},
				wantErr: context.DeadlineExceeded,
			},
		} {
			t.Run(blocked+"/"+tc.name, func(t *testing.T) {
				semaphore := internal.NewSemaphore(1)
				memory := internal.NewMemoryLimitController(1024, 0)
				if blocked == "semaphore" {
					require.True(t, semaphore.TryAcquire())
				} else {
					require.True(t, memory.TryReserveMemory(1025)) // usage must exceed the limit to block
				}

				p := &partitionProducer{
					publishSemaphore: semaphore,
					client:           &client{memLimit: memory},
					options:          &ProducerOptions{},
					log:              plog.DefaultNopLogger(),
					metrics:          internal.NewMetricsProvider(4, nil, prometheus.NewRegistry()).GetLeveledMetrics("topic"),
				}
				ctx, cancel := tc.ctx()
				defer cancel()
				sr := &sendRequest{
					ctx:              ctx,
					totalChunks:      1,
					uncompressedSize: 1,
					compressedSize:   1,
					sendAsBatch:      true,
				}

				err := p.reserveResources(sr)
				require.Error(t, err)
				pulsarErr, ok := err.(*Error) //nolint:errorlint // direct assertion must keep working
				require.True(t, ok, "expected *Error, got %T", err)
				require.Equal(t, TimeoutError, pulsarErr.Result())
				require.ErrorIs(t, err, ErrContextExpired)
				require.ErrorIs(t, err, tc.wantErr)
				require.Contains(t, err.Error(), ErrContextExpired.Error())
				require.Contains(t, err.Error(), tc.wantErr.Error())
			})
		}
	}
}

func TestRaiseLastSequenceID(t *testing.T) {
	p := &partitionProducer{lastSequenceID: -1}

	// A new or reconnected producer adopts the last sequence id the broker reports.
	p.raiseLastSequenceID(9)
	require.Equal(t, int64(9), p.LastSequenceID())

	// The value never moves backwards, including when the broker reports -1.
	p.raiseLastSequenceID(5)
	p.raiseLastSequenceID(-1)
	require.Equal(t, int64(9), p.LastSequenceID())

	p.raiseLastSequenceID(12)
	require.Equal(t, int64(12), p.LastSequenceID())
}

// TestReceivedSendReceiptKeepsRecoveredLastSequenceID covers a reconnect where the broker already
// persisted sequence ids 8 and 9 before their receipts arrived. The handshake seeds LastSequenceID to 9
// and the producer replays both sends; the duplicate receipt for 8 must complete normally without
// lowering LastSequenceID, including when it is read from the send callback.
func TestReceivedSendReceiptKeepsRecoveredLastSequenceID(t *testing.T) {
	p, q := newReceiptTestProducer()
	p.raiseLastSequenceID(9)

	seenInCallback, callbackErrs := enqueuePendingSends(p, 8, 9)
	receipt := func(seq uint64) *pb.CommandSendReceipt {
		return &pb.CommandSendReceipt{
			SequenceId: proto.Uint64(seq),
			// The broker answers a duplicate send with ledger and entry id -1.
			MessageId: &pb.MessageIdData{LedgerId: proto.Uint64(^uint64(0)), EntryId: proto.Uint64(^uint64(0))},
		}
	}

	p.ReceivedSendReceipt(receipt(8))
	require.NoError(t, callbackErrs[8])
	require.Equal(t, int64(9), seenInCallback[8], "duplicate receipt must not lower the recovered id")
	require.Equal(t, int64(9), p.LastSequenceID())

	p.ReceivedSendReceipt(receipt(9))
	require.NoError(t, callbackErrs[9])
	require.Equal(t, int64(9), seenInCallback[9])
	require.Equal(t, int64(9), p.LastSequenceID())
	require.Zero(t, q.Size())
}

// TestReceivedSendReceiptTracksLatestPublishedSequenceID covers ordinary (non-duplicate) receipts:
// LastSequenceID reports the most recently published and acknowledged id, even when explicit
// sequence ids go backwards, e.g. 9 and then 8 with deduplication disabled.
func TestReceivedSendReceiptTracksLatestPublishedSequenceID(t *testing.T) {
	p, q := newReceiptTestProducer()
	seenInCallback, callbackErrs := enqueuePendingSends(p, 9, 8)
	receipt := func(seq, entryID uint64) *pb.CommandSendReceipt {
		return &pb.CommandSendReceipt{
			SequenceId: proto.Uint64(seq),
			MessageId:  &pb.MessageIdData{LedgerId: proto.Uint64(3), EntryId: proto.Uint64(entryID)},
		}
	}

	p.ReceivedSendReceipt(receipt(9, 0))
	require.NoError(t, callbackErrs[9])
	require.Equal(t, int64(9), seenInCallback[9])
	require.Equal(t, int64(9), p.LastSequenceID())

	p.ReceivedSendReceipt(receipt(8, 1))
	require.NoError(t, callbackErrs[8])
	require.Equal(t, int64(8), seenInCallback[8])
	require.Equal(t, int64(8), p.LastSequenceID())
	require.Zero(t, q.Size())
}

func newReceiptTestProducer() (*partitionProducer, internal.BlockingQueue) {
	q := internal.NewBlockingQueue(2)
	p := &partitionProducer{
		pendingQueue:   q,
		options:        &ProducerOptions{},
		log:            plog.DefaultNopLogger(),
		metrics:        internal.NewMetricsProvider(4, nil, prometheus.NewRegistry()).GetLeveledMetrics("topic"),
		lastSequenceID: -1,
	}
	p.setProducerState(producerReady)
	return p, q
}

// enqueuePendingSends queues one single-message pending item per sequence id and records, per id,
// the error and the LastSequenceID observed in its send callback.
func enqueuePendingSends(p *partitionProducer, seqs ...uint64) (map[uint64]int64, map[uint64]error) {
	seenInCallback := map[uint64]int64{}
	callbackErrs := map[uint64]error{}
	for _, seq := range seqs {
		seq := seq
		ctx, cancel := context.WithCancel(context.Background())
		sr := &sendRequest{
			producer: p, pool: &sync.Pool{}, msg: &ProducerMessage{},
			callbackOnce: &sync.Once{}, publishTime: time.Now(),
			callback: func(_ MessageID, _ *ProducerMessage, err error) {
				seenInCallback[seq] = p.LastSequenceID()
				callbackErrs[seq] = err
			},
		}
		pi := &pendingItem{
			ctx: ctx, cancel: cancel, buffer: internal.NewBuffer(1),
			sequenceID: seq, createdAt: time.Now(), sentAt: time.Now(),
			sendRequests: []interface{}{sr},
		}
		pi.buffer.Retain()
		p.pendingQueue.Put(pi)
	}
	return seenInCallback, callbackErrs
}
