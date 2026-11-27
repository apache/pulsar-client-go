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
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	"github.com/apache/pulsar-client-go/pulsar/internal/crypto"
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

func TestReceivedSendReceiptUpdatesLastSequenceIDToHighest(t *testing.T) {
	for _, tc := range []struct {
		name            string
		highest         *uint64
		expectedLastSeq int64
	}{
		// A batch of sequence ids 5..7 is acknowledged with highest_sequence_id 7.
		{name: "highest echoed by broker", highest: proto.Uint64(7), expectedLastSeq: 7},
		// Without highest_sequence_id the first sequence id of the item is used.
		{name: "highest not echoed", highest: nil, expectedLastSeq: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := internal.NewBlockingQueue(1)
			p := &partitionProducer{
				pendingQueue:   q,
				lastSequenceID: -1,
				options:        &ProducerOptions{SendTimeout: time.Minute},
				log:            plog.DefaultNopLogger(),
				metrics:        internal.NewMetricsProvider(4, nil, prometheus.NewRegistry()).GetLeveledMetrics("topic"),
			}
			p.setProducerState(producerReady)
			memory := internal.NewMemoryLimitController(1024, 0)
			semaphore := internal.NewSemaphore(2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			require.True(t, semaphore.TryAcquire())
			memory.ForceReserveMemory(10)
			p.metrics.MessagesPending.Inc()
			p.metrics.BytesPending.Add(10)
			var result error
			req := &sendRequest{
				producer: p, pool: &sync.Pool{}, msg: &ProducerMessage{},
				callbackOnce: &sync.Once{}, publishTime: time.Now(),
				semaphore: semaphore, memLimit: memory, reservedMem: 10,
				callback: func(_ MessageID, _ *ProducerMessage, err error) { result = err },
			}
			pi := &pendingItem{
				ctx: ctx, cancel: cancel, buffer: internal.NewBuffer(1), sequenceID: 5,
				sentAt: time.Now(), sendRequests: []interface{}{req},
			}
			pi.buffer.Retain()
			q.Put(pi)

			p.ReceivedSendReceipt(&pb.CommandSendReceipt{
				SequenceId:        proto.Uint64(5),
				HighestSequenceId: tc.highest,
				MessageId:         &pb.MessageIdData{LedgerId: proto.Uint64(1), EntryId: proto.Uint64(7)},
			})

			require.NoError(t, result)
			require.Equal(t, tc.expectedLastSeq, p.LastSequenceID())
		})
	}
}

// frameRecordingConnection records the SEND commands written to the connection.
type frameRecordingConnection struct {
	dummyConnection
	sends []*pb.CommandSend
}

func (c *frameRecordingConnection) WriteData(_ context.Context, data internal.Buffer) {
	frame := data.ReadableSlice()
	cmdSize := binary.BigEndian.Uint32(frame[4:8])
	cmd := &pb.BaseCommand{}
	if err := proto.Unmarshal(frame[8:8+cmdSize], cmd); err != nil {
		panic(err)
	}
	c.sends = append(c.sends, cmd.GetSend())
}

func newSequenceTestProducer(t *testing.T, cnx internal.Connection) *partitionProducer {
	metrics := internal.NewMetricsProvider(4, nil, prometheus.NewRegistry())
	p := &partitionProducer{
		client:              &client{metrics: metrics},
		ctx:                 context.Background(),
		pendingQueue:        internal.NewBlockingQueue(10),
		lastSequenceID:      -1,
		options:             &ProducerOptions{SendTimeout: time.Minute},
		log:                 plog.DefaultNopLogger(),
		metrics:             metrics.GetLeveledMetrics("topic"),
		compressionProvider: compression.NewNoopProvider(),
		encryptor:           crypto.NewNoopEncryptor(),
		sequenceIDGenerator: new(uint64),
	}
	p._setConn(cnx)
	p.setProducerState(producerReady)
	var err error
	p.batchBuilder, err = internal.NewBatchBuilder(10, 128*1024, uint32(cnx.GetMaxMessageSize()), "producer", 1,
		pb.CompressionType_NONE, compression.Level(0), buffersPool, metrics, p.log, p.encryptor)
	require.NoError(t, err)
	return p
}

// prepareSend runs the part of SendAsync that happens on the caller's goroutine, before the
// request is handed to the producer event loop.
func prepareSend(t *testing.T, p *partitionProducer, msg *ProducerMessage, flushImmediately bool) *sendRequest {
	sr := &sendRequest{
		pool: &sync.Pool{}, msg: msg, producer: p, callbackOnce: &sync.Once{},
		flushImmediately: flushImmediately, publishTime: time.Now(), chunkID: -1,
		uncompressedPayload: msg.Payload, uncompressedSize: int64(len(msg.Payload)),
	}
	p.updateMetaData(sr)
	require.NoError(t, p.updateChunkInfo(sr))
	return sr
}

// A send prepared on the caller's goroutine can reach the event loop after a later send. The
// sequence ids must still go out in increasing order, or the broker's deduplication drops the
// message whose id is below a batch's highest_sequence_id that it has already persisted.
func TestSequenceIDsOnTheWireAreMonotonicWhenUnbatchedSendIsDelayed(t *testing.T) {
	cnx := &frameRecordingConnection{}
	p := newSequenceTestProducer(t, cnx)

	// Message 0 is buffered in the batch.
	p.internalSend(prepareSend(t, p, &ProducerMessage{Payload: []byte("batched")}, false))

	// A delayed message is prepared, but pauses before it reaches the event loop.
	delayed := prepareSend(t, p, &ProducerMessage{Payload: []byte("delayed"), DeliverAfter: time.Minute}, false)
	require.False(t, delayed.sendAsBatch)

	// A synchronous send joins the batch and flushes it.
	p.internalSend(prepareSend(t, p, &ProducerMessage{Payload: []byte("sync")}, true))

	// The delayed message reaches the event loop.
	p.internalSend(delayed)

	require.Len(t, cnx.sends, 2)
	var previousHighest int64 = -1
	for i, send := range cnx.sends {
		lowest := int64(send.GetSequenceId())
		highest := lowest
		if send.HighestSequenceId != nil {
			highest = int64(send.GetHighestSequenceId())
		}
		require.Greater(t, lowest, previousHighest,
			"send %d has sequence ids [%d, %d] after a send with highest sequence id %d",
			i, lowest, highest, previousHighest)
		previousHighest = highest
	}
	require.Equal(t, uint64(2), cnx.sends[1].GetSequenceId())
}

// Buffered batch messages are written before an unbatched send, so a batch never carries
// sequence ids lower than an unbatched send written before it.
func TestUnbatchedSendFlushesBufferedBatchFirst(t *testing.T) {
	cnx := &frameRecordingConnection{}
	p := newSequenceTestProducer(t, cnx)

	p.internalSend(prepareSend(t, p, &ProducerMessage{Payload: []byte("batched")}, false))
	p.internalSend(prepareSend(t, p, &ProducerMessage{Payload: []byte("delayed"), DeliverAfter: time.Minute}, false))
	p.internalFlushCurrentBatch()

	require.Len(t, cnx.sends, 2)
	require.Equal(t, uint64(0), cnx.sends[0].GetSequenceId())
	require.Equal(t, int32(1), cnx.sends[0].GetNumMessages())
	require.Equal(t, uint64(1), cnx.sends[1].GetSequenceId())
}

// A sequence id set by the application is used as is, whichever path the message takes.
func TestExplicitSequenceIDIsKeptForUnbatchedSend(t *testing.T) {
	cnx := &frameRecordingConnection{}
	p := newSequenceTestProducer(t, cnx)

	id := int64(42)
	p.internalSend(prepareSend(t, p,
		&ProducerMessage{Payload: []byte("delayed"), DeliverAfter: time.Minute, SequenceID: &id}, false))
	p.internalSend(prepareSend(t, p, &ProducerMessage{Payload: []byte("auto"), DeliverAfter: time.Minute}, false))

	require.Len(t, cnx.sends, 2)
	require.Equal(t, uint64(42), cnx.sends[0].GetSequenceId())
	require.Equal(t, uint64(0), cnx.sends[1].GetSequenceId())
}
