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
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
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
					semaphore: semaphore, reservedSemaphore: 1, memLimit: memory, reservedMem: 10,
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

// closeTestRPC answers CLOSE_PRODUCER locally so doClose can run without a broker. If closeEntered is set, the
// close RPC signals it and then waits for closeRelease, so a test can act while the RPC is in flight.
type closeTestRPC struct {
	internal.RPCClient
	closeEntered chan struct{}
	closeRelease chan struct{}
}

func (*closeTestRPC) NewRequestID() uint64 { return 1 }

func (r *closeTestRPC) RequestOnCnx(_ internal.Connection, _ uint64, cmd pb.BaseCommand_Type,
	_ proto.Message) (*internal.RPCResult, error) {
	if cmd == pb.BaseCommand_CLOSE_PRODUCER && r.closeEntered != nil {
		close(r.closeEntered)
		<-r.closeRelease
	}
	return nil, nil
}

// gatedSemaphore signals entered when Acquire is called and waits for release before acquiring, so a test knows the
// sender has passed the producer state check and controls when it continues.
type gatedSemaphore struct {
	internal.Semaphore
	entered chan struct{}
	release chan struct{}
}

func (s *gatedSemaphore) Acquire(ctx context.Context) bool {
	close(s.entered)
	<-s.release
	return s.Semaphore.Acquire(ctx)
}

// doneSignalCtx signals entered the first time Done is called. FlushWithCtx calls Done only in its enqueue select,
// after the producer state check.
type doneSignalCtx struct {
	context.Context
	once    sync.Once
	entered chan struct{}
}

func (c *doneSignalCtx) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

// newClosableTestProducer builds a ready partitionProducer that can run its event loop and doClose without a broker.
func newClosableTestProducer(t *testing.T, maxPending int) *partitionProducer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p := &partitionProducer{
		client: &client{
			rpcClient: &closeTestRPC{},
			memLimit:  internal.NewMemoryLimitController(1<<20, 0),
			metrics:   internal.NewMetricsProvider(4, nil, prometheus.NewRegistry()),
		},
		topic:            "topic",
		log:              plog.DefaultNopLogger(),
		options:          &ProducerOptions{},
		dataChan:         make(chan *sendRequest, maxPending),
		cmdChan:          make(chan interface{}, 10),
		closedCh:         make(chan struct{}),
		closingCh:        make(chan struct{}),
		loopDone:         make(chan struct{}),
		connectClosedCh:  make(chan *connectionClosed, 1),
		batchFlushTicker: time.NewTicker(time.Hour),
		publishSemaphore: internal.NewSemaphore(int32(maxPending)),
		pendingQueue:     internal.NewBlockingQueue(maxPending),
		metrics:          internal.NewMetricsProvider(4, nil, prometheus.NewRegistry()).GetLeveledMetrics("topic"),
		ctx:              ctx,
		cancelFunc:       cancel,
	}
	p.setProducerState(producerReady)
	p._setConn(&dummyConnection{})
	return p
}

// startEventLoop runs the producer event loop and returns a channel that is closed when the loop exits.
func startEventLoop(p *partitionProducer) <-chan struct{} {
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		p.runEventsLoop()
	}()
	return exited
}

// holdCloseAfterStateCheck makes Close stop right after its state check, where it calls cancelFunc. It returns a
// channel that is closed when Close gets there, and a function that lets Close continue.
func holdCloseAfterStateCheck(p *partitionProducer) (<-chan struct{}, func()) {
	entered, release := make(chan struct{}), make(chan struct{})
	cancel := p.cancelFunc
	p.cancelFunc = func() {
		close(entered)
		<-release
		cancel()
	}
	return entered, func() { close(release) }
}

// addPendingSend puts a sent but unacknowledged message in the pending queue, holding one publish permit.
func addPendingSend(t *testing.T, p *partitionProducer, cb func(MessageID, *ProducerMessage, error)) {
	t.Helper()
	require.True(t, p.publishSemaphore.TryAcquire())
	p.metrics.MessagesPending.Inc()
	sr := &sendRequest{
		producer: p, msg: &ProducerMessage{}, callbackOnce: &sync.Once{}, publishTime: time.Now(),
		semaphore: p.publishSemaphore, reservedSemaphore: 1, callback: cb,
	}
	buf := internal.NewBuffer(1)
	buf.Retain()
	p.pendingQueue.Put(&pendingItem{
		buffer: buf, sequenceID: 1, createdAt: time.Now(), sentAt: time.Now(), sendRequests: []interface{}{sr},
	})
}

// newTestTopLevelProducer wraps p in a producer, so a test can call the public Close and its closeOnce.
func newTestTopLevelProducer(p *partitionProducer) *producer {
	p.client.handlers = internal.NewClientHandlers()
	pp := &producer{client: p.client, topic: p.topic, log: p.log, metrics: p.metrics, stopDiscovery: func() {}}
	pp.producers.Store([]Producer{p})
	return pp
}

// runAsync runs f in a goroutine and returns a channel that is closed when it returns.
func runAsync(f func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	return done
}

// callOutcome records what a concurrent call returned, or whether it panicked.
type callOutcome struct {
	err      error
	panicked any
}

func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// Regression for #1583: a sender that passed the state check before the producer closed itself must get
// ErrProducerClosed. It must not panic with "send on closed channel".
func TestSendAsyncBlockedWhenProducerClosesDoesNotPanic(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	go p.runEventsLoop()
	sem := &gatedSemaphore{Semaphore: p.publishSemaphore, entered: make(chan struct{}), release: make(chan struct{})}
	p.publishSemaphore = sem

	resCh := make(chan callOutcome, 1)
	go func() {
		var out callOutcome
		defer func() {
			out.panicked = recover()
			resCh <- out
		}()
		p.SendAsync(context.Background(), &ProducerMessage{Payload: []byte("blocked")},
			func(_ MessageID, _ *ProducerMessage, err error) { out.err = err })
	}()

	// The sender is in Acquire, so it has passed the state check. Close the producer the way the reconnect path
	// does, and only then let the sender continue to the enqueue.
	awaitSignal(t, sem.entered, "the sender to reach Acquire")
	p.doClose(ErrTopicTerminated)
	close(sem.release)

	select {
	case out := <-resCh:
		require.Nil(t, out.panicked, "SendAsync panicked while the producer was closing")
		require.ErrorIs(t, out.err, ErrProducerClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("SendAsync did not return after the producer closed")
	}
}

// Regression for #1583: a FlushWithCtx blocked on a full cmdChan when the producer closes itself must
// return ErrProducerClosed. It must not panic with "send on closed channel".
func TestFlushBlockedWhenProducerClosesDoesNotPanic(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	// No event loop runs here, so the full cmdChan keeps FlushWithCtx blocked on its send.
	close(p.loopDone)
	for i := 0; i < cap(p.cmdChan); i++ {
		p.cmdChan <- &flushRequest{doneCh: make(chan struct{})}
	}

	ctx := &doneSignalCtx{Context: context.Background(), entered: make(chan struct{})}
	resCh := make(chan callOutcome, 1)
	go func() {
		var out callOutcome
		defer func() {
			out.panicked = recover()
			resCh <- out
		}()
		out.err = p.FlushWithCtx(ctx)
	}()

	awaitSignal(t, ctx.entered, "FlushWithCtx to reach its enqueue")
	p.doClose(ErrTopicTerminated)

	select {
	case out := <-resCh:
		require.Nil(t, out.panicked, "FlushWithCtx panicked while the producer was closing")
		require.ErrorIs(t, out.err, ErrProducerClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("FlushWithCtx did not return after the producer closed")
	}
}

// Close passes its state check, then the producer starts closing itself. Close queues its request while the close
// RPC is in flight, and must return once the producer is closed.
func TestCloseQueuedDuringSelfCloseRPCReturns(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	rpc := &closeTestRPC{closeEntered: make(chan struct{}), closeRelease: make(chan struct{})}
	p.client.rpcClient = rpc
	startEventLoop(p)
	closeEntered, releaseClose := holdCloseAfterStateCheck(p)

	closeDone := runAsync(p.Close)
	awaitSignal(t, closeEntered, "Close to pass its state check")
	selfCloseDone := runAsync(func() { p.doClose(ErrTopicTerminated) })
	awaitSignal(t, rpc.closeEntered, "the self-close RPC to start")

	releaseClose()
	close(rpc.closeRelease)
	awaitSignal(t, closeDone, "Close to return")
	awaitSignal(t, selfCloseDone, "the self-close to finish")
}

// Regression for #1584: Close passes its state check, then the producer closes itself and the event loop exits.
// Close then queues its request into the buffered cmdChan, where nobody takes it. Close must still return.
func TestCloseAfterSelfCloseFinishedReturns(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	loopExited := startEventLoop(p)
	closeEntered, releaseClose := holdCloseAfterStateCheck(p)

	closeDone := runAsync(p.Close)
	awaitSignal(t, closeEntered, "Close to pass its state check")
	p.doClose(ErrTopicTerminated)
	awaitSignal(t, loopExited, "the event loop to exit")

	releaseClose()
	awaitSignal(t, closeDone, "Close to return after the producer closed itself")
}

// Regression for #1584: Close blocked on a full cmdChan must return when the producer closes itself.
func TestCloseBlockedOnFullCmdChanReturnsWhenProducerClosesItself(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	// No event loop runs, so the full cmdChan keeps Close blocked on its send.
	close(p.loopDone)
	for i := 0; i < cap(p.cmdChan); i++ {
		p.cmdChan <- &flushRequest{doneCh: make(chan struct{})}
	}
	closeEntered, releaseClose := holdCloseAfterStateCheck(p)

	closeDone := runAsync(p.Close)
	awaitSignal(t, closeEntered, "Close to pass its state check")
	releaseClose()
	p.doClose(ErrTopicTerminated)
	awaitSignal(t, closeDone, "Close to return after the producer closed itself")
}

// A pending message fails while the producer closes itself, and its callback makes the first public Close call.
// Close must not wait for the teardown that is running the callback.
func TestPendingCallbackClosesProducerDuringSelfClose(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	pp := newTestTopLevelProducer(p)
	startEventLoop(p)
	cbErr := make(chan error, 1)
	addPendingSend(t, p, func(_ MessageID, _ *ProducerMessage, err error) {
		pp.Close()
		cbErr <- err
	})

	selfCloseDone := runAsync(func() { p.doClose(ErrTopicTerminated) })
	awaitSignal(t, selfCloseDone, "the self-close to finish")
	require.ErrorIs(t, <-cbErr, ErrTopicTerminated)
}

// A public Close passes the partition state check, then the producer closes itself. A pending message's callback
// calls the public Close too, and waits for the first call to leave closeOnce. The first call must return once the
// producer is closed, without waiting for the callbacks.
func TestPendingCallbackClosesProducerWhileCloseRacesSelfClose(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	pp := newTestTopLevelProducer(p)
	startEventLoop(p)
	cbEntered, cbErr := make(chan struct{}), make(chan error, 1)
	addPendingSend(t, p, func(_ MessageID, _ *ProducerMessage, err error) {
		close(cbEntered)
		pp.Close()
		cbErr <- err
	})
	closeEntered, releaseClose := holdCloseAfterStateCheck(p)

	closeDone := runAsync(pp.Close)
	awaitSignal(t, closeEntered, "Close to pass its state check")
	selfCloseDone := runAsync(func() { p.doClose(ErrTopicTerminated) })
	awaitSignal(t, cbEntered, "the pending callback to run")

	releaseClose()
	awaitSignal(t, closeDone, "the first Close to return")
	awaitSignal(t, selfCloseDone, "the self-close to finish")
	require.ErrorIs(t, <-cbErr, ErrTopicTerminated)
}

// An explicit Close fails a pending message. Close returns only after the message's callback has run, as before.
// The callback calls Close again, which must not wait for the teardown that runs it.
func TestExplicitCloseReturnsAfterPendingCallbacks(t *testing.T) {
	p := newClosableTestProducer(t, 2)
	startEventLoop(p)
	closeDone := make(chan struct{})
	cbErr, closedBeforeCallback := make(chan error, 1), make(chan bool, 1)
	addPendingSend(t, p, func(_ MessageID, _ *ProducerMessage, err error) {
		// Close must not return while this callback runs. The bound only limits how long that is checked.
		select {
		case <-closeDone:
			closedBeforeCallback <- true
		case <-time.After(200 * time.Millisecond):
			closedBeforeCallback <- false
		}
		p.Close()
		cbErr <- err
	})

	go func() {
		defer close(closeDone)
		p.Close()
	}()
	awaitSignal(t, closeDone, "Close to return")
	require.False(t, <-closedBeforeCallback, "Close returned before the pending callback finished")
	require.ErrorIs(t, <-cbErr, ErrProducerClosed)
	for i := 0; i < 2; i++ {
		require.True(t, p.publishSemaphore.TryAcquire(), "permit %d was not released", i)
	}
}

// A public Close queues its request after it cancelled the context, and the event loop handles a send before it
// would take the request. The send is not written, and the event loop runs no callback for it. Then the producer
// closes itself: the send fails with the self-close reason, and its callback calls the public Close while the first
// call holds closeOnce. Both calls must return.
func TestSendAfterCloseCancelFailsWhenSelfCloseWins(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	p.options = &ProducerOptions{DisableBatching: true}
	p.compressionProvider = internal.GetCompressionProvider(pb.CompressionType_NONE, compression.Level(0))
	p.sequenceIDGenerator = new(uint64)
	p.encryptor = internalcrypto.NewNoopEncryptor()
	p.client.metrics = internal.NewMetricsProvider(4, nil, prometheus.NewRegistry())
	conn := &countingWriteConnection{}
	p._setConn(conn)
	pp := newTestTopLevelProducer(p)
	cbErr := make(chan error, 1)
	p.SendAsync(context.Background(), &ProducerMessage{Payload: []byte("late")},
		func(_ MessageID, _ *ProducerMessage, err error) {
			pp.Close()
			cbErr <- err
		})
	require.Len(t, p.dataChan, 1)

	closeDone := runAsync(pp.Close)
	// No event loop runs. Wait until Close has cancelled the context and queued its request, and leave it queued.
	select {
	case cmd := <-p.cmdChan:
		p.cmdChan <- cmd
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not queue its request")
	}
	// Play the event loop: it handles the send first, then sees closingCh and exits without taking the request.
	p.internalSend(<-p.dataChan)
	close(p.loopDone)

	p.doClose(ErrTopicTerminated)
	awaitSignal(t, closeDone, "the first Close to return after the producer closed itself")
	require.ErrorIs(t, <-cbErr, ErrTopicTerminated)
	require.Zero(t, conn.writes.Load(), "a message was written after Close")
}

// Regression for the retryable backlog-quota path: failPendingMessages fails the pending messages but the producer
// keeps running. A Flush that runs while a callback is still running must not return nil while later pending
// messages are being failed.
func TestFlushWhilePendingMessagesFailReturnsError(t *testing.T) {
	p := newClosableTestProducer(t, 2)
	p.options = &ProducerOptions{DisableBatching: true}
	cbEntered, cbRelease := make(chan struct{}), make(chan struct{})
	addPendingSend(t, p, func(_ MessageID, _ *ProducerMessage, _ error) {
		close(cbEntered)
		<-cbRelease
	})
	addPendingSend(t, p, nil)

	failDone := runAsync(func() { p.failPendingMessages(ErrProducerBlockedQuotaExceeded) })
	awaitSignal(t, cbEntered, "the first pending callback to run")

	// Flush the way the event loop does, while the first callback is held.
	fr := &flushRequest{doneCh: make(chan struct{})}
	p.internalFlush(fr)
	select {
	case <-fr.doneCh:
		t.Fatalf("Flush completed with %v while the pending messages were being failed", fr.err)
	default:
	}

	close(cbRelease)
	awaitSignal(t, failDone, "failPendingMessages to return")
	awaitSignal(t, fr.doneCh, "Flush to complete")
	require.ErrorIs(t, fr.err, ErrProducerBlockedQuotaExceeded)
	for i := 0; i < 2; i++ {
		require.True(t, p.publishSemaphore.TryAcquire(), "permit %d was not released", i)
	}
}

// Regression for #1582: send requests still buffered in dataChan when the producer closes itself must fail
// with the close reason, and must release their permits and memory reservations.
func TestBufferedSendRequestsFailWhenProducerCloses(t *testing.T) {
	p := newClosableTestProducer(t, 10)
	// No event loop runs, so the requests stay in dataChan. Mark the loop as already exited.
	close(p.loopDone)

	const n = 3
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		p.SendAsync(context.Background(), &ProducerMessage{Payload: make([]byte, 1024)},
			func(_ MessageID, _ *ProducerMessage, err error) { results <- err })
	}
	require.Len(t, p.dataChan, n)
	require.Positive(t, p.client.memLimit.CurrentUsage())

	p.doClose(ErrTopicTerminated)

	for i := 0; i < n; i++ {
		select {
		case err := <-results:
			require.ErrorIs(t, err, ErrTopicTerminated)
		case <-time.After(5 * time.Second):
			t.Fatalf("callback %d of %d was never invoked", i+1, n)
		}
	}
	require.Zero(t, p.client.memLimit.CurrentUsage())
	for i := 0; i < 10; i++ {
		require.True(t, p.publishSemaphore.TryAcquire(), "permit %d was not released", i)
	}
}

// countingEncryptor counts Encrypt calls and fails them, so a test can see whether a batch was encoded.
type countingEncryptor struct {
	calls atomic.Int32
}

func (e *countingEncryptor) Encrypt([]byte, *pb.MessageMetadata) ([]byte, error) {
	e.calls.Add(1)
	return nil, errors.New("encrypt failed")
}

// countingGauge tracks the value of a gauge that is only incremented and decremented.
type countingGauge struct {
	prometheus.Gauge
	v atomic.Int64
}

func (g *countingGauge) Inc() { g.v.Add(1) }
func (g *countingGauge) Dec() { g.v.Add(-1) }

// countPendingMessages replaces the MessagesPending gauge of p with one the test can read.
func countPendingMessages(p *partitionProducer) *countingGauge {
	g := &countingGauge{Gauge: p.metrics.MessagesPending}
	p.metrics.MessagesPending = g
	return g
}

// setTestBatchBuilder gives p a batch builder of the given type whose encryptor is enc.
func setTestBatchBuilder(t *testing.T, p *partitionProducer, typ BatcherBuilderType, enc internalcrypto.Encryptor) {
	t.Helper()
	provider, err := GetBatcherBuilderProvider(typ)
	require.NoError(t, err)
	p.batchBuilder, err = provider(1000, 128*1024, 5*1024*1024, "test", 1, pb.CompressionType_NONE,
		compression.Level(0), buffersPool, p.client.metrics, p.log, enc)
	require.NoError(t, err)
	p.encryptor = enc
	p.sequenceIDGenerator = new(uint64)
}

func awaitErrors(t *testing.T, results <-chan error, n int, want error) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case err := <-results:
			require.ErrorIs(t, err, want)
		case <-time.After(5 * time.Second):
			t.Fatalf("callback %d of %d was never invoked", i+1, n)
		}
	}
}

// Regression for #1582: messages still in the open batch when the producer closes itself must fail their
// callbacks and release their memory reservations. The batch is discarded without being encoded.
func TestBatchedMessagesFailWhenProducerCloses(t *testing.T) {
	builders := map[string]BatcherBuilderType{"default": DefaultBatchBuilder, "key_based": KeyBasedBatchBuilder}
	for name, typ := range builders {
		t.Run(name, func(t *testing.T) {
			p := newClosableTestProducer(t, 10)
			enc := &countingEncryptor{}
			setTestBatchBuilder(t, p, typ, enc)
			// No event loop runs. Feed requests into the batch builder the way the loop does, one at a time.
			close(p.loopDone)

			const n = 5
			results := make(chan error, n)
			for i := 0; i < n; i++ {
				p.SendAsync(context.Background(), &ProducerMessage{Key: fmt.Sprint(i % 2), Payload: make([]byte, 1024)},
					func(_ MessageID, _ *ProducerMessage, err error) { results <- err })
			}
			for i := 0; i < n; i++ {
				p.internalSend(<-p.dataChan)
			}
			// The messages must be held in the open batch, not in pendingQueue, or this does not test the batch path.
			require.Zero(t, p.pendingQueue.Size())
			require.Positive(t, p.client.memLimit.CurrentUsage())

			p.doClose(ErrTopicTerminated)

			awaitErrors(t, results, n, ErrTopicTerminated)
			require.Zero(t, enc.calls.Load(), "the open batch was encoded on close")
			require.Zero(t, p.client.memLimit.CurrentUsage())
			for i := 0; i < 10; i++ {
				require.True(t, p.publishSemaphore.TryAcquire(), "permit %d was not released", i)
			}
		})
	}
}

// A queued send fails while the producer closes itself, and its callback calls the public Close. Close must not
// wait for the teardown that runs the callback.
func TestQueuedSendCallbackClosesProducerDuringSelfClose(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	pp := newTestTopLevelProducer(p)
	// No event loop runs, so the request stays in dataChan. Mark the loop as already exited.
	close(p.loopDone)
	cbErr := make(chan error, 1)
	p.SendAsync(context.Background(), &ProducerMessage{Payload: []byte("queued")},
		func(_ MessageID, _ *ProducerMessage, err error) {
			pp.Close()
			cbErr <- err
		})
	require.Len(t, p.dataChan, 1)

	selfCloseDone := runAsync(func() { p.doClose(ErrTopicTerminated) })
	awaitSignal(t, selfCloseDone, "the self-close to finish")
	require.ErrorIs(t, <-cbErr, ErrTopicTerminated)
}

// expectCloseAfterCallback returns a callback that fails the test if Close has already returned when it runs, then
// calls Close itself and reports its error. closeDone must be closed when the outer Close returns.
func expectCloseAfterCallback(t *testing.T, p *partitionProducer, closeDone <-chan struct{}) (
	func(MessageID, *ProducerMessage, error), <-chan error) {
	t.Helper()
	cbErr := make(chan error, 1)
	return func(_ MessageID, _ *ProducerMessage, err error) {
		// Close must not return while this callback runs. The bound only limits how long that is checked.
		select {
		case <-closeDone:
			cbErr <- errors.New("Close returned before the callback ran")
			return
		case <-time.After(200 * time.Millisecond):
		}
		p.Close()
		cbErr <- err
	}, cbErr
}

// playEventLoopClose takes the close request that Close queued, and closes the producer the way the event loop does.
func playEventLoopClose(t *testing.T, p *partitionProducer) {
	t.Helper()
	select {
	case cmd := <-p.cmdChan:
		require.IsType(t, &closeProducer{}, cmd)
		p.internalClose(cmd.(*closeProducer))
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not queue its request")
	}
}

// An explicit Close fails a queued send, and its callback calls Close again. The outer Close returns after the
// callback, and the nested call must not wait for the teardown that runs it.
func TestQueuedSendCallbackClosesProducerDuringExplicitClose(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	closeDone := make(chan struct{})
	cb, cbErr := expectCloseAfterCallback(t, p, closeDone)
	p.SendAsync(context.Background(), &ProducerMessage{Payload: []byte("queued")}, cb)
	require.Len(t, p.dataChan, 1)

	// No event loop runs. The test plays the loop, so the send request is still queued when the producer closes.
	go func() {
		defer close(closeDone)
		p.Close()
	}()
	playEventLoopClose(t, p)

	awaitSignal(t, closeDone, "Close to return")
	require.ErrorIs(t, <-cbErr, ErrProducerClosed)
}

// A send request reaches the event loop after Close cancelled the producer context but before the loop takes the
// close request. It must not be sent, and its callback must run before Close returns.
func TestSendAfterCloseCancelFailsBeforeCloseReturns(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	p.options = &ProducerOptions{DisableBatching: true}
	p.compressionProvider = internal.GetCompressionProvider(pb.CompressionType_NONE, compression.Level(0))
	p.sequenceIDGenerator = new(uint64)
	p.encryptor = internalcrypto.NewNoopEncryptor()
	p.client.metrics = internal.NewMetricsProvider(4, nil, prometheus.NewRegistry())
	conn := &countingWriteConnection{}
	p._setConn(conn)
	closeDone := make(chan struct{})
	cb, cbErr := expectCloseAfterCallback(t, p, closeDone)
	p.SendAsync(context.Background(), &ProducerMessage{Payload: []byte("late")}, cb)
	require.Len(t, p.dataChan, 1)

	go func() {
		defer close(closeDone)
		p.Close()
	}()
	// Close cancels the context before it queues the close request. Play the loop: handle the send first.
	select {
	case cmd := <-p.cmdChan:
		p.internalSend(<-p.dataChan)
		p.internalClose(cmd.(*closeProducer))
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not queue its request")
	}

	awaitSignal(t, closeDone, "Close to return")
	require.ErrorIs(t, <-cbErr, ErrProducerClosed)
	require.Zero(t, conn.writes.Load(), "a message was written after Close")
	require.True(t, p.publishSemaphore.TryAcquire(), "the permit was not released")
}

// countingWriteConnection counts WriteData calls.
type countingWriteConnection struct {
	dummyConnection
	writes atomic.Int32
}

func (c *countingWriteConnection) WriteData(_ context.Context, _ internal.Buffer) { c.writes.Add(1) }

// A batch fails to serialize on the event loop, and the failed message's callback calls Close. Close needs the event
// loop, so the callback must not run on it.
func TestBatchErrorCallbackClosesProducer(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	enc := &countingEncryptor{}
	setTestBatchBuilder(t, p, DefaultBatchBuilder, enc)
	startEventLoop(p)

	cbErr := make(chan error, 1)
	p.SendAsync(context.Background(), &ProducerMessage{Payload: []byte("batched")},
		func(_ MessageID, _ *ProducerMessage, err error) {
			p.Close()
			cbErr <- err
		})
	// Flush makes the event loop serialize the batch, which fails in the encryptor.
	flushDone := runAsync(func() { _ = p.Flush() })

	select {
	case err := <-cbErr:
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrProducerClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("Close called from the batch error callback did not return")
	}
	awaitSignal(t, flushDone, "Flush to return")
	require.EqualValues(t, 1, enc.calls.Load())
}

// Regression for #1582: a chunked send still queued in dataChan holds one permit per chunk. Failing it on close must
// return all of them.
func TestQueuedChunkedSendReleasesAllPermitsWhenProducerCloses(t *testing.T) {
	const chunks = 3
	p := newClosableTestProducer(t, chunks)
	p.options = &ProducerOptions{DisableBatching: true, EnableChunking: true, ChunkMaxMessageSize: 1024}
	p.compressionProvider = internal.GetCompressionProvider(pb.CompressionType_NONE, compression.Level(0))
	p.sequenceIDGenerator = new(uint64)
	// No event loop runs, so the request stays in dataChan. Mark the loop as already exited.
	close(p.loopDone)
	pending := countPendingMessages(p)

	results := make(chan error, 1)
	p.SendAsync(context.Background(), &ProducerMessage{Payload: make([]byte, 1024*chunks-100)},
		func(_ MessageID, _ *ProducerMessage, err error) { results <- err })
	require.Len(t, p.dataChan, 1)
	require.False(t, p.publishSemaphore.TryAcquire(), "the chunked send should hold every permit")
	require.EqualValues(t, chunks, pending.v.Load())

	p.doClose(ErrTopicTerminated)

	awaitErrors(t, results, 1, ErrTopicTerminated)
	for i := 0; i < chunks; i++ {
		require.True(t, p.publishSemaphore.TryAcquire(), "permit %d was not released", i)
	}
	require.Zero(t, pending.v.Load())
	require.Zero(t, p.client.memLimit.CurrentUsage())
}

// A chunked send already split into chunks by the event loop holds one permit per pending chunk. Failing the
// chunks on close must return all of them.
func TestPendingChunksReleaseAllPermitsWhenProducerCloses(t *testing.T) {
	const chunks = 3
	p := newClosableTestProducer(t, chunks)
	p.options = &ProducerOptions{DisableBatching: true, EnableChunking: true, ChunkMaxMessageSize: 1024}
	p.compressionProvider = internal.GetCompressionProvider(pb.CompressionType_NONE, compression.Level(0))
	p.sequenceIDGenerator = new(uint64)
	p.encryptor = internalcrypto.NewNoopEncryptor()
	p.client.metrics = internal.NewMetricsProvider(4, nil, prometheus.NewRegistry())
	close(p.loopDone)
	pending := countPendingMessages(p)

	results := make(chan error, 1)
	p.SendAsync(context.Background(), &ProducerMessage{Payload: make([]byte, 1024*chunks-100)},
		func(_ MessageID, _ *ProducerMessage, err error) { results <- err })
	// Split the request into chunks the way the event loop does. The producer is not connected, so the chunks
	// stay in pendingQueue.
	p.setProducerState(producerConnecting)
	p.internalSend(<-p.dataChan)
	require.Equal(t, chunks, p.pendingQueue.Size())

	p.doClose(ErrTopicTerminated)

	awaitErrors(t, results, 1, ErrTopicTerminated)
	for i := 0; i < chunks; i++ {
		require.True(t, p.publishSemaphore.TryAcquire(), "permit %d was not released", i)
	}
	require.False(t, p.publishSemaphore.TryAcquire(), "more permits were released than acquired")
	require.Zero(t, pending.v.Load())
	require.Zero(t, p.client.memLimit.CurrentUsage())
}

// An explicit public Close fails a pending message and a queued send, and both callbacks call the public Close
// again. The nested calls must return although the outer call has not returned yet, and the outer call must return
// after the callbacks.
func TestShutdownCallbacksCallPublicCloseDuringExplicitClose(t *testing.T) {
	p := newClosableTestProducer(t, 2)
	pp := newTestTopLevelProducer(p)
	closeDone := make(chan struct{})
	cbErrs := make(chan error, 2)
	cb := func(_ MessageID, _ *ProducerMessage, err error) {
		// The outer Close must not return while this callback runs. The bound only limits how long that is checked.
		select {
		case <-closeDone:
			cbErrs <- errors.New("Close returned before the callback ran")
			return
		case <-time.After(200 * time.Millisecond):
		}
		pp.Close()
		cbErrs <- err
	}
	addPendingSend(t, p, cb)
	p.SendAsync(context.Background(), &ProducerMessage{Payload: []byte("queued")}, cb)
	require.Len(t, p.dataChan, 1)

	go func() {
		defer close(closeDone)
		pp.Close()
	}()
	// No event loop runs. Play it, so the send request is still queued when the producer closes.
	loopClosed := runAsync(func() {
		cmd := <-p.cmdChan
		p.internalClose(cmd.(*closeProducer))
	})

	awaitSignal(t, closeDone, "the outer public Close to return")
	awaitSignal(t, loopClosed, "the event loop to finish the close")
	for i := 0; i < 2; i++ {
		require.ErrorIs(t, <-cbErrs, ErrProducerClosed)
	}
}

// reconnectTestRPC lets grabCnx reconnect the producer without a broker.
type reconnectTestRPC struct {
	closeTestRPC
	cnx internal.Connection
}

func (r *reconnectTestRPC) LookupService(_ string) (internal.LookupService, error) {
	u, _ := url.Parse("pulsar://localhost:6650")
	return &grabConnMockLookup{result: &internal.LookupResult{LogicalAddr: u, PhysicalAddr: u}}, nil
}

func (r *reconnectTestRPC) RequestOnCnx(cnx internal.Connection, id uint64, cmd pb.BaseCommand_Type,
	msg proto.Message) (*internal.RPCResult, error) {
	if cmd == pb.BaseCommand_PRODUCER {
		return &internal.RPCResult{Cnx: r.cnx, Response: &pb.BaseCommand{
			ProducerSuccess: &pb.CommandProducerSuccess{ProducerName: proto.String("test")},
		}}, nil
	}
	return r.closeTestRPC.RequestOnCnx(cnx, id, cmd, msg)
}

// A send reaches the event loop after Close cancelled the producer context, and a reconnect that was already
// running finishes before the loop takes the close request. The reconnect must not write that message: Close fails
// it with ErrProducerClosed, and a caller that retries could otherwise duplicate it.
func TestSendAfterCloseCancelIsNotResentOnReconnect(t *testing.T) {
	p := newClosableTestProducer(t, 1)
	p.options = &ProducerOptions{DisableBatching: true}
	p.compressionProvider = internal.GetCompressionProvider(pb.CompressionType_NONE, compression.Level(0))
	p.sequenceIDGenerator = new(uint64)
	p.encryptor = internalcrypto.NewNoopEncryptor()
	p.client.metrics = internal.NewMetricsProvider(4, nil, prometheus.NewRegistry())
	conn := &countingWriteConnection{}
	p._setConn(conn)
	p.client.rpcClient = &reconnectTestRPC{cnx: conn}
	p.client.cnxPool = &grabConnMockPool{cnx: conn}
	cbErr := make(chan error, 1)
	p.SendAsync(context.Background(), &ProducerMessage{Payload: []byte("late")},
		func(_ MessageID, _ *ProducerMessage, err error) { cbErr <- err })
	require.Len(t, p.dataChan, 1)

	// Hold Close after it cancelled the context, before it queues its request.
	cancelled, release := make(chan struct{}), make(chan struct{})
	cancel := p.cancelFunc
	p.cancelFunc = func() {
		cancel()
		close(cancelled)
		<-release
	}
	closeDone := runAsync(p.Close)
	awaitSignal(t, cancelled, "Close to cancel the context")

	// Play the event loop: the send reaches it after the context was cancelled. Then the reconnect finishes and
	// resends what is in pendingQueue.
	p.internalSend(<-p.dataChan)
	require.NoError(t, p.grabCnx(""))
	require.Zero(t, conn.writes.Load(), "the reconnect wrote a message sent after Close")

	close(release)
	playEventLoopClose(t, p)
	awaitSignal(t, closeDone, "Close to return")
	require.ErrorIs(t, <-cbErr, ErrProducerClosed)
	require.Zero(t, conn.writes.Load(), "a message was written after Close")
	require.True(t, p.publishSemaphore.TryAcquire(), "the permit was not released")
}
