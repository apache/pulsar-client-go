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
		},
		topic:            "topic",
		log:              plog.DefaultNopLogger(),
		options:          &ProducerOptions{},
		dataChan:         make(chan *sendRequest, maxPending),
		cmdChan:          make(chan interface{}, 10),
		closedCh:         make(chan struct{}),
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

// An explicit Close fails a pending message, and its callback calls the public Close again. The nested call waits
// for the outer one to leave closeOnce, so the outer Close must not wait for the callbacks. The permit must already
// be released when Close returns.
func TestPendingCallbackClosesProducerDuringExplicitClose(t *testing.T) {
	p := newClosableTestProducer(t, 2)
	pp := newTestTopLevelProducer(p)
	startEventLoop(p)
	cbErr := make(chan error, 1)
	addPendingSend(t, p, func(_ MessageID, _ *ProducerMessage, err error) {
		pp.Close()
		cbErr <- err
	})

	closeDone := runAsync(pp.Close)
	awaitSignal(t, closeDone, "Close to return")
	for i := 0; i < 2; i++ {
		require.True(t, p.publishSemaphore.TryAcquire(), "permit %d was not released", i)
	}
	select {
	case err := <-cbErr:
		require.ErrorIs(t, err, ErrProducerClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("the pending callback did not finish")
	}
}
