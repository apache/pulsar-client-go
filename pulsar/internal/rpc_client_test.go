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

package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestNewRPCClient_InvalidURL_ShouldNotPanic(t *testing.T) {
	// Test that NewRPCClient doesn't panic with invalid URL
	invalidURL := "invalid://scheme"

	// This should not panic and should return an error
	_, err := NewRPCClient(invalidURL, nil, 0, log.DefaultNopLogger(), nil, "", nil, nil, nil)
	assert.Error(t, err)
}

func TestLookupService_InvalidURL_ShouldNotPanic(t *testing.T) {
	// Create a minimal RPC client for testing
	validURL := "pulsar://localhost:6650"
	rpcClient, err := NewRPCClient(validURL, nil, 0, log.DefaultNopLogger(), nil, "", nil, nil, nil)
	assert.NoError(t, err)

	// Test that LookupService doesn't panic with invalid URL
	_, err = rpcClient.LookupService("invalid://url")
	assert.Error(t, err)
}

func TestLookupService_InvalidScheme_ShouldNotPanic(t *testing.T) {
	// Create a minimal RPC client for testing
	validURL := "pulsar://localhost:6650"
	rpcClient, err := NewRPCClient(validURL, nil, 0, log.DefaultNopLogger(), nil, "", nil, nil, nil)
	assert.NoError(t, err)

	// Test that LookupService doesn't panic with invalid scheme
	_, err = rpcClient.LookupService("ftp://localhost:21")
	assert.Error(t, err)
}

func newTestRPCClient(requestTimeout time.Duration) *rpcClient {
	return &rpcClient{
		requestTimeout: requestTimeout,
		log:            log.DefaultNopLogger(),
		metrics:        NewMetricsProvider(0, map[string]string{}, prometheus.NewRegistry()),
	}
}

func TestRequestOnCnxWithContext_CancelledReturnsCauseAndCleansPending(t *testing.T) {
	c := newTestConnection()
	c.setStateReady()

	registered := make(chan struct{})
	go func() {
		req := <-c.incomingRequestsCh
		c.pendingLock.Lock()
		c.pendingReqs[*req.id] = req
		c.pendingLock.Unlock()
		close(registered)
	}()

	rpc := newTestRPCClient(5 * time.Second)
	cancelCause := errors.New("broker closed resource during connection")
	ctx, cancel := context.WithCancelCause(context.Background())

	reqID := uint64(7)
	errCh := make(chan error, 1)
	go func() {
		_, err := rpc.RequestOnCnxWithContext(ctx, c, reqID, pb.BaseCommand_PRODUCER, &pb.CommandProducer{
			Topic:      proto.String("persistent://public/default/test"),
			ProducerId: proto.Uint64(1),
			RequestId:  proto.Uint64(reqID),
		})
		errCh <- err
	}()

	<-registered
	cancel(cancelCause)

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, cancelCause)
	case <-time.After(time.Second):
		t.Fatal("RequestOnCnxWithContext did not return promptly after cancellation")
	}

	_, ok := c.findPendingRequest(reqID)
	assert.False(t, ok, "cancelled request should be removed from pendingReqs")

	// A late broker response for the cancelled request must be ignored without blocking.
	c.handleResponse(reqID, &pb.BaseCommand{
		Type: pb.BaseCommand_PRODUCER_SUCCESS.Enum(),
		ProducerSuccess: &pb.CommandProducerSuccess{
			RequestId:    proto.Uint64(reqID),
			ProducerName: proto.String("late-producer"),
		},
	})
}

func TestRequestOnCnxWithContext_TimeoutCleansPending(t *testing.T) {
	c := newTestConnection()
	c.setStateReady()

	go func() {
		req := <-c.incomingRequestsCh
		c.pendingLock.Lock()
		c.pendingReqs[*req.id] = req
		c.pendingLock.Unlock()
	}()

	rpc := newTestRPCClient(40 * time.Millisecond)
	reqID := uint64(9)
	_, err := rpc.RequestOnCnxWithContext(context.Background(), c, reqID, pb.BaseCommand_PRODUCER, &pb.CommandProducer{
		Topic:      proto.String("persistent://public/default/test"),
		ProducerId: proto.Uint64(1),
		RequestId:  proto.Uint64(reqID),
	})
	require.ErrorIs(t, err, ErrRequestTimeOut)

	_, ok := c.findPendingRequest(reqID)
	assert.False(t, ok, "timed out request should be removed from pendingReqs")
}

func TestRequestOnCnxWithContext_AlreadyCancelledSkipsSend(t *testing.T) {
	c := newTestConnection()
	c.setStateReady()

	rpc := newTestRPCClient(time.Second)
	cancelCause := errors.New("already cancelled")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cancelCause)

	_, err := rpc.RequestOnCnxWithContext(ctx, c, 11, pb.BaseCommand_PRODUCER, &pb.CommandProducer{
		Topic:      proto.String("persistent://public/default/test"),
		ProducerId: proto.Uint64(1),
		RequestId:  proto.Uint64(11),
	})
	require.ErrorIs(t, err, cancelCause)
	assert.Equal(t, 0, len(c.incomingRequestsCh))
}

func TestRequestOnCnxWithContext_ProducerNotReadyCancelledByContext(t *testing.T) {
	c := newTestConnection()
	c.setStateReady()

	notReadyDelivered := make(chan struct{})
	go func() {
		req := <-c.incomingRequestsCh
		c.pendingLock.Lock()
		c.pendingReqs[*req.id] = req
		c.pendingLock.Unlock()

		// Deliver ProducerReady=false (WaitForExclusive queued response), which leaves
		// the request in pendingReqs and disables the request timeout.
		c.receivedCommand(&pb.BaseCommand{
			Type: pb.BaseCommand_PRODUCER_SUCCESS.Enum(),
			ProducerSuccess: &pb.CommandProducerSuccess{
				RequestId:     proto.Uint64(*req.id),
				ProducerName:  proto.String("queued-producer"),
				ProducerReady: proto.Bool(false),
			},
		}, nil)
		close(notReadyDelivered)
	}()

	rpc := newTestRPCClient(40 * time.Millisecond)
	cancelCause := errors.New("broker closed while queued")
	ctx, cancel := context.WithCancelCause(context.Background())

	reqID := uint64(13)
	errCh := make(chan error, 1)
	go func() {
		_, err := rpc.RequestOnCnxWithContext(ctx, c, reqID, pb.BaseCommand_PRODUCER, &pb.CommandProducer{
			Topic:      proto.String("persistent://public/default/test"),
			ProducerId: proto.Uint64(1),
			RequestId:  proto.Uint64(reqID),
		})
		errCh <- err
	}()

	<-notReadyDelivered
	// Wait past requestTimeout to verify timeout was disabled after ProducerReady=false.
	time.Sleep(80 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("expected queued producer request to keep waiting, got %v", err)
	default:
	}

	cancel(cancelCause)
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, cancelCause)
	case <-time.After(time.Second):
		t.Fatal("queued producer request did not return on context cancel")
	}

	_, ok := c.findPendingRequest(reqID)
	assert.False(t, ok, "queued producer request should be cleaned from pendingReqs on cancel")
}

func TestRequestOnCnxWithContext_CancelWhileResponseBufferFullDoesNotDeadlock(t *testing.T) {
	c := newTestConnection()
	c.setStateReady()

	// Simulate the race where a ProducerReady=false notification has already filled
	// ch (capacity 1) when the select chooses ctx.Done() and invokes CloseRequest
	// synchronously on the same goroutine.
	for i := 0; i < 20; i++ {
		reqID := uint64(100 + i)
		cancelCause := errors.New("broker closed while producer not ready buffered")
		ctx, cancel := context.WithCancelCause(context.Background())

		go func() {
			req := <-c.incomingRequestsCh
			c.pendingLock.Lock()
			c.pendingReqs[*req.id] = req
			c.pendingLock.Unlock()

			// Deliver ProducerReady=false to fill ch, and immediately cancel ctx so
			// both ch and ctx.Done() are ready concurrently.
			c.receivedCommand(&pb.BaseCommand{
				Type: pb.BaseCommand_PRODUCER_SUCCESS.Enum(),
				ProducerSuccess: &pb.CommandProducerSuccess{
					RequestId:     proto.Uint64(*req.id),
					ProducerName:  proto.String("queued-producer"),
					ProducerReady: proto.Bool(false),
				},
			}, nil)
			cancel(cancelCause)
		}()

		rpc := newTestRPCClient(time.Second)
		errCh := make(chan error, 1)
		go func() {
			_, err := rpc.RequestOnCnxWithContext(ctx, c, reqID, pb.BaseCommand_PRODUCER, &pb.CommandProducer{
				Topic:      proto.String("persistent://public/default/test"),
				ProducerId: proto.Uint64(1),
				RequestId:  proto.Uint64(reqID),
			})
			errCh <- err
		}()

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, cancelCause)
		case <-time.After(time.Second):
			t.Fatalf("iteration %d deadlocked when cancelling with buffered ProducerReady=false", i)
		}
	}
}

func TestRequestOnCnxWithContext_CancelWhileSendQueueFull(t *testing.T) {
	c := newTestConnection()
	c.setStateReady()

	// Fill incomingRequestsCh completely so enqueueing blocks.
	for i := 0; i < cap(c.incomingRequestsCh); i++ {
		c.incomingRequestsCh <- &request{}
	}

	rpc := newTestRPCClient(5 * time.Second)
	cancelCause := errors.New("cancelled while send queue full")
	ctx, cancel := context.WithCancelCause(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, err := rpc.RequestOnCnxWithContext(ctx, c, 200, pb.BaseCommand_PRODUCER, &pb.CommandProducer{
			Topic:      proto.String("persistent://public/default/test"),
			ProducerId: proto.Uint64(1),
			RequestId:  proto.Uint64(200),
		})
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel(cancelCause)

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, cancelCause)
	case <-time.After(time.Second):
		t.Fatal("RequestOnCnxWithContext stayed blocked on full incomingRequestsCh after cancel")
	}
}

func TestRequestOnCnxWithContext_TimeoutWhileSendQueueFull(t *testing.T) {
	c := newTestConnection()
	c.setStateReady()

	for i := 0; i < cap(c.incomingRequestsCh); i++ {
		c.incomingRequestsCh <- &request{}
	}

	rpc := newTestRPCClient(30 * time.Millisecond)
	_, err := rpc.RequestOnCnxWithContext(context.Background(), c, 201, pb.BaseCommand_PRODUCER, &pb.CommandProducer{
		Topic:      proto.String("persistent://public/default/test"),
		ProducerId: proto.Uint64(1),
		RequestId:  proto.Uint64(201),
	})
	require.ErrorIs(t, err, ErrRequestTimeOut)
}
