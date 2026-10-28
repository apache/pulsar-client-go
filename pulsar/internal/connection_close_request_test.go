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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/stretchr/testify/assert"
)

var errRequestCancelled = errors.New("request cancelled")

func registerPendingRequest(c *connection, requestID uint64,
	callback func(*pb.BaseCommand, error)) {
	c.pendingLock.Lock()
	c.pendingReqs[requestID] = &request{
		id:       &requestID,
		cmd:      &pb.BaseCommand{},
		callback: callback,
	}
	c.pendingLock.Unlock()
}

func TestCloseRequestFailsRegisteredRequestOnce(t *testing.T) {
	c := newTestConnection()

	requestID := uint64(7)
	callbacks := make(chan error, 3)
	registerPendingRequest(c, requestID, func(_ *pb.BaseCommand, err error) {
		callbacks <- err
	})

	c.CloseRequest(requestID, errRequestCancelled)

	select {
	case err := <-callbacks:
		assert.ErrorIs(t, err, errRequestCancelled)
	case <-time.After(time.Second):
		t.Fatal("CloseRequest did not complete the pending request")
	}

	c.CloseRequest(requestID, errRequestCancelled)
	c.handleResponse(requestID, &pb.BaseCommand{})

	select {
	case err := <-callbacks:
		t.Fatalf("request completed twice: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	c.pendingLock.Lock()
	remembered := len(c.cancelledReqs)
	c.pendingLock.Unlock()
	assert.Zero(t, remembered)
}

func TestCloseRequestAfterResponseNotRemembered(t *testing.T) {
	c := newTestConnection()

	requestID := uint64(5)
	callbacks := make(chan error, 1)
	registerPendingRequest(c, requestID, func(_ *pb.BaseCommand, err error) {
		callbacks <- err
	})

	if req, ok := c.deletePendingRequest(requestID); ok {
		req.callback(&pb.BaseCommand{}, nil)
	}

	select {
	case err := <-callbacks:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("response did not complete the request")
	}

	c.CloseRequest(requestID, errRequestCancelled)

	c.pendingLock.Lock()
	remembered := len(c.cancelledReqs)
	c.pendingLock.Unlock()
	assert.Zero(t, remembered,
		"cancellation of a completed request must not be remembered")
}

func TestCloseRequestBeforeRegistration(t *testing.T) {
	c := newTestConnection()

	requestID := uint64(11)
	c.CloseRequest(requestID, errRequestCancelled)

	callbacks := make(chan error, 1)
	c.internalSendRequest(&request{
		id:  &requestID,
		cmd: &pb.BaseCommand{},
		callback: func(_ *pb.BaseCommand, err error) {
			callbacks <- err
		},
	})

	select {
	case err := <-callbacks:
		assert.ErrorIs(t, err, errRequestCancelled)
	case <-time.After(time.Second):
		t.Fatal("cancelled request was registered without being failed")
	}

	c.pendingLock.Lock()
	_, pending := c.pendingReqs[requestID]
	_, staleCancellation := c.cancelledReqs[requestID]
	c.pendingLock.Unlock()
	assert.False(t, pending, "cancelled request must not be registered")
	assert.False(t, staleCancellation, "applied cancellation must be consumed")
}

func TestFailPendingRequestsDropsRememberedCancellations(t *testing.T) {
	c := newTestConnection()

	c.CloseRequest(42, errRequestCancelled)

	c.failPendingRequests(ErrConnectionClosed)

	c.pendingLock.Lock()
	remembered := len(c.cancelledReqs)
	c.pendingLock.Unlock()
	assert.Zero(t, remembered)
}

func TestCloseRequestConcurrentWithResponse(t *testing.T) {
	const numTrials = 100

	for trial := 0; trial < numTrials; trial++ {
		c := newTestConnection()
		requestID := uint64(100 + trial)

		var completions atomic.Int32
		completed := make(chan error, 2)
		callback := func(_ *pb.BaseCommand, err error) {
			completions.Add(1)
			completed <- err
		}
		registerPendingRequest(c, requestID, callback)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			c.CloseRequest(requestID, errRequestCancelled)
		}()
		go func() {
			defer wg.Done()
			if req, ok := c.deletePendingRequest(requestID); ok {
				req.callback(&pb.BaseCommand{}, nil)
			}
		}()
		wg.Wait()

		c.pendingLock.Lock()
		remembered := len(c.cancelledReqs)
		c.pendingLock.Unlock()

		assert.Equal(t, int32(1), completions.Load(),
			"request must complete exactly once (trial %d)", trial)
		assert.Zero(t, remembered, "trial %d", trial)
		select {
		case err := <-completed:
			if err != nil {
				assert.ErrorIs(t, err, errRequestCancelled)
			}
		default:
			t.Fatalf("request never completed (trial %d)", trial)
		}
	}
}
