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
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionRejectRequestsAfterClose(t *testing.T) {
	c := newTestConnection()

	c.Close()

	assertConnectionClosed(t, c)
}

func TestConnectionSendRequestRaceWithClose(t *testing.T) {
	// Regression test for concurrent Add/Wait on WaitGroup during Close.
	//
	// Without proper synchronization in registerIncomingRequest(), calling
	// WaitGroup.Add(1) and checking state under c.mu.RLock(), a concurrent
	// failLeftRequestsWhenClose() calling WaitGroup.Wait() could race with Add()
	// in Go 1.25+, causing panic: "sync: WaitGroup is reused before previous Wait has returned"
	//
	// This test directly exercises the synchronization:
	// 1. Many goroutines call registerIncomingRequest() to Add() to the WaitGroup
	// 2. While they are still running, failLeftRequestsWhenClose() calls Wait()
	// 3. The connection transitions to closed so new registrations are rejected
	//    and existing ones drain, letting Wait() return
	// 4. The test verifies no panic occurs during the Add/Wait overlap

	const (
		numTrials     = 10
		numGoroutines = 50
	)

	for trial := 0; trial < numTrials; trial++ {
		c := newTestConnection()

		startCh := make(chan struct{})
		stopCh := make(chan struct{})
		panicCh := make(chan any, 1)

		var wg sync.WaitGroup
		var registerCount int32

		// Producer goroutines that register requests
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						panicCh <- r
					}
				}()

				<-startCh

				for {
					select {
					case <-stopCh:
						return
					default:
					}

					// Call registerIncomingRequest() directly to exercise the WaitGroup Add/state check
					if c.registerIncomingRequest() {
						atomic.AddInt32(&registerCount, 1)
						c.incomingRequestsWG.Done()
					}
				}
			}()
		}

		// Start producers
		close(startCh)

		// Let producers run and accumulate pending adds
		time.Sleep(20 * time.Millisecond)

		// Transition the connection to closed — this runs under the write lock,
		// matching the real Close() flow. After this, registerIncomingRequest()
		// will reject new Add() calls, but goroutines already past the state
		// check and holding RLock will still complete their Add()/Done().
		c.mu.Lock()
		c.setStateClosed()
		c.mu.Unlock()

		// Immediately start failLeftRequestsWhenClose() in a goroutine — it
		// calls Wait(). With the fix, goroutines that already called Add()
		// under RLock will finish their Done(), and no new Add() can happen
		// because setStateClosed() above drained pending RLock holders. Without
		// the fix, a goroutine slipping through could call Add() after Wait()
		// returns, causing "WaitGroup is reused before previous Wait has returned".
		drainDone := make(chan struct{})
		go func() {
			defer func() {
				if r := recover(); r != nil {
					panicCh <- r
				}
			}()
			c.failLeftRequestsWhenClose()
			close(drainDone)
		}()

		// Signal producers to stop
		close(stopCh)

		// Wait for drain to complete
		select {
		case <-drainDone:
		case <-time.After(5 * time.Second):
			t.Fatal("failLeftRequestsWhenClose() did not complete (deadlock in WaitGroup)")
		}

		// Wait for all producers to finish (they should already be done)
		wg.Wait()

		// Check for panic
		select {
		case p := <-panicCh:
			t.Fatalf("trial %d: panic during WaitGroup race: %v", trial, p)
		default:
		}

		t.Logf("trial %d: %d successful registers", trial, atomic.LoadInt32(&registerCount))
	}
}

func assertConnectionClosed(t *testing.T, c *connection) {
	t.Helper()

	callbackCh := make(chan error, 1)

	c.SendRequest(
		999,
		&pb.BaseCommand{},
		func(_ *pb.BaseCommand, err error) {
			callbackCh <- err
		},
	)

	select {
	case err := <-callbackCh:
		assert.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("SendRequest callback was not invoked")
	}

	assert.Error(t, c.SendRequestNoWait(&pb.BaseCommand{}))

	released := make(chan struct{}, 1)

	buf := NewBufferPool().GetBuffer(8)
	buf.SetReleaseCallback(func() {
		released <- struct{}{}
	})

	c.WriteData(context.Background(), buf)

	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("WriteData buffer was not released")
	}
}

func newTestConnection() *connection {
	opts := connectionOptions{
		logicalAddr:       &url.URL{Host: "test:6650"},
		physicalAddr:      &url.URL{Host: "test:6650"},
		connectionTimeout: time.Second,
		keepAliveInterval: 30 * time.Second,
		logger:            log.DefaultNopLogger(),
		metrics:           newMockMetrics(),
	}

	c := newConnection(opts)

	require.NotNil(&testing.T{}, c)

	return c
}

// newMockMetrics creates Metrics with real prometheus counters for testing.
func newMockMetrics() *Metrics {
	return &Metrics{
		ConnectionsClosed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_connections_closed",
		}),
		ConnectionsEstablishmentErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_connections_establishment_errors",
		}),
		ConnectionsHandshakeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_connections_handshake_errors",
		}),
	}
}
