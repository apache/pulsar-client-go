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
	// Without proper synchronization between:
	//   - registerIncomingRequest() calling WaitGroup.Add(1)
	//   - Close() calling WaitGroup.Wait()
	//
	// Go 1.25+ may panic with:
	//
	//   sync: WaitGroup is reused before previous Wait has returned
	//
	// This test continuously issues requests while concurrently closing
	// the connection to maximize the Add/Wait overlap window.

	const (
		numTrials     = 20
		numGoroutines = 100
	)

	for trial := 0; trial < numTrials; trial++ {
		c := newTestConnection()

		startCh := make(chan struct{})
		stopCh := make(chan struct{})

		panicCh := make(chan any, numGoroutines)

		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)

			go func(idx int) {
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

					if idx%2 == 0 {
						c.SendRequest(
							uint64(idx),
							&pb.BaseCommand{},
							func(*pb.BaseCommand, error) {},
						)
					} else {
						_ = c.SendRequestNoWait(&pb.BaseCommand{})
					}
				}
			}(i)
		}

		// Start all concurrent request producers together.
		close(startCh)

		// Allow requests to race with Close().
		time.Sleep(10 * time.Millisecond)

		c.Close()

		close(stopCh)

		wg.Wait()

		select {
		case p := <-panicCh:
			t.Fatalf("unexpected panic during concurrent Close: %v", p)
		default:
		}

		assertConnectionClosed(t, c)
	}
}

func assertConnectionClosed(t *testing.T, c *connection) {
	t.Helper()

	callbackCh := make(chan error, 1)

	c.SendRequest(
		999,
		&pb.BaseCommand{},
		func(cmd *pb.BaseCommand, err error) {
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
