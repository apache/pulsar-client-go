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
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/auth"
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

func testConnectionOptions(t *testing.T, physicalAddr string) connectionOptions {
	t.Helper()

	addr, err := url.Parse(physicalAddr)
	assert.NoError(t, err)

	return connectionOptions{
		logicalAddr:  addr,
		physicalAddr: addr,
		auth:         auth.NewAuthDisabled(),
		logger:       log.DefaultNopLogger(),
		metrics:      newMockMetrics(),
	}
}

// listen starts a TCP listener that accepts and immediately closes connections,
// so connect() can complete a plaintext dial without a broker.
func listen(t *testing.T) net.Listener {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	return l
}

func TestConnectionDialerIsUsed(t *testing.T) {
	l := listen(t)

	var (
		gotNetwork string
		gotAddr    string
		gotSet     bool
	)

	opts := testConnectionOptions(t, "pulsar://"+l.Addr().String())
	opts.connectionTimeout = 10 * time.Second
	opts.dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		gotNetwork, gotAddr, gotSet = network, addr, true

		deadline, ok := ctx.Deadline()
		assert.True(t, ok, "dialer context should carry the connection timeout")
		assert.WithinDuration(t, time.Now().Add(10*time.Second), deadline, time.Second)

		return net.Dial(network, addr)
	}

	cnx := newConnection(opts)
	assert.True(t, cnx.connect())
	cnx.Close()

	assert.True(t, gotSet, "dialer should have been called")
	assert.Equal(t, "tcp", gotNetwork)
	assert.Equal(t, l.Addr().String(), gotAddr)
}

func TestConnectionDialerNoTimeoutHasNoDeadline(t *testing.T) {
	l := listen(t)

	opts := testConnectionOptions(t, "pulsar://"+l.Addr().String())
	opts.dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		_, ok := ctx.Deadline()
		assert.False(t, ok, "no ConnectionTimeout should mean no deadline")
		return net.Dial(network, addr)
	}

	cnx := newConnection(opts)
	assert.True(t, cnx.connect())
	cnx.Close()
}

func TestConnectionDialerError(t *testing.T) {
	l := listen(t)

	opts := testConnectionOptions(t, "pulsar://"+l.Addr().String())
	opts.dialer = func(_ context.Context, _, _ string) (net.Conn, error) {
		return nil, errors.New("dial rejected")
	}

	cnx := newConnection(opts)
	assert.False(t, cnx.connect(), "a dialer error should fail the connection")
}

// The dialer returns a plain net.Conn and the library performs the TLS
// handshake itself, so a custom dialer must not bypass certificate
// verification.
func TestConnectionDialerWithTLS(t *testing.T) {
	cert, err := tls.LoadX509KeyPair("../../integration-tests/certs/broker-cert.pem",
		"../../integration-tests/certs/broker-key.pem")
	assert.NoError(t, err)

	l, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	assert.NoError(t, err)
	defer l.Close()

	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c.(*tls.Conn).Handshake()
			_ = c.Close()
		}
	}()

	dialed := false
	opts := testConnectionOptions(t, "pulsar+ssl://"+l.Addr().String())
	opts.tls = &TLSOptions{TrustCertsFilePath: "../../integration-tests/certs/cacert.pem"}
	opts.dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialed = true
		return net.Dial(network, addr)
	}

	cnx := newConnection(opts)
	// The broker certificate is not valid for 127.0.0.1, so the handshake the
	// library performs on the dialer's conn must reject it.
	assert.False(t, cnx.connect())
	assert.True(t, dialed, "dialer should be used for TLS connections too")

	// ...and it succeeds once the name matches.
	opts.tls.ServerName = "localhost"
	opts.tls.ValidateHostname = true
	cnx = newConnection(opts)
	assert.True(t, cnx.connect())
	cnx.Close()
}

// With ValidateHostname off, getTLSConfig() leaves ServerName empty.
// tls.DialWithDialer used to infer it from the dialed address; tls.Client does
// not, so connect() fills it in and verification still works.
func TestConnectionTLSServerNameInferredFromAddress(t *testing.T) {
	cert, err := tls.LoadX509KeyPair("../../integration-tests/certs/broker-cert.pem",
		"../../integration-tests/certs/broker-key.pem")
	assert.NoError(t, err)

	l, err := tls.Listen("tcp", "localhost:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	assert.NoError(t, err)
	defer l.Close()

	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c.(*tls.Conn).Handshake()
			_ = c.Close()
		}
	}()

	_, port, err := net.SplitHostPort(l.Addr().String())
	assert.NoError(t, err)

	opts := testConnectionOptions(t, "pulsar+ssl://localhost:"+port)
	opts.tls = &TLSOptions{TrustCertsFilePath: "../../integration-tests/certs/cacert.pem"}

	cnx := newConnection(opts)
	assert.True(t, cnx.connect())
	cnx.Close()
}

// A peer that completes the TCP accept then never speaks TLS.
func TestConnectionTLSHandshakeBlackhole(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer l.Close()

	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			accepted <- c // hold it open, send nothing
		}
	}()

	_, port, _ := net.SplitHostPort(l.Addr().String())
	opts := testConnectionOptions(t, "pulsar+ssl://localhost:"+port)
	opts.connectionTimeout = 2 * time.Second
	opts.tls = &TLSOptions{TrustCertsFilePath: "../../integration-tests/certs/cacert.pem"}

	cnx := newConnection(opts)

	done := make(chan bool, 1)
	start := time.Now()
	go func() { done <- cnx.connect() }()

	select {
	case ok := <-done:
		assert.False(t, ok)
		t.Logf("connect() returned after %v", time.Since(start))
	case <-time.After(10 * time.Second):
		t.Fatalf("connect() HUNG: still blocked after 10s with ConnectionTimeout=2s")
	}
}

// After connect() returns, the connection must remain usable indefinitely:
// the connect deadline must not linger on the socket.
func TestConnectionNoLingeringDeadlineAfterHandshake(t *testing.T) {
	cert, err := tls.LoadX509KeyPair("../../integration-tests/certs/broker-cert.pem",
		"../../integration-tests/certs/broker-key.pem")
	assert.NoError(t, err)

	l, err := tls.Listen("tcp", "localhost:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	assert.NoError(t, err)
	defer l.Close()

	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		c, err := l.Accept()
		if err != nil {
			return
		}
		_ = c.(*tls.Conn).Handshake()
		// Stay silent past the connect timeout, then send a byte.
		time.Sleep(1500 * time.Millisecond)
		_, _ = c.Write([]byte{0x42})
		time.Sleep(2 * time.Second)
		_ = c.Close()
	}()

	_, port, _ := net.SplitHostPort(l.Addr().String())
	opts := testConnectionOptions(t, "pulsar+ssl://localhost:"+port)
	opts.connectionTimeout = 500 * time.Millisecond
	opts.tls = &TLSOptions{TrustCertsFilePath: "../../integration-tests/certs/cacert.pem"}

	cnx := newConnection(opts)
	assert.True(t, cnx.connect())

	// Read well after the 500ms connect timeout would have elapsed.
	buf := make([]byte, 1)
	n, err := cnx.cnx.Read(buf)
	assert.NoError(t, err, "read after connect timeout elapsed must not fail with i/o timeout")
	assert.Equal(t, 1, n)
	assert.Equal(t, byte(0x42), buf[0])

	cnx.Close()
	<-srvDone
}
