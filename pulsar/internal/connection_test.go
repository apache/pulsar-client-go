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
	"io"
	"net"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestConnection_WriteData_recyclesBufferOnContextCanceled(t *testing.T) {
	pool := &capturingPool{}
	c := makeTestConnection()
	c.writeRequestsCh = make(chan *dataRequest)
	c.bufferPool = pool

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c.WriteData(ctx, NewSharedBuffer(NewBuffer(1024)))
	assert.NotNil(t, pool.Get())
}

func TestConnection_WriteData_recyclesBufferOnConnectionClosed(t *testing.T) {
	pool := &capturingPool{}
	c := makeTestConnection()
	c.writeRequestsCh = make(chan *dataRequest)
	c.state.Store(connectionClosed)
	c.bufferPool = pool

	c.WriteData(context.Background(), NewSharedBuffer(NewBuffer(1024)))
	assert.NotNil(t, pool.Get())
}

func TestConnection_WriteData_doNotRecyclesBufferWhenWritten(t *testing.T) {
	pool := &capturingPool{}
	c := makeTestConnection()
	c.bufferPool = pool

	c.WriteData(context.Background(), NewSharedBuffer(NewBuffer(1024)))
	assert.Nil(t, pool.Get())
}

func TestConnection_run_recyclesBufferOnceDone(t *testing.T) {
	pool := &capturingPool{}
	c := makeTestConnection()
	c.bufferPool = pool

	c.writeRequestsCh <- &dataRequest{ctx: context.Background(), data: NewSharedBuffer(NewBuffer(1024))}
	close(c.writeRequestsCh)

	c.run()
	assert.NotNil(t, pool.Get())
}

func TestConnection_run_recyclesBufferEvenOnContextCanceled(t *testing.T) {
	pool := &capturingPool{}
	c := makeTestConnection()
	c.bufferPool = pool

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c.writeRequestsCh <- &dataRequest{ctx: ctx, data: NewSharedBuffer(NewBuffer(1024))}
	close(c.writeRequestsCh)

	c.run()
	assert.NotNil(t, pool.Get())
}

// --- Helpers

func makeTestConnection() *connection {
	c := &connection{
		log:               log.DefaultNopLogger(),
		keepAliveInterval: 30 * time.Second,
		writeRequestsCh:   make(chan *dataRequest, 10),
		closeCh:           make(chan struct{}),
		cnx:               happyCnx{},
		metrics:           NewMetricsProvider(0, make(map[string]string), prometheus.DefaultRegisterer),
		bufferPool:        GetDefaultBufferPool(),
	}
	c.reader = newConnectionReader(c)
	c.state.Store(connectionReady)
	return c
}

type happyCnx struct{}

func (happyCnx) Read(_ []byte) (n int, err error) {
	return 0, io.EOF
}

func (happyCnx) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (happyCnx) Close() error {
	return nil
}

func (happyCnx) LocalAddr() net.Addr {
	return &net.IPAddr{IP: net.ParseIP("127.0.0.1")}
}

func (happyCnx) RemoteAddr() net.Addr {
	return &net.IPAddr{IP: net.ParseIP("127.0.0.1")}
}

func (happyCnx) SetDeadline(_ time.Time) error {
	return nil
}

func (happyCnx) SetReadDeadline(_ time.Time) error {
	return nil
}

func (happyCnx) SetWriteDeadline(_ time.Time) error {
	return nil
}
