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
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/internal/crypto"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestSingleMessageIDNoAckTracker(t *testing.T) {
	eventsCh := make(chan interface{}, 1)
	pc := partitionConsumer{
		queueCh:              make(chan []*message, 1),
		eventsCh:             eventsCh,
		compressionProviders: sync.Map{},
		options:              &partitionConsumerOpts{},
		metrics:              newTestMetrics(),
		decryptor:            crypto.NewNoopDecryptor(),
	}
	pc._setConn(dummyConnection{})
	pc.availablePermits = &availablePermits{pc: &pc}
	pc.ackGroupingTracker = newAckGroupingTracker(&AckGroupingOptions{MaxSize: 0},
		func(id MessageID) { pc.sendIndividualAck(id) }, nil, nil)

	headersAndPayload := internal.NewBufferWrapper(rawCompatSingleMessage)
	if err := pc.MessageReceived(nil, headersAndPayload); err != nil {
		t.Fatal(err)
	}

	// ensure the tracker was set on the message id
	messages := <-pc.queueCh
	for _, m := range messages {
		assert.Nil(t, m.ID().(*trackingMessageID).tracker)
	}

	// ack the message id
	pc.AckID(messages[0].msgID.(*trackingMessageID))

	select {
	case <-eventsCh:
	default:
		t.Error("Expected an ack request to be triggered!")
	}
}

func newTestMetrics() *internal.LeveledMetrics {
	return internal.NewMetricsProvider(4, map[string]string{}, prometheus.DefaultRegisterer).GetLeveledMetrics("topic")
}

func TestBatchMessageIDNoAckTracker(t *testing.T) {
	eventsCh := make(chan interface{}, 1)
	pc := partitionConsumer{
		queueCh:              make(chan []*message, 1),
		eventsCh:             eventsCh,
		compressionProviders: sync.Map{},
		options:              &partitionConsumerOpts{},
		metrics:              newTestMetrics(),
		decryptor:            crypto.NewNoopDecryptor(),
	}
	pc._setConn(dummyConnection{})
	pc.availablePermits = &availablePermits{pc: &pc}
	pc.ackGroupingTracker = newAckGroupingTracker(&AckGroupingOptions{MaxSize: 0},
		func(id MessageID) { pc.sendIndividualAck(id) }, nil, nil)

	headersAndPayload := internal.NewBufferWrapper(rawBatchMessage1)
	if err := pc.MessageReceived(nil, headersAndPayload); err != nil {
		t.Fatal(err)
	}

	// ensure the tracker was set on the message id
	messages := <-pc.queueCh
	for _, m := range messages {
		assert.Nil(t, m.ID().(*trackingMessageID).tracker)
	}

	// ack the message id
	err := pc.AckID(messages[0].msgID.(*trackingMessageID))
	assert.Nil(t, err)

	select {
	case <-eventsCh:
	default:
		t.Error("Expected an ack request to be triggered!")
	}
}

func TestBatchMessageIDWithAckTracker(t *testing.T) {
	eventsCh := make(chan interface{}, 1)
	pc := partitionConsumer{
		queueCh:              make(chan []*message, 10),
		eventsCh:             eventsCh,
		compressionProviders: sync.Map{},
		options:              &partitionConsumerOpts{},
		metrics:              newTestMetrics(),
		decryptor:            crypto.NewNoopDecryptor(),
	}
	pc._setConn(dummyConnection{})
	pc.availablePermits = &availablePermits{pc: &pc}
	pc.ackGroupingTracker = newAckGroupingTracker(&AckGroupingOptions{MaxSize: 0},
		func(id MessageID) { pc.sendIndividualAck(id) }, nil, nil)

	headersAndPayload := internal.NewBufferWrapper(rawBatchMessage10)
	if err := pc.MessageReceived(nil, headersAndPayload); err != nil {
		t.Fatal(err)
	}

	// ensure the tracker was set on the message id
	messages := <-pc.queueCh
	for _, m := range messages {
		assert.NotNil(t, m.ID().(*trackingMessageID).tracker)
	}

	// ack all message ids except the last one
	for i := 0; i < 9; i++ {
		err := pc.AckID(messages[i].msgID.(*trackingMessageID))
		assert.Nil(t, err)
	}

	select {
	case <-eventsCh:
		t.Error("The message id should not be acked!")
	default:
	}

	// ack last message
	err := pc.AckID(messages[9].msgID.(*trackingMessageID))
	assert.Nil(t, err)

	select {
	case <-eventsCh:
	default:
		t.Error("Expected an ack request to be triggered!")
	}
}

// Raw single message in old format
// metadata properties:<key:"a" value:"1" > properties:<key:"b" value:"2" >
// payload = "hello"
var rawCompatSingleMessage = []byte{
	0x0e, 0x01, 0x08, 0x36, 0xb4, 0x66, 0x00, 0x00,
	0x00, 0x31, 0x0a, 0x0f, 0x73, 0x74, 0x61, 0x6e,
	0x64, 0x61, 0x6c, 0x6f, 0x6e, 0x65, 0x2d, 0x37,
	0x34, 0x2d, 0x30, 0x10, 0x00, 0x18, 0xac, 0xef,
	0xe8, 0xa0, 0xe2, 0x2d, 0x22, 0x06, 0x0a, 0x01,
	0x61, 0x12, 0x01, 0x31, 0x22, 0x06, 0x0a, 0x01,
	0x62, 0x12, 0x01, 0x32, 0x48, 0x05, 0x60, 0x05,
	0x82, 0x01, 0x00, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
}

// Message with batch of 1
// singe message metadata properties:<key:"a" value:"1" > properties:<key:"b" value:"2" >
// payload = "hello"
var rawBatchMessage1 = []byte{
	0x0e, 0x01, 0x1f, 0x80, 0x09, 0x68, 0x00, 0x00,
	0x00, 0x1f, 0x0a, 0x0f, 0x73, 0x74, 0x61, 0x6e,
	0x64, 0x61, 0x6c, 0x6f, 0x6e, 0x65, 0x2d, 0x37,
	0x34, 0x2d, 0x31, 0x10, 0x00, 0x18, 0xdb, 0x80,
	0xf4, 0xa0, 0xe2, 0x2d, 0x58, 0x01, 0x82, 0x01,
	0x00, 0x00, 0x00, 0x00, 0x16, 0x0a, 0x06, 0x0a,
	0x01, 0x61, 0x12, 0x01, 0x31, 0x0a, 0x06, 0x0a,
	0x01, 0x62, 0x12, 0x01, 0x32, 0x18, 0x05, 0x28,
	0x05, 0x40, 0x00, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
}

var rawBatchMessage10 = []byte{
	0x0e, 0x01, 0x7b, 0x28, 0x8c, 0x08,
	0x00, 0x00, 0x00, 0x1f, 0x0a, 0x0f, 0x73, 0x74,
	0x61, 0x6e, 0x64, 0x61, 0x6c, 0x6f, 0x6e, 0x65,
	0x2d, 0x37, 0x34, 0x2d, 0x32, 0x10, 0x00, 0x18,
	0xd0, 0xc2, 0xfa, 0xa0, 0xe2, 0x2d, 0x58, 0x0a,
	0x82, 0x01, 0x00, 0x00, 0x00, 0x00, 0x16, 0x0a,
	0x06, 0x0a, 0x01, 0x61, 0x12, 0x01, 0x31, 0x0a,
	0x06, 0x0a, 0x01, 0x62, 0x12, 0x01, 0x32, 0x18,
	0x05, 0x28, 0x05, 0x40, 0x00, 0x68, 0x65, 0x6c,
	0x6c, 0x6f, 0x00, 0x00, 0x00, 0x16, 0x0a, 0x06,
	0x0a, 0x01, 0x61, 0x12, 0x01, 0x31, 0x0a, 0x06,
	0x0a, 0x01, 0x62, 0x12, 0x01, 0x32, 0x18, 0x05,
	0x28, 0x05, 0x40, 0x01, 0x68, 0x65, 0x6c, 0x6c,
	0x6f, 0x00, 0x00, 0x00, 0x16, 0x0a, 0x06, 0x0a,
	0x01, 0x61, 0x12, 0x01, 0x31, 0x0a, 0x06, 0x0a,
	0x01, 0x62, 0x12, 0x01, 0x32, 0x18, 0x05, 0x28,
	0x05, 0x40, 0x02, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
	0x00, 0x00, 0x00, 0x16, 0x0a, 0x06, 0x0a, 0x01,
	0x61, 0x12, 0x01, 0x31, 0x0a, 0x06, 0x0a, 0x01,
	0x62, 0x12, 0x01, 0x32, 0x18, 0x05, 0x28, 0x05,
	0x40, 0x03, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00,
	0x00, 0x00, 0x16, 0x0a, 0x06, 0x0a, 0x01, 0x61,
	0x12, 0x01, 0x31, 0x0a, 0x06, 0x0a, 0x01, 0x62,
	0x12, 0x01, 0x32, 0x18, 0x05, 0x28, 0x05, 0x40,
	0x04, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00,
	0x00, 0x16, 0x0a, 0x06, 0x0a, 0x01, 0x61, 0x12,
	0x01, 0x31, 0x0a, 0x06, 0x0a, 0x01, 0x62, 0x12,
	0x01, 0x32, 0x18, 0x05, 0x28, 0x05, 0x40, 0x05,
	0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00,
	0x16, 0x0a, 0x06, 0x0a, 0x01, 0x61, 0x12, 0x01,
	0x31, 0x0a, 0x06, 0x0a, 0x01, 0x62, 0x12, 0x01,
	0x32, 0x18, 0x05, 0x28, 0x05, 0x40, 0x06, 0x68,
	0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x16,
	0x0a, 0x06, 0x0a, 0x01, 0x61, 0x12, 0x01, 0x31,
	0x0a, 0x06, 0x0a, 0x01, 0x62, 0x12, 0x01, 0x32,
	0x18, 0x05, 0x28, 0x05, 0x40, 0x07, 0x68, 0x65,
	0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x16, 0x0a,
	0x06, 0x0a, 0x01, 0x61, 0x12, 0x01, 0x31, 0x0a,
	0x06, 0x0a, 0x01, 0x62, 0x12, 0x01, 0x32, 0x18,
	0x05, 0x28, 0x05, 0x40, 0x08, 0x68, 0x65, 0x6c,
	0x6c, 0x6f, 0x00, 0x00, 0x00, 0x16, 0x0a, 0x06,
	0x0a, 0x01, 0x61, 0x12, 0x01, 0x31, 0x0a, 0x06,
	0x0a, 0x01, 0x62, 0x12, 0x01, 0x32, 0x18, 0x05,
	0x28, 0x05, 0x40, 0x09, 0x68, 0x65, 0x6c, 0x6c,
	0x6f,
}

// TestMessageReceivedAllMessagesDiscarded verifies that when all messages in a batch
// are discarded by messageShouldBeDiscarded (because startMessageID is greater than
// all message IDs), no empty slice is sent to queueCh and no panic occurs.
// This is the regression test for https://github.com/apache/pulsar-client-go/issues/1454
func TestMessageReceivedAllMessagesDiscarded(t *testing.T) {
	pc := partitionConsumer{
		queueCh:              make(chan []*message, 1),
		eventsCh:             make(chan interface{}, 1),
		compressionProviders: sync.Map{},
		maxQueueSize:         1000,
		options:              &partitionConsumerOpts{},
		metrics:              newTestMetrics(),
		decryptor:            crypto.NewNoopDecryptor(),
		log:                  log.DefaultNopLogger(),
	}
	pc._setConn(dummyConnection{})
	pc.availablePermits = &availablePermits{pc: &pc}
	pc.ackGroupingTracker = newAckGroupingTracker(&AckGroupingOptions{MaxSize: 0},
		func(id MessageID) { pc.sendIndividualAck(id) }, nil, nil)

	// Set startMessageID to a value greater than the message IDs in the batch.
	// The raw messages use ledgerID=0, entryID=0 (from nil response), so setting
	// startMessageID with ledgerID=100 ensures all messages are discarded.
	pc.startMessageID.set(newTrackingMessageID(100, 0, 0, 0, 0, nil))

	// Use a batch of 10 messages; all should be discarded
	headersAndPayload := internal.NewBufferWrapper(rawBatchMessage10)
	err := pc.MessageReceived(nil, headersAndPayload)
	assert.Nil(t, err)

	// queueCh should be empty since all messages were discarded
	select {
	case msgs := <-pc.queueCh:
		t.Fatalf("expected no messages on queueCh, but got %d", len(msgs))
	default:
	}
}

// TestMessageReceivedSingleMessageDiscarded verifies the same behavior for a
// single (non-batch) message that gets discarded.
func TestMessageReceivedSingleMessageDiscarded(t *testing.T) {
	pc := partitionConsumer{
		queueCh:              make(chan []*message, 1),
		eventsCh:             make(chan interface{}, 1),
		compressionProviders: sync.Map{},
		maxQueueSize:         1000,
		options:              &partitionConsumerOpts{},
		metrics:              newTestMetrics(),
		decryptor:            crypto.NewNoopDecryptor(),
		log:                  log.DefaultNopLogger(),
	}
	pc._setConn(dummyConnection{})
	pc.availablePermits = &availablePermits{pc: &pc}
	pc.ackGroupingTracker = newAckGroupingTracker(&AckGroupingOptions{MaxSize: 0},
		func(id MessageID) { pc.sendIndividualAck(id) }, nil, nil)

	pc.startMessageID.set(newTrackingMessageID(100, 0, 0, 0, 0, nil))

	headersAndPayload := internal.NewBufferWrapper(rawBatchMessage1)
	err := pc.MessageReceived(nil, headersAndPayload)
	assert.Nil(t, err)

	select {
	case msgs := <-pc.queueCh:
		t.Fatalf("expected no messages on queueCh, but got %d", len(msgs))
	default:
	}
}

// TestMessageReceivedAllMessagesDuplicate verifies that when all messages in a batch
// are detected as duplicates by ackGroupingTracker, no empty slice is sent to queueCh.
func TestMessageReceivedAllMessagesDuplicate(t *testing.T) {
	pc := partitionConsumer{
		queueCh:              make(chan []*message, 1),
		eventsCh:             make(chan interface{}, 1),
		compressionProviders: sync.Map{},
		maxQueueSize:         1000,
		options:              &partitionConsumerOpts{},
		metrics:              newTestMetrics(),
		decryptor:            crypto.NewNoopDecryptor(),
		log:                  log.DefaultNopLogger(),
	}
	pc._setConn(dummyConnection{})
	pc.availablePermits = &availablePermits{pc: &pc}
	// Use a timedAckGroupingTracker (MaxSize > 1) so that isDuplicate tracks pending acks
	pc.ackGroupingTracker = newAckGroupingTracker(&AckGroupingOptions{
		MaxSize: 1000,
		MaxTime: 1 * time.Hour,
	}, func(_ MessageID) {}, nil, nil)

	// First, receive the batch normally to populate the queue
	headersAndPayload := internal.NewBufferWrapper(rawBatchMessage10)
	err := pc.MessageReceived(nil, headersAndPayload)
	assert.Nil(t, err)

	messages := <-pc.queueCh
	assert.Equal(t, 10, len(messages))

	// Ack all messages so they are recorded in the ack tracker as pending/duplicate
	for _, m := range messages {
		pc.ackGroupingTracker.add(m.msgID)
	}

	// Send the same batch again; all messages should be detected as duplicates
	headersAndPayload = internal.NewBufferWrapper(rawBatchMessage10)
	err = pc.MessageReceived(nil, headersAndPayload)
	assert.Nil(t, err)

	// queueCh should be empty since all messages were duplicates
	select {
	case msgs := <-pc.queueCh:
		t.Fatalf("expected no messages on queueCh, but got %d", len(msgs))
	default:
	}
}

// TestGrabConn_HandlerRegisteredBeforeSubscribe verifies that the consumer
// handler is registered on the connection BEFORE the subscribe RPC is sent.
//
// Without this ordering, the broker can send MESSAGE and ACTIVE_CONSUMER_CHANGE
// frames immediately after the subscribe succeeds, but the client's read
// goroutine cannot route them because the handler isn't in the map yet.
// Those frames are silently dropped.
func TestGrabConn_HandlerRegisteredBeforeSubscribe(t *testing.T) {
	cnx := newSpyConnection()
	rpc := &grabConnSpyRPCClient{cnx: cnx}
	pc := newGrabConnTestConsumer(cnx, rpc)

	err := pc.grabConn("")
	assert.NoError(t, err)

	// Drain the connectedCh goroutine spawned on success to assert it fires
	// and avoid relying solely on the channel's buffer capacity.
	<-pc.connectedCh

	assert.True(t, rpc.handlerRegisteredDuringRPC.Load(),
		"AddConsumeHandler must be called before the subscribe RPC is sent")
}

// TestGrabConn_HandlerRemovedOnSubscribeFailure verifies that when the
// subscribe RPC fails, the pre-registered consumer handler is removed from
// the connection so it does not leak.
func TestGrabConn_HandlerRemovedOnSubscribeFailure(t *testing.T) {
	cnx := newSpyConnection()
	rpc := &grabConnSpyRPCClient{
		cnx:          cnx,
		subscribeErr: fmt.Errorf("broker rejected subscribe"),
	}
	pc := newGrabConnTestConsumer(cnx, rpc)

	err := pc.grabConn("")
	assert.Error(t, err)

	assert.True(t, cnx.handlerRemoved.Load(),
		"DeleteConsumeHandler must be called when subscribe fails")
}

// TestGrabConn_HandlerRemovedOnSubscribeTimeout verifies cleanup on timeout
// and that the close command is sent on the same connection (not a potentially
// different one from the pool).
func TestGrabConn_HandlerRemovedOnSubscribeTimeout(t *testing.T) {
	cnx := newSpyConnection()
	rpc := &grabConnSpyRPCClient{
		cnx:          cnx,
		subscribeErr: internal.ErrRequestTimeOut,
	}
	pc := newGrabConnTestConsumer(cnx, rpc)

	err := pc.grabConn("")
	assert.ErrorIs(t, err, internal.ErrRequestTimeOut)

	assert.True(t, cnx.handlerRemoved.Load(),
		"DeleteConsumeHandler must be called on timeout")
	assert.True(t, rpc.closeSentOnCnx.Load(),
		"CloseConsumer must be sent via RequestOnCnx on the same connection")
}

// TestGrabConn_ConnResetOnFirstCallFailure verifies that when grabConn fails
// on the very first call (no prior connection), pc.conn is reset to nil rather
// than left pointing at the stale connection on which subscribe failed.
func TestGrabConn_ConnResetOnFirstCallFailure(t *testing.T) {
	cnx := newSpyConnection()
	rpc := &grabConnSpyRPCClient{
		cnx:          cnx,
		subscribeErr: fmt.Errorf("broker rejected subscribe"),
	}
	pc := newGrabConnTestConsumerNoConn(cnx, rpc)

	err := pc.grabConn("")
	assert.Error(t, err)

	assert.Nil(t, pc.conn.Load(),
		"pc.conn must be reset to nil on first-call failure, not left pointing at the stale connection")
}

// TestGrabConn_BrokerFrameDuringSubscribe simulates the exact race: the broker
// sends a frame (e.g. ActiveConsumerChange) while the subscribe RPC is still
// in flight. Because the handler is registered before the RPC, the frame
// must be delivered to the consumer — not dropped.
func TestGrabConn_BrokerFrameDuringSubscribe(t *testing.T) {
	cnx := newSpyConnection()
	var consumerReceivedChange atomic.Bool

	rpc := &grabConnSpyRPCClient{
		cnx: cnx,
		duringSubscribe: func() {
			// Simulate the broker's read goroutine delivering a frame
			// while the subscribe RPC is in flight.
			if handler, ok := cnx.handler.Load().(*partitionConsumer); ok && handler != nil {
				handler.ActiveConsumerChanged(true)
				consumerReceivedChange.Store(true)
			}
		},
	}
	pc := newGrabConnTestConsumer(cnx, rpc)

	err := pc.grabConn("")
	assert.NoError(t, err)

	// Drain the connectedCh goroutine spawned on success.
	<-pc.connectedCh

	assert.True(t, consumerReceivedChange.Load(),
		"Frames sent by the broker during the subscribe RPC must reach the consumer handler")
}

// TestGrabConn_MessageReceivedDuringSubscribe_NilConn verifies that when the
// broker delivers a MESSAGE frame while the subscribe RPC is in flight, the
// consumer does not panic due to a nil connection. This is a regression test:
// registering the handler before calling _setConn means MessageReceived ->
// discardCorruptedMessage -> _getConn() dereferences nil.
func TestGrabConn_MessageReceivedDuringSubscribe_NilConn(t *testing.T) {
	cnx := newSpyConnection()

	rpc := &grabConnSpyRPCClient{
		cnx: cnx,
		duringSubscribe: func() {
			handler, ok := cnx.handler.Load().(*partitionConsumer)
			if !ok || handler == nil {
				return
			}
			// Deliver an empty (invalid) message frame. ReadBrokerMetadata
			// will fail, causing discardCorruptedMessage which calls
			// pc._getConn(). If conn is nil this panics.
			msgID := &pb.MessageIdData{
				LedgerId: proto.Uint64(1),
				EntryId:  proto.Uint64(1),
			}
			cmd := &pb.CommandMessage{MessageId: msgID}
			// 4 bytes: ReadBrokerMetadata reads 2 bytes for magic number (won't
			// match broker metadata magic → returns nil, nil), then
			// ReadMessageMetadata fails → discardCorruptedMessage → _getConn()
			// panics if conn is nil.
			buf := internal.NewBuffer(4)
			buf.Write([]byte{0x00, 0x00, 0x00, 0x00})
			_ = handler.MessageReceived(cmd, buf)
		},
	}
	// Build consumer WITHOUT pre-setting _setConn to simulate the real
	// first-call path where conn is nil before grabConn completes.
	pc := newGrabConnTestConsumerNoConn(cnx, rpc)

	// This must not panic.
	assert.NotPanics(t, func() {
		_ = pc.grabConn("")
	})
}

// TestGrabConn_GetConnectionFailure verifies that grabConn returns the error
// from GetConnection without registering a handler or sending an RPC.
func TestGrabConn_GetConnectionFailure(t *testing.T) {
	cnx := newSpyConnection()
	rpc := &grabConnSpyRPCClient{cnx: cnx}
	pc := newGrabConnTestConsumer(cnx, rpc)

	// Override the pool to return an error
	pc.client.cnxPool = &grabConnMockPool{err: fmt.Errorf("connection refused")}

	err := pc.grabConn("")
	assert.ErrorContains(t, err, "connection refused")

	assert.False(t, cnx.handlerRegistered.Load(),
		"AddConsumeHandler must not be called when GetConnection fails")
}

// TestGrabConn_AddConsumeHandlerFailure verifies that grabConn returns the
// error from AddConsumeHandler without sending a subscribe RPC.
func TestGrabConn_AddConsumeHandlerFailure(t *testing.T) {
	cnx := newSpyConnection()
	cnx.addHandlerErr = fmt.Errorf("connection closed")
	rpc := &grabConnSpyRPCClient{cnx: cnx}
	pc := newGrabConnTestConsumer(cnx, rpc)

	err := pc.grabConn("")
	assert.ErrorContains(t, err, "connection closed")

	assert.False(t, rpc.handlerRegisteredDuringRPC.Load(),
		"Subscribe RPC must not be sent when AddConsumeHandler fails")
}

// TestGrabConn_ConnResetOnAddHandlerFailure verifies that when AddConsumeHandler
// fails on the first call (no prior connection), pc.conn is reset to nil.
func TestGrabConn_ConnResetOnAddHandlerFailure(t *testing.T) {
	cnx := newSpyConnection()
	cnx.addHandlerErr = fmt.Errorf("connection closed")
	rpc := &grabConnSpyRPCClient{cnx: cnx}
	pc := newGrabConnTestConsumerNoConn(cnx, rpc)

	err := pc.grabConn("")
	assert.ErrorContains(t, err, "connection closed")

	assert.Nil(t, pc.conn.Load(),
		"pc.conn must be reset to nil on first-call AddConsumeHandler failure")
}

// --- Helpers

// newGrabConnTestConsumer builds a minimal partitionConsumer wired to the
// given spy connection and RPC client, suitable for testing grabConn.
func newGrabConnTestConsumer(cnx *spyConnection, rpc *grabConnSpyRPCClient) *partitionConsumer {
	brokerURL, _ := url.Parse("pulsar://localhost:6650")
	if rpc.lookupResult == nil {
		rpc.lookupResult = &internal.LookupResult{
			LogicalAddr:  brokerURL,
			PhysicalAddr: brokerURL,
		}
	}
	pool := &grabConnMockPool{cnx: cnx}

	c := &client{
		cnxPool:   pool,
		rpcClient: rpc,
		log:       log.DefaultNopLogger(),
	}

	pc := &partitionConsumer{
		client:               c,
		topic:                "persistent://public/default/test",
		options:              &partitionConsumerOpts{subscription: "sub"},
		log:                  log.DefaultNopLogger(),
		compressionProviders: sync.Map{},
		connectedCh:          make(chan struct{}, 1),
		metrics:              newTestMetrics(),
	}
	// Required: lookupTopic calls _getConn().IsProxied() when assignedBrokerURL != "".
	// grabConn will overwrite this with the same connection after a successful subscribe.
	pc._setConn(cnx)
	// availablePermits is NOT initialised here because the success path and
	// simple error paths don't touch it. Only newGrabConnTestConsumerNoConn
	// sets it — the MessageReceived path (discardCorruptedMessage) calls
	// availablePermits.inc() which would panic on a nil receiver.
	return pc
}

// newGrabConnTestConsumerNoConn is like newGrabConnTestConsumer but does NOT
// pre-set _setConn, simulating the real first-call path where conn is nil.
// Only use with grabConn("") since a non-empty assignedBrokerURL would call
// _getConn().IsProxied() in lookupTopic and panic.
func newGrabConnTestConsumerNoConn(cnx *spyConnection, rpc *grabConnSpyRPCClient) *partitionConsumer {
	brokerURL, _ := url.Parse("pulsar://localhost:6650")
	if rpc.lookupResult == nil {
		rpc.lookupResult = &internal.LookupResult{
			LogicalAddr:  brokerURL,
			PhysicalAddr: brokerURL,
		}
	}
	pool := &grabConnMockPool{cnx: cnx}

	c := &client{
		cnxPool:   pool,
		rpcClient: rpc,
		log:       log.DefaultNopLogger(),
	}

	pc := &partitionConsumer{
		client:               c,
		topic:                "persistent://public/default/test",
		options:              &partitionConsumerOpts{subscription: "sub"},
		log:                  log.DefaultNopLogger(),
		compressionProviders: sync.Map{},
		connectedCh:          make(chan struct{}, 1),
		metrics:              newTestMetrics(),
	}
	pc.availablePermits = &availablePermits{pc: pc}
	return pc
}

// spyConnection tracks AddConsumeHandler / DeleteConsumeHandler calls and
// stores the registered handler so tests can deliver frames through it.
type spyConnection struct {
	dummyConnection
	handlerRegistered atomic.Bool
	handlerRemoved    atomic.Bool
	handler           atomic.Value // stores *partitionConsumer
	addHandlerErr     error        // when set, AddConsumeHandler returns this error
}

func newSpyConnection() *spyConnection {
	return &spyConnection{}
}

func (s *spyConnection) AddConsumeHandler(_ uint64, h internal.ConsumerHandler) error {
	if s.addHandlerErr != nil {
		return s.addHandlerErr
	}
	s.handlerRegistered.Store(true)
	// Store as *partitionConsumer so all stores use the same concrete type
	// (atomic.Value panics if you mix concrete types across stores).
	s.handler.Store(h.(*partitionConsumer))
	return nil
}

func (s *spyConnection) DeleteConsumeHandler(_ uint64) {
	s.handlerRemoved.Store(true)
	var h *partitionConsumer
	s.handler.Store(h)
}

// grabConnSpyRPCClient records the ordering of AddConsumeHandler relative to
// the subscribe RPC, and optionally injects errors or mid-RPC callbacks.
type grabConnSpyRPCClient struct {
	internal.RPCClient
	cnx          *spyConnection
	lookupResult *internal.LookupResult

	// handlerRegisteredDuringRPC is true if AddConsumeHandler was called
	// before the subscribe RPC executed.
	handlerRegisteredDuringRPC atomic.Bool

	// subscribeErr, when set, makes the subscribe RPC return this error.
	subscribeErr error

	// duringSubscribe, when set, is called inside the subscribe RPC to
	// simulate broker frames arriving while the RPC is in flight.
	duringSubscribe func()

	// closeSentOnCnx is true if a CLOSE_CONSUMER was sent via RequestOnCnx
	// (as opposed to RequestWithCnxKeySuffix which could pick a different connection).
	closeSentOnCnx atomic.Bool
}

func (r *grabConnSpyRPCClient) NewRequestID() uint64 { return 1 }

func (r *grabConnSpyRPCClient) RequestOnCnxNoWait(_ internal.Connection, _ pb.BaseCommand_Type,
	_ proto.Message) error {
	return nil
}

func (r *grabConnSpyRPCClient) RequestOnCnx(_ internal.Connection, _ uint64,
	cmdType pb.BaseCommand_Type, _ proto.Message) (*internal.RPCResult, error) {

	switch cmdType {
	case pb.BaseCommand_CLOSE_CONSUMER:
		r.closeSentOnCnx.Store(true)
		return nil, nil
	case pb.BaseCommand_SUBSCRIBE:
		// handled below
	default:
		panic(fmt.Sprintf("grabConnSpyRPCClient: unexpected command type %v", cmdType))
	}

	r.handlerRegisteredDuringRPC.Store(r.cnx.handlerRegistered.Load())

	if r.duringSubscribe != nil {
		r.duringSubscribe()
	}

	if r.subscribeErr != nil {
		return nil, r.subscribeErr
	}

	successType := pb.BaseCommand_SUCCESS
	return &internal.RPCResult{
		Response: &pb.BaseCommand{Type: &successType},
		Cnx:      r.cnx,
	}, nil
}

func (r *grabConnSpyRPCClient) LookupService(_ string) (internal.LookupService, error) {
	return &grabConnMockLookup{result: r.lookupResult}, nil
}

type grabConnMockLookup struct {
	internal.LookupService
	result *internal.LookupResult
}

func (m *grabConnMockLookup) Lookup(_ string) (*internal.LookupResult, error) {
	return m.result, nil
}

type grabConnMockPool struct {
	internal.ConnectionPool
	cnx internal.Connection
	err error
}

func (m *grabConnMockPool) GetConnection(_ *url.URL, _ *url.URL, _ int32) (internal.Connection, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.cnx, nil
}
