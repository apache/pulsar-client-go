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
	"sync/atomic"
	"testing"
	"time"
)

// TestAsyncProducerContextCancellation tests that SendAsync respects context cancellation
func TestAsyncProducerContextCancellation(t *testing.T) {
	// Create a mock partition producer for testing
	pp := &partitionProducer{
		log:              &mockLogger{},
		dataChan:         make(chan *sendRequest, 10),
		options:          &ProducerOptions{SendTimeout: 1 * time.Second},
		publishSemaphore: &mockSemaphore{},
		client:           &client{memLimit: &mockMemoryLimit{}},
		metrics:          &mockMetrics{},
	}
	pp.state.Store(int32(producerReady))

	// Start a goroutine that simulates the event loop but blocks (simulating connection failure)
	blockingEventLoop := make(chan struct{})
	go func() {
		for range pp.dataChan {
			// Block forever to simulate connection failure
			<-blockingEventLoop
		}
	}()

	// Create a context with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Track if callback was called
	var callbackCalled int32
	var callbackErr error
	var wg sync.WaitGroup
	wg.Add(1)

	// Send a message with the timeout context
	pp.SendAsync(ctx, &ProducerMessage{
		Payload: []byte("test"),
	}, func(msgID MessageID, msg *ProducerMessage, err error) {
		atomic.StoreInt32(&callbackCalled, 1)
		callbackErr = err
		wg.Done()
	})

	// Wait for the callback to be called
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - callback was called
		if atomic.LoadInt32(&callbackCalled) != 1 {
			t.Fatal("Callback was not called")
		}
		if callbackErr != ErrContextExpired {
			t.Fatalf("Expected ErrContextExpired, got %v", callbackErr)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Test timed out - context cancellation was not honored")
	}

	// Cleanup
	close(blockingEventLoop)
	close(pp.dataChan)
}

// Mock implementations for testing
type mockLogger struct{}

func (m *mockLogger) SubLogger(fields map[string]interface{}) Logger                     { return m }
func (m *mockLogger) WithFields(fields map[string]interface{}) Entry                     { return m }
func (m *mockLogger) WithField(name string, value interface{}) Entry                     { return m }
func (m *mockLogger) WithError(err error) Entry                                          { return m }
func (m *mockLogger) Debug(args ...interface{})                                          {}
func (m *mockLogger) Info(args ...interface{})                                           {}
func (m *mockLogger) Warn(args ...interface{})                                           {}
func (m *mockLogger) Error(args ...interface{})                                          {}
func (m *mockLogger) Debugf(format string, args ...interface{})                          {}
func (m *mockLogger) Infof(format string, args ...interface{})                           {}
func (m *mockLogger) Warnf(format string, args ...interface{})                           {}
func (m *mockLogger) Errorf(format string, args ...interface{})                          {}

type mockSemaphore struct{}

func (m *mockSemaphore) Acquire(ctx context.Context) bool { return true }
func (m *mockSemaphore) TryAcquire() bool                 { return true }
func (m *mockSemaphore) Release()                         {}

type mockMemoryLimit struct{}

func (m *mockMemoryLimit) ReserveMemory(ctx context.Context, size int64) bool { return true }
func (m *mockMemoryLimit) TryReserveMemory(size int64) bool                   { return true }
func (m *mockMemoryLimit) ReleaseMemory(size int64)                           {}

type mockMetrics struct{}

func (m *mockMetrics) ProducersOpened() Counter                      { return &mockCounter{} }
func (m *mockMetrics) ProducersClosed() Counter                      { return &mockCounter{} }
func (m *mockMetrics) ProducersPartitions() Gauge                    { return &mockGauge{} }
func (m *mockMetrics) ProducersReconnectFailure() Counter            { return &mockCounter{} }
func (m *mockMetrics) ProducersReconnectMaxRetry() Counter           { return &mockCounter{} }
func (m *mockMetrics) ConnectionsOpened() Counter                    { return &mockCounter{} }
func (m *mockMetrics) ConnectionsClosed() Counter                    { return &mockCounter{} }
func (m *mockMetrics) ConnectionsEstablishmentErrors() Counter       { return &mockCounter{} }
func (m *mockMetrics) ConnectionsHandshakeErrors() Counter           { return &mockCounter{} }
func (m *mockMetrics) LookupRequestsCount() Counter                  { return &mockCounter{} }
func (m *mockMetrics) PartitionedTopicMetadataRequestsCount() Counter { return &mockCounter{} }
func (m *mockMetrics) RPCRequestCount() Counter                      { return &mockCounter{} }
func (m *mockMetrics) ConsumersOpened() Counter                      { return &mockCounter{} }
func (m *mockMetrics) ConsumersClosed() Counter                      { return &mockCounter{} }
func (m *mockMetrics) ConsumersPartitions() Gauge                    { return &mockGauge{} }
func (m *mockMetrics) ConsumersReconnectFailure() Counter            { return &mockCounter{} }
func (m *mockMetrics) ConsumersReconnectMaxRetry() Counter           { return &mockCounter{} }
func (m *mockMetrics) ReadersOpened() Counter                        { return &mockCounter{} }
func (m *mockMetrics) ReadersClosed() Counter                        { return &mockCounter{} }
func (m *mockMetrics) MessagesReceived() Counter                     { return &mockCounter{} }
func (m *mockMetrics) MessagesReceivedAckTimeouts() Counter          { return &mockCounter{} }
func (m *mockMetrics) BytesReceived() Counter                        { return &mockCounter{} }
func (m *mockMetrics) PrefetchedMessages() Gauge                     { return &mockGauge{} }
func (m *mockMetrics) PrefetchedBytes() Gauge                        { return &mockGauge{} }
func (m *mockMetrics) ConsumeAcksCount() Counter                     { return &mockCounter{} }
func (m *mockMetrics) ConsumeNacksCount() Counter                    { return &mockCounter{} }
func (m *mockMetrics) ConsumeErrorsUnknownCount() Counter            { return &mockCounter{} }
func (m *mockMetrics) MessageReceiveLatency() Histogram              { return &mockHistogram{} }
func (m *mockMetrics) MessageProcessingLatency() Histogram           { return &mockHistogram{} }
func (m *mockMetrics) AcksLatency() Histogram                        { return &mockHistogram{} }
func (m *mockMetrics) NacksLatency() Histogram                       { return &mockHistogram{} }
func (m *mockMetrics) DlqMessagesCounter() Counter                   { return &mockCounter{} }
func (m *mockMetrics) MessagesPublished() Counter                    { return &mockCounter{} }
func (m *mockMetrics) BytesPublished() Counter                       { return &mockCounter{} }
func (m *mockMetrics) MessagesPending() Gauge                        { return &mockGauge{} }
func (m *mockMetrics) BytesPending() Gauge                           { return &mockGauge{} }
func (m *mockMetrics) PublishErrorsTimeout() Counter                 { return &mockCounter{} }
func (m *mockMetrics) PublishErrorsMsgTooLarge() Counter             { return &mockCounter{} }
func (m *mockMetrics) PublishLatency() Histogram                     { return &mockHistogram{} }
func (m *mockMetrics) PublishRPCLatency() Histogram                  { return &mockHistogram{} }
func (m *mockMetrics) SendingBuffersCount() Gauge                    { return &mockGauge{} }

type mockCounter struct{}

func (m *mockCounter) Inc()            {}
func (m *mockCounter) Add(delta int64) {}

type mockGauge struct{}

func (m *mockGauge) Inc()               {}
func (m *mockGauge) Dec()               {}
func (m *mockGauge) Add(delta float64) {}
func (m *mockGauge) Sub(delta float64) {}
func (m *mockGauge) Set(value float64) {}

type mockHistogram struct{}

func (m *mockHistogram) Observe(value float64) {}