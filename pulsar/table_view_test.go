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
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/apache/pulsar-client-go/integration-tests/pb"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableView(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.NoError(t, err)
	defer client.Close()

	topic := newTopicName()
	schema := NewStringSchema(nil)

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: schema,
	})
	assert.NoError(t, err)
	defer producer.Close()

	numMsg := 10
	valuePrefix := "hello pulsar: "
	for i := 0; i < numMsg; i++ {
		key := fmt.Sprintf("%d", i)
		t.Log(key)
		_, err = producer.Send(context.Background(), &ProducerMessage{
			Key:   key,
			Value: valuePrefix + key,
		})
		assert.NoError(t, err)
	}

	// create table view
	v := ""
	tv, err := client.CreateTableView(TableViewOptions{
		Topic:           topic,
		Schema:          schema,
		SchemaValueType: reflect.TypeOf(&v),
	})
	assert.NoError(t, err)
	defer tv.Close()

	// Wait until tv receives all messages
	for tv.Size() < 10 {
		time.Sleep(time.Second * 1)
		t.Logf("TableView number of elements: %d", tv.Size())
	}

	for k, v := range tv.Entries() {
		assert.Equal(t, valuePrefix+k, *(v.(*string)))
	}
}

func TestTableViewSchemas(t *testing.T) {
	var tests = []struct {
		name          string
		schema        Schema
		schemaType    interface{}
		producerValue interface{}
		expValueOut   interface{}
		valueCheck    func(t *testing.T, got interface{}) // Overrides expValueOut for more complex checks
	}{
		{
			name:          "BytesSchema",
			schema:        NewBytesSchema(nil),
			schemaType:    []byte(`any`),
			producerValue: []byte(`hello pulsar`),
			expValueOut:   []byte(`hello pulsar`),
		},
		{
			name:          "StringSchema",
			schema:        NewStringSchema(nil),
			schemaType:    strPointer("hello pulsar"),
			producerValue: "hello pulsar",
			expValueOut:   strPointer("hello pulsar"),
		},
		{
			name:          "JSONSchema",
			schema:        NewJSONSchema(exampleSchemaDef, nil),
			schemaType:    testJSON{},
			producerValue: testJSON{ID: 1, Name: "Pulsar"},
			expValueOut:   testJSON{ID: 1, Name: "Pulsar"},
		},
		{
			name:          "JSONSchema pointer type",
			schema:        NewJSONSchema(exampleSchemaDef, nil),
			schemaType:    &testJSON{ID: 1, Name: "Pulsar"},
			producerValue: testJSON{ID: 1, Name: "Pulsar"},
			expValueOut:   &testJSON{ID: 1, Name: "Pulsar"},
		},
		{
			name:          "AvroSchema",
			schema:        NewAvroSchema(exampleSchemaDef, nil),
			schemaType:    testAvro{ID: 1, Name: "Pulsar"},
			producerValue: testAvro{ID: 1, Name: "Pulsar"},
			expValueOut:   testAvro{ID: 1, Name: "Pulsar"},
		},
		{
			name:          "Int8Schema",
			schema:        NewInt8Schema(nil),
			schemaType:    int8(0),
			producerValue: int8(1),
			expValueOut:   int8(1),
		},
		{
			name:          "Int16Schema",
			schema:        NewInt16Schema(nil),
			schemaType:    int16(0),
			producerValue: int16(1),
			expValueOut:   int16(1),
		},
		{
			name:          "Int32Schema",
			schema:        NewInt32Schema(nil),
			schemaType:    int32(0),
			producerValue: int32(1),
			expValueOut:   int32(1),
		},
		{
			name:          "Int64Schema",
			schema:        NewInt64Schema(nil),
			schemaType:    int64(0),
			producerValue: int64(1),
			expValueOut:   int64(1),
		},
		{
			name:          "FloatSchema",
			schema:        NewFloatSchema(nil),
			schemaType:    float32(0),
			producerValue: float32(1),
			expValueOut:   float32(1),
		},
		{
			name:          "DoubleSchema",
			schema:        NewDoubleSchema(nil),
			schemaType:    float64(0),
			producerValue: float64(1),
			expValueOut:   float64(1),
		},
		{
			name:          "ProtoSchema",
			schema:        NewProtoSchema(protoSchemaDef, nil),
			schemaType:    pb.Test{},
			producerValue: &pb.Test{Num: 1, Msf: "Pulsar"},
			valueCheck: func(t *testing.T, got interface{}) {
				assert.IsType(t, pb.Test{}, got)
				assert.Equal(t, int32(1), got.(pb.Test).Num)
				assert.Equal(t, "Pulsar", got.(pb.Test).Msf)
			},
		},
		{
			name:          "ProtoNativeSchema",
			schema:        NewProtoNativeSchemaWithMessage(&pb.Test{}, nil),
			schemaType:    pb.Test{},
			producerValue: &pb.Test{Num: 1, Msf: "Pulsar"},
			valueCheck: func(t *testing.T, got interface{}) {
				assert.IsType(t, pb.Test{}, got)
				assert.Equal(t, int32(1), got.(pb.Test).Num)
				assert.Equal(t, "Pulsar", got.(pb.Test).Msf)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, err := NewClient(ClientOptions{
				URL: lookupURL,
			})

			assert.NoError(t, err)
			defer client.Close()

			topic := newTopicName()

			// create producer
			producer, err := client.CreateProducer(ProducerOptions{
				Topic:  topic,
				Schema: test.schema,
			})
			assert.NoError(t, err)
			defer producer.Close()

			_, err = producer.Send(context.Background(), &ProducerMessage{
				Key:   "testKey",
				Value: test.producerValue,
			})
			assert.NoError(t, err)

			// create table view
			tv, err := client.CreateTableView(TableViewOptions{
				Topic:           topic,
				Schema:          test.schema,
				SchemaValueType: reflect.TypeOf(test.schemaType),
			})
			assert.NoError(t, err)
			defer tv.Close()

			value := tv.Get("testKey")
			if test.valueCheck != nil {
				test.valueCheck(t, value)
			} else {
				assert.IsType(t, test.expValueOut, value)
				assert.Equal(t, test.expValueOut, value)
			}
		})
	}
}

func strPointer(s string) *string {
	return &s
}

func TestPublishNilValue(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.NoError(t, err)
	defer client.Close()

	topic := newTopicName()
	schema := NewStringSchema(nil)

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: schema,
	})
	assert.NoError(t, err)
	defer producer.Close()

	// create table view
	v := ""
	tv, err := client.CreateTableView(TableViewOptions{
		Topic:           topic,
		Schema:          schema,
		SchemaValueType: reflect.TypeOf(&v),
	})
	assert.NoError(t, err)
	defer tv.Close()

	_, err = producer.Send(context.Background(), &ProducerMessage{
		Key:   "key-1",
		Value: "value-1",
	})
	assert.NoError(t, err)

	require.Eventually(t, func() bool {
		return tv.Size() == 1
	}, 5*time.Second, 100*time.Millisecond)

	assert.Equal(t, *(tv.Get("key-1").(*string)), "value-1")

	// send nil value
	_, err = producer.Send(context.Background(), &ProducerMessage{
		Key: "key-1",
	})
	assert.NoError(t, err)

	require.Eventually(t, func() bool {
		return tv.Size() == 0
	}, 5*time.Second, 100*time.Millisecond)

	_, err = producer.Send(context.Background(), &ProducerMessage{
		Key:   "key-2",
		Value: "value-2",
	})
	assert.NoError(t, err)

	require.Eventually(t, func() bool {
		return tv.Size() == 1
	}, 5*time.Second, 100*time.Millisecond)

	assert.Equal(t, *(tv.Get("key-2").(*string)), "value-2")
}

func TestForEachAndListenJSONSchema(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.NoError(t, err)
	defer client.Close()

	topic := newTopicName()
	schema := NewJSONSchema(exampleSchemaDef, nil)

	// create table view
	tv, err := client.CreateTableView(TableViewOptions{
		Topic:           topic,
		Schema:          schema,
		SchemaValueType: reflect.TypeOf(testJSON{}),
	})
	assert.NoError(t, err)
	defer tv.Close()

	// create listener
	valuePrefix := "hello pulsar: "
	tv.ForEachAndListen(func(key string, value interface{}) error {
		t.Log("foreach" + key)
		s, ok := value.(testJSON)
		assert.Truef(t, ok, "expected value to be testJSON type got %T", value)
		assert.Equal(t, valuePrefix+key, s.Name)
		return nil
	})

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: schema,
	})
	assert.NoError(t, err)
	defer producer.Close()

	numMsg := 10
	for i := 0; i < numMsg; i++ {
		key := fmt.Sprintf("%d", i)
		t.Log("producing" + key)
		_, err = producer.Send(context.Background(), &ProducerMessage{
			Key: key,
			Value: testJSON{
				ID:   i,
				Name: valuePrefix + key,
			},
		})
		assert.NoError(t, err)
	}

	// Wait until tv receives all messages
	for tv.Size() < 10 {
		time.Sleep(time.Second * 1)
		t.Logf("TableView number of elements: %d", tv.Size())
	}
}

func TestTableViewIsEmpty(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topic := newTopicName()
	schema := NewStringSchema(nil)

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: schema,
	})
	assert.NoError(t, err)
	defer producer.Close()

	// create table view
	v := ""
	tv, err := client.CreateTableView(TableViewOptions{
		Topic:           topic,
		Schema:          schema,
		SchemaValueType: reflect.TypeOf(&v),
	})
	assert.NoError(t, err)
	defer tv.Close()

	// IsEmpty should return true when no messages have been published
	assert.True(t, tv.IsEmpty())
	assert.Equal(t, 0, tv.Size())

	// Publish a message
	_, err = producer.Send(context.Background(), &ProducerMessage{
		Key:   "key-1",
		Value: "value-1",
	})
	assert.NoError(t, err)

	require.Eventually(t, func() bool {
		return tv.Size() == 1
	}, 5*time.Second, 100*time.Millisecond)

	// IsEmpty should return false after receiving a message
	assert.False(t, tv.IsEmpty())
	assert.True(t, tv.ContainsKey("key-1"))
	assert.False(t, tv.ContainsKey("non-existent-key"))

	keys := tv.Keys()
	assert.Equal(t, 1, len(keys))
	assert.Equal(t, "key-1", keys[0])
}

// TestTableViewEntriesReturnsCopy is a pure unit test for Entries() that
// does not depend on a broker. It verifies two things:
//  1. The returned map has the exact same content as the internal data map.
//  2. The returned map is an independent copy: callers mutating the result
//     must not affect the TableView's internal state, and vice versa.
func TestTableViewEntriesReturnsCopy(t *testing.T) {
	tv := &TableViewImpl{
		data: map[string]interface{}{
			"k1": "v1",
			"k2": 42,
			"k3": []byte("bytes"),
		},
	}

	entries := tv.Entries()

	// 1. The returned content must match the internal data.
	assert.Equal(t, 3, len(entries))
	assert.Equal(t, "v1", entries["k1"])
	assert.Equal(t, 42, entries["k2"])
	assert.Equal(t, []byte("bytes"), entries["k3"])

	// 2. Mutations on the returned map must not affect internal data.
	entries["k1"] = "modified"
	delete(entries, "k2")
	entries["new-key"] = "new-value"

	assert.Equal(t, "v1", tv.data["k1"], "external modification should not affect internal data")
	assert.Equal(t, 42, tv.data["k2"], "external delete should not affect internal data")
	_, exists := tv.data["new-key"]
	assert.False(t, exists, "external insertion should not leak into internal data")

	// 3. Changes to internal data must not affect the previously returned snapshot.
	tv.data["k1"] = "internal-change"
	assert.Equal(t, "modified", entries["k1"],
		"internal modification should not affect the already-returned snapshot")
}

// TestTableViewEntriesEmpty verifies that calling Entries() on an empty
// TableView returns a non-nil empty map that is safe to range over or
// write to.
func TestTableViewEntriesEmpty(t *testing.T) {
	tv := &TableViewImpl{
		data: map[string]interface{}{},
	}

	entries := tv.Entries()
	assert.NotNil(t, entries, "Entries() should not return nil")
	assert.Equal(t, 0, len(entries))

	// Writing to the returned map must not affect internal state.
	entries["x"] = 1
	assert.Equal(t, 0, len(tv.data), "external writes should not affect internal data")
}

// TestTableViewEntriesConcurrent verifies that Entries() does not trigger
// a "concurrent map read and map write" panic when readers iterate the
// returned map while writers mutate the internal data map. Run with
// `go test -race` to additionally detect data races.
func TestTableViewEntriesConcurrent(_ *testing.T) {
	tv := &TableViewImpl{
		data: make(map[string]interface{}),
	}

	stop := make(chan struct{})
	done := make(chan struct{}, 2)

	// writer: hold the lock to simulate handleMessage writes.
	go func() {
		defer func() { done <- struct{}{} }()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				tv.dataMu.Lock()
				tv.data[fmt.Sprintf("k-%d", i%50)] = i
				if i%3 == 0 {
					delete(tv.data, fmt.Sprintf("k-%d", (i+1)%50))
				}
				tv.dataMu.Unlock()
			}
		}
	}()

	// reader: repeatedly call Entries() and range over the returned copy.
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			select {
			case <-stop:
				return
			default:
				entries := tv.Entries()
				// Ranging the copy must not trigger concurrent map r/w.
				for k, v := range entries {
					_ = k
					_ = v
				}
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	<-done
	<-done
}

// TestTableViewEntriesIntegration is an end-to-end test against a real
// broker. It verifies that after a TableView is populated via producer
// writes, Entries() stays consistent with tv.Get() and tv.Size(), and
// that mutating the returned map does not affect subsequent lookups.
func TestTableViewEntriesIntegration(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	topic := newTopicName()
	schema := NewStringSchema(nil)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: schema,
	})
	assert.NoError(t, err)
	defer producer.Close()

	numMsg := 5
	valuePrefix := "entries-test: "
	for i := 0; i < numMsg; i++ {
		key := fmt.Sprintf("key-%d", i)
		_, err = producer.Send(context.Background(), &ProducerMessage{
			Key:   key,
			Value: valuePrefix + key,
		})
		assert.NoError(t, err)
	}

	v := ""
	tv, err := client.CreateTableView(TableViewOptions{
		Topic:           topic,
		Schema:          schema,
		SchemaValueType: reflect.TypeOf(&v),
	})
	assert.NoError(t, err)
	defer tv.Close()

	require.Eventually(t, func() bool {
		return tv.Size() == numMsg
	}, 10*time.Second, 200*time.Millisecond)

	entries := tv.Entries()
	assert.Equal(t, numMsg, len(entries))
	for k, val := range entries {
		assert.Equal(t, valuePrefix+k, *(val.(*string)))
	}

	// Poison the returned map and ensure tv.Get / tv.Size are unaffected.
	for k := range entries {
		delete(entries, k)
	}
	entries["poison"] = strPointer("should-not-leak")

	assert.Equal(t, numMsg, tv.Size(),
		"mutating the result of Entries() should not affect TableView state")
	assert.Nil(t, tv.Get("poison"),
		"externally injected keys should not leak into the TableView")
	for i := 0; i < numMsg; i++ {
		key := fmt.Sprintf("key-%d", i)
		got := tv.Get(key)
		if assert.NotNil(t, got, "key %s should still exist", key) {
			assert.Equal(t, valuePrefix+key, *(got.(*string)))
		}
	}
}

// recordingLogger wraps a nop logger and counts Errorf invocations so
// shutdown tests can assert that normal termination paths
// (ConsumerClosed / context cancellation) emit no error logs while
// transient failures do. It embeds the package's nop logger to inherit
// no-op implementations for every method we do not care about.
type recordingLogger struct {
	log.Logger
	mu        sync.Mutex
	errorMsgs []string
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{Logger: log.DefaultNopLogger()}
}

func (r *recordingLogger) Errorf(format string, args ...interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errorMsgs = append(r.errorMsgs, fmt.Sprintf(format, args...))
}

func (r *recordingLogger) errorLogCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.errorMsgs)
}

func (r *recordingLogger) snapshotErrorMsgs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.errorMsgs))
	copy(out, r.errorMsgs)
	return out
}

// mockReader is a lightweight Reader implementation used to drive
// watchReaderForNewMessages in unit tests. nextFn is invoked for every
// call to Next so each test can script the exact (message, error) sequence
// it wants to exercise.
type mockReader struct {
	topic     string
	nextCalls int32
	nextFn    func(ctx context.Context, call int32) (Message, error)
}

func (m *mockReader) Topic() string { return m.topic }

func (m *mockReader) Next(ctx context.Context) (Message, error) {
	call := atomic.AddInt32(&m.nextCalls, 1)
	return m.nextFn(ctx, call)
}

func (m *mockReader) HasNext() bool                        { return false }
func (m *mockReader) Close()                               {}
func (m *mockReader) Seek(MessageID) error                 { return nil }
func (m *mockReader) SeekByTime(time.Time) error           { return nil }
func (m *mockReader) GetLastMessageID() (MessageID, error) { return nil, nil }

// TestWatchReaderExitsOnConsumerClosed verifies that watchReaderForNewMessages
// exits immediately when the reader reports ConsumerClosed, without entering
// another iteration.
func TestWatchReaderExitsOnConsumerClosed(t *testing.T) {
	rec := newRecordingLogger()
	tv := &TableViewImpl{
		data:   make(map[string]interface{}),
		logger: rec,
	}

	reader := &mockReader{
		topic: "persistent://public/default/unit-test",
		nextFn: func(_ context.Context, _ int32) (Message, error) {
			return nil, newError(ConsumerClosed, "consumer closed")
		},
	}

	done := make(chan struct{})
	go func() {
		tv.watchReaderForNewMessages(context.Background(), reader)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchReaderForNewMessages did not exit on ConsumerClosed")
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&reader.nextCalls),
		"ConsumerClosed must cause an immediate exit without further Next calls")
	assert.Equal(t, 0, rec.errorLogCount(),
		"ConsumerClosed is a normal shutdown and must not produce an error log, got: %v",
		rec.snapshotErrorMsgs())
}

// TestWatchReaderExitsOnContextCanceled verifies that watchReaderForNewMessages
// exits immediately once the context is cancelled.
func TestWatchReaderExitsOnContextCanceled(t *testing.T) {
	rec := newRecordingLogger()
	tv := &TableViewImpl{
		data:   make(map[string]interface{}),
		logger: rec,
	}

	ctx, cancel := context.WithCancel(context.Background())

	reader := &mockReader{
		topic: "persistent://public/default/unit-test",
		nextFn: func(ctx context.Context, _ int32) (Message, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	done := make(chan struct{})
	go func() {
		tv.watchReaderForNewMessages(ctx, reader)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchReaderForNewMessages did not exit after context cancellation")
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&reader.nextCalls),
		"a cancelled context must cause an immediate exit without further Next calls")
	assert.Equal(t, 0, rec.errorLogCount(),
		"context cancellation is a normal shutdown and must not produce an error log, got: %v",
		rec.snapshotErrorMsgs())
}

// TestWatchReaderRecoversFromTransientError verifies that watchReaderForNewMessages
// keeps consuming after a non-fatal error: the error is logged, the loop
// continues, a subsequent valid message is handled, and only ConsumerClosed
// finally stops it.
func TestWatchReaderRecoversFromTransientError(t *testing.T) {
	rec := newRecordingLogger()
	tv := &TableViewImpl{
		data:    map[string]interface{}{"k1": "existing value"},
		logger:  rec,
		options: TableViewOptions{SchemaValueType: reflect.TypeOf("")},
	}

	reader := &mockReader{
		topic: "persistent://public/default/unit-test",
		nextFn: func(_ context.Context, call int32) (Message, error) {
			switch call {
			case 1:
				return nil, fmt.Errorf("transient read error")
			case 2:
				return &mockMessage{key: "k1", payload: []byte{}}, nil
			default:
				return nil, newError(ConsumerClosed, "consumer closed")
			}
		},
	}

	done := make(chan struct{})
	go func() {
		tv.watchReaderForNewMessages(context.Background(), reader)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchReaderForNewMessages did not exit after ConsumerClosed")
	}

	assert.Equal(t, int32(3), atomic.LoadInt32(&reader.nextCalls))
	assert.False(t, tv.ContainsKey("k1"),
		"tombstone after transient error must be handled")
	assert.Equal(t, 1, rec.errorLogCount(),
		"only the transient error should be logged, got: %v", rec.snapshotErrorMsgs())
}

// mockMessage is a minimal Message implementation sufficient for exercising
// handleMessage's tombstone (empty payload) branch.
type mockMessage struct {
	key     string
	payload []byte
}

func (m *mockMessage) Topic() string                            { return "" }
func (m *mockMessage) ProducerName() string                     { return "" }
func (m *mockMessage) Properties() map[string]string            { return nil }
func (m *mockMessage) Payload() []byte                          { return m.payload }
func (m *mockMessage) IsNullValue() bool                        { return len(m.payload) == 0 }
func (m *mockMessage) ID() MessageID                            { return nil }
func (m *mockMessage) PublishTime() time.Time                   { return time.Time{} }
func (m *mockMessage) EventTime() time.Time                     { return time.Time{} }
func (m *mockMessage) Key() string                              { return m.key }
func (m *mockMessage) OrderingKey() string                      { return "" }
func (m *mockMessage) RedeliveryCount() uint32                  { return 0 }
func (m *mockMessage) IsReplicated() bool                       { return false }
func (m *mockMessage) GetReplicatedFrom() string                { return "" }
func (m *mockMessage) GetSchemaValue(_ interface{}) error       { return nil }
func (m *mockMessage) SchemaVersion() []byte                    { return nil }
func (m *mockMessage) GetEncryptionContext() *EncryptionContext { return nil }
func (m *mockMessage) Index() *uint64                           { return nil }
func (m *mockMessage) BrokerPublishTime() *time.Time            { return nil }

// blockingCloseReader wraps a Reader and makes Close() block until the
// given channel is closed. This models the real reader.Close() →
// consumer.closeWithCause() → wg.Wait() behaviour.
type blockingCloseReader struct {
	Reader
	nextExited chan struct{}
}

func (b *blockingCloseReader) Close() {
	<-b.nextExited
}

// TestCloseCallsCancelFuncToAvoidDeadlock verifies that Close() cancels
// each reader's context so that watchReaderForNewMessages can exit and
// Close() does not deadlock.
//
// The mock models the real shutdown coupling: Close() blocks until Next()
// returns. Without the cancelFunc() call, Next() never unblocks, Close()
// never returns, and the test times out.
func TestCloseCallsCancelFuncToAvoidDeadlock(t *testing.T) {
	tv := &TableViewImpl{
		data:          make(map[string]interface{}),
		cancelReaders: make(map[string]cancelReader),
		logger:        log.DefaultNopLogger(),
		closedCh:      make(chan struct{}),
	}

	entered := make(chan struct{})    // closed when Next() enters its blocking wait
	nextExited := make(chan struct{}) // closed when Next() returns

	ctx, cancel := context.WithCancel(context.Background())
	reader := &blockingCloseReader{
		Reader: &mockReader{
			topic: "test-topic",
			nextFn: func(ctx context.Context, _ int32) (Message, error) {
				close(entered)
				<-ctx.Done()
				close(nextExited)
				return nil, ctx.Err()
			},
		},
		nextExited: nextExited,
	}
	tv.cancelReaders["test-topic"] = cancelReader{reader: reader, cancelFunc: cancel}
	go tv.watchReaderForNewMessages(ctx, reader)

	// Wait until Next() is entered so the deadlock scenario is fully set up.
	<-entered

	done := make(chan struct{})
	go func() { tv.Close(); close(done) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close() deadlocked: cancelFunc() was probably not called")
	}
}

// schemaMessage extends mockMessage so that GetSchemaValue copies a
// predefined value into the destination pointer. It is used to exercise
// handleMessage's non-tombstone branch in a pure unit test, without
// requiring a real broker or schema.
type schemaMessage struct {
	mockMessage
	value string
}

func (m *schemaMessage) GetSchemaValue(dst interface{}) error {
	// dst is *string here because the test configures SchemaValueType as
	// reflect.TypeOf("") and handleMessage passes reflect.New(T).Interface().
	p, ok := dst.(*string)
	if !ok {
		return fmt.Errorf("unexpected dst type %T", dst)
	}
	*p = m.value
	return nil
}

// TestHandleMessageListenerValue is a pure unit test that pins down the
// contract between handleMessage and registered listeners:
//
//  1. For a normal message, the listener receives the deserialized value
//     and tv.data is updated accordingly.
//  2. For a tombstone (empty payload), the listener receives nil (not the
//     type's zero value) and the key is removed from tv.data.
//
// This directly guards the bug where tombstones used to pass a type zero
// value to listeners, making it impossible to distinguish "key deleted"
// from "value updated to zero".
func TestHandleMessageListenerValue(t *testing.T) {
	tv := &TableViewImpl{
		data:    make(map[string]interface{}),
		logger:  log.DefaultNopLogger(),
		options: TableViewOptions{SchemaValueType: reflect.TypeOf("")},
	}

	type callback struct {
		key   string
		value interface{}
	}
	var got []callback
	tv.listeners = append(tv.listeners, func(k string, v interface{}) error {
		got = append(got, callback{key: k, value: v})
		return nil
	})

	// 1) Normal message: listener must receive the deserialized value and
	//    tv.data must contain it.
	tv.handleMessage(&schemaMessage{
		mockMessage: mockMessage{key: "k1", payload: []byte("non-empty")},
		value:       "hello",
	})
	require.Len(t, got, 1)
	assert.Equal(t, "k1", got[0].key)
	assert.Equal(t, "hello", got[0].value,
		"listener must receive the deserialized value for a normal message")
	assert.Equal(t, "hello", tv.data["k1"],
		"tv.data must be updated to the deserialized value")

	// 2) Tombstone: listener must receive nil and the key must be removed.
	tv.handleMessage(&mockMessage{key: "k1", payload: nil})
	require.Len(t, got, 2)
	assert.Equal(t, "k1", got[1].key)
	assert.Nil(t, got[1].value,
		"listener must receive nil for a tombstone, not the type zero value")
	_, exists := tv.data["k1"]
	assert.False(t, exists, "tombstone must delete the key from tv.data")

	// 3) Tombstone for a key that never existed is still a nil callback.
	tv.handleMessage(&mockMessage{key: "never-existed", payload: []byte{}})
	require.Len(t, got, 3)
	assert.Equal(t, "never-existed", got[2].key)
	assert.Nil(t, got[2].value,
		"tombstone on an absent key must still deliver nil to listeners")
}

// failingSchemaMessage is a Message whose GetSchemaValue always returns
// an error. It is used to exercise handleMessage's deserialization-failure
// branch in a pure unit test.
type failingSchemaMessage struct {
	mockMessage
	err error
}

func (m *failingSchemaMessage) GetSchemaValue(_ interface{}) error {
	return m.err
}

// TestHandleMessageSkipsOnDecodeError pins down the contract for the
// GetSchemaValue failure path in handleMessage:
//
//  1. A previously stored good value for the same key must NOT be
//     overwritten with the type's zero value.
//  2. Registered listeners must NOT be invoked (so they cannot observe
//     an ambiguous zero value for a failed decode).
//  3. For a key that had no prior value, tv.data must remain unchanged
//     (no zero-value entry is inserted).
//  4. The failure must be surfaced via an error log so operators can
//     still diagnose the issue.
//
// This guards the fix for the "silent overwrite / zero-value ambiguity"
// bug: before the fix, handleMessage logged the error but continued to
// write reflect.New(T) (a zero value) into tv.data and notified
// listeners with that zero value.
func TestHandleMessageSkipsOnDecodeError(t *testing.T) {
	rec := newRecordingLogger()
	tv := &TableViewImpl{
		data:    map[string]interface{}{"k1": "existing value"},
		logger:  rec,
		options: TableViewOptions{SchemaValueType: reflect.TypeOf("")},
	}

	var listenerCalls int
	tv.listeners = append(tv.listeners, func(_ string, _ interface{}) error {
		listenerCalls++
		return nil
	})

	// 1) Decode error on a key with a prior value: value must be preserved
	//    and no listener must be called.
	tv.handleMessage(&failingSchemaMessage{
		mockMessage: mockMessage{key: "k1", payload: []byte("non-empty")},
		err:         fmt.Errorf("boom: cannot decode"),
	})
	assert.Equal(t, "existing value", tv.data["k1"],
		"decode failure must NOT overwrite a previously stored value")
	assert.Equal(t, 0, listenerCalls,
		"listener must NOT be invoked when decoding fails")
	assert.Equal(t, 1, rec.errorLogCount(),
		"decode failure must still be reported via an error log, got: %v",
		rec.snapshotErrorMsgs())

	// 2) Decode error on a brand new key: no zero-value entry must be
	//    inserted and the listener must still not be called.
	tv.handleMessage(&failingSchemaMessage{
		mockMessage: mockMessage{key: "brand-new", payload: []byte("non-empty")},
		err:         fmt.Errorf("boom: cannot decode"),
	})
	_, exists := tv.data["brand-new"]
	assert.False(t, exists,
		"decode failure on a new key must NOT create a zero-value entry")
	assert.Equal(t, 0, listenerCalls,
		"listener must NOT be invoked on decode failure, even for new keys")
	assert.Equal(t, 2, rec.errorLogCount(),
		"each decode failure must produce an error log, got: %v",
		rec.snapshotErrorMsgs())

	// 3) A subsequent successful message must still update tv.data and
	//    reach the listener, proving that the earlier `return` only
	//    skips the failing message and does not break later processing.
	tv.handleMessage(&schemaMessage{
		mockMessage: mockMessage{key: "k1", payload: []byte("non-empty")},
		value:       "updated",
	})
	assert.Equal(t, "updated", tv.data["k1"],
		"a subsequent successful message must update tv.data normally")
	assert.Equal(t, 1, listenerCalls,
		"listener must be called exactly once, only for the successful message")
}

// TestHandleMessageListenersNoDataRace exercises the data race between
// handleMessage (which reads tv.listeners) and ForEachAndListen (which
// appends to tv.listeners). Run with `go test -race` — before the fix
// the race detector flags a concurrent read/write on tv.listeners.
func TestHandleMessageListenersNoDataRace(t *testing.T) {
	tv := &TableViewImpl{
		data:    make(map[string]interface{}),
		logger:  log.DefaultNopLogger(),
		options: TableViewOptions{SchemaValueType: reflect.TypeOf("")},
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writer goroutine: continuously register new listeners.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = tv.ForEachAndListen(func(string, interface{}) error { return nil })
			}
		}
	}()

	// Reader goroutine: continuously deliver messages, which iterates tv.listeners.
	wg.Add(1)
	go func() {
		defer wg.Done()
		msg := &mockMessage{key: "k", payload: []byte{}}
		for {
			select {
			case <-stop:
				return
			default:
				tv.handleMessage(msg)
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestHandleMessageInvokesRegisteredListener verifies the basic contract
// that a listener registered via ForEachAndListen receives subsequent
// messages delivered through handleMessage.
func TestHandleMessageInvokesRegisteredListener(t *testing.T) {
	tv := &TableViewImpl{
		data:    make(map[string]interface{}),
		logger:  log.DefaultNopLogger(),
		options: TableViewOptions{SchemaValueType: reflect.TypeOf("")},
	}

	var got int32
	err := tv.ForEachAndListen(func(_ string, _ interface{}) error {
		atomic.AddInt32(&got, 1)
		return nil
	})
	require.NoError(t, err)

	tv.handleMessage(&mockMessage{key: "k1", payload: []byte{}})
	tv.handleMessage(&mockMessage{key: "k2", payload: []byte{}})

	assert.Equal(t, int32(2), atomic.LoadInt32(&got),
		"listener must be invoked once per handleMessage call")
}
