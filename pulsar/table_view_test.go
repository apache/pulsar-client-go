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

// mockReader is a minimal Reader stub whose Next blocks until ctx is cancelled.
type mockReader struct {
	topic string
}

func (m *mockReader) Topic() string                             { return m.topic }
func (m *mockReader) Next(ctx context.Context) (Message, error) { <-ctx.Done(); return nil, ctx.Err() }
func (m *mockReader) HasNext() bool                             { return false }
func (m *mockReader) Close()                                    {}
func (m *mockReader) Seek(MessageID) error                      { return nil }
func (m *mockReader) SeekByTime(time.Time) error                { return nil }
func (m *mockReader) GetLastMessageID() (MessageID, error)      { return nil, nil }

// TestCloseCallsCancelFuncToAvoidDeadlock verifies that Close() cancels
// each reader's context so that watchReaderForNewMessages can exit and
// Close() does not deadlock.
func TestCloseCallsCancelFuncToAvoidDeadlock(t *testing.T) {
	tv := &TableViewImpl{
		data:          make(map[string]interface{}),
		cancelReaders: make(map[string]cancelReader),
		logger:        log.DefaultNopLogger(),
		closedCh:      make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	reader := &mockReader{topic: "test-topic"}
	tv.cancelReaders["test-topic"] = cancelReader{reader: reader, cancelFunc: cancel}
	go tv.watchReaderForNewMessages(ctx, reader)

	done := make(chan struct{})
	go func() { tv.Close(); close(done) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close() deadlocked: cancelFunc() was probably not called")
	}
}
