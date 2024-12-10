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

func TestTableView_Message(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})

	assert.NoError(t, err)
	defer client.Close()

	topic := newTopicName()
	schema := NewStringSchema(nil)

	// Create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: schema,
	})
	assert.NoError(t, err)
	defer producer.Close()

	numMsg := 10
	valuePrefix := "hello table view: "
	publicationTimeForKey := map[string]time.Time{}
	keys := make([]string, 0, numMsg)

	for i := 0; i < numMsg; i++ {
		key := fmt.Sprintf("%d", i)
		keys = append(keys, key)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		_, err = producer.Send(ctx, &ProducerMessage{
			Key:   key,
			Value: fmt.Sprintf(valuePrefix + key),
		})
		assert.NoError(t, err)

		publicationTimeForKey[key] = time.Now()
	}

	// Create table view
	v := ""
	tv, err := client.CreateTableView(TableViewOptions{
		Topic:           topic,
		Schema:          schema,
		SchemaValueType: reflect.TypeOf(&v),
	})
	assert.NoError(t, err)
	defer tv.Close()

	// Wait until table view receives all messages
	for tv.Size() < numMsg {
		time.Sleep(time.Second * 500)
		t.Logf("TableView number of elements: %d", tv.Size())
	}

	for _, k := range keys {
		msg := tv.Message(k)

		// Check that the payload can be accessed as bytes
		assert.Equal(t, []byte(fmt.Sprintf("%s%s", valuePrefix, k)), msg.Payload())

		// Check publication times can be accessed and are close to the recorded times above
		assert.WithinDuration(t, publicationTimeForKey[k], msg.PublishTime(), time.Millisecond*10)
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
