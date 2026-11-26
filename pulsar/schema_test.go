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
	"log"
	"sync"
	"testing"
	"time"

	pb "github.com/apache/pulsar-client-go/integration-tests/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testJSON struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type testAvro struct {
	ID   int
	Name string
}

var (
	exampleSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
	protoSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
		"\"fields\":[{\"name\":\"num\",\"type\":\"int\"},{\"name\":\"msf\",\"type\":\"string\"}]}"
)

func createClient() Client {
	// create client
	lookupURL := "pulsar://localhost:6650"
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func createHTTPLookupClient() Client {
	// create client
	lookupURL := "http://localhost:8080"
	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func TestBytesSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	topic := newTopicName()

	properties := make(map[string]string)
	properties["pulsar"] = "hello"
	producerSchemaBytes := NewBytesSchema(properties)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: producerSchemaBytes,
	})
	assert.NoError(t, err)

	_, err = producer.Send(context.Background(), &ProducerMessage{
		Value: []byte(`{"key": "value"}`),
	})
	require.NoError(t, err)
	_, err = producer.Send(context.Background(), &ProducerMessage{
		Value: []byte(`something else`),
	})
	require.NoError(t, err)
	producer.Close()

	// Create consumer
	consumerSchemaBytes := NewBytesSchema(nil)
	assert.NotNil(t, consumerSchemaBytes)
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            "sub-1",
		Schema:                      consumerSchemaBytes,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// Receive first message
	var out1 []byte
	msg1, err := consumer.Receive(ctx)
	assert.NoError(t, err)
	err = msg1.GetSchemaValue(&out1)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"key": "value"}`), out1)
	consumer.Ack(msg1)
	require.NoError(t, err)

	// Receive second message
	var out2 []byte
	msg2, err := consumer.Receive(ctx)
	fmt.Println(string(msg2.Payload()))
	assert.NoError(t, err)
	err = msg2.GetSchemaValue(&out2)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`something else`), out2)

	defer consumer.Close()
}

func TestJsonSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	properties := make(map[string]string)
	properties["pulsar"] = "hello"
	jsonSchemaWithProperties := NewJSONSchema(exampleSchemaDef, properties)
	producer1, err := client.CreateProducer(ProducerOptions{
		Topic:  "jsonTopic",
		Schema: jsonSchemaWithProperties,
	})
	assert.Nil(t, err)

	_, err = producer1.Send(context.Background(), &ProducerMessage{
		Value: &testJSON{
			ID:   100,
			Name: "pulsar",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	producer1.Close()

	//create consumer
	var s testJSON

	consumerJS, err := NewJSONSchemaWithValidation(exampleSchemaDef, nil)
	assert.Nil(t, err)
	assert.NotNil(t, consumerJS)
	consumerJS = NewJSONSchema(exampleSchemaDef, nil) // test this legacy function
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "jsonTopic",
		SubscriptionName:            "sub-1",
		Schema:                      consumerJS,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&s)
	assert.Nil(t, err)
	assert.Equal(t, s.ID, 100)
	assert.Equal(t, s.Name, "pulsar")

	defer consumer.Close()
}

func TestHTTPLookupJsonSchema(t *testing.T) {
	client := createHTTPLookupClient()
	defer client.Close()

	properties := make(map[string]string)
	properties["pulsar"] = "hello"
	jsonSchemaWithProperties := NewJSONSchema(exampleSchemaDef, properties)
	producer1, err := client.CreateProducer(ProducerOptions{
		Topic:  "httpLookupJsonTopic",
		Schema: jsonSchemaWithProperties,
	})
	assert.Nil(t, err)

	_, err = producer1.Send(context.Background(), &ProducerMessage{
		Value: &testJSON{
			ID:   100,
			Name: "pulsar",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	producer1.Close()

	//create consumer
	var s testJSON

	consumerJS, err := NewJSONSchemaWithValidation(exampleSchemaDef, nil)
	assert.Nil(t, err)
	assert.NotNil(t, consumerJS)
	consumerJS = NewJSONSchema(exampleSchemaDef, nil) // test this legacy function
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "httpLookupJsonTopic",
		SubscriptionName:            "sub-1",
		Schema:                      consumerJS,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&s)
	assert.Nil(t, err)
	assert.Equal(t, s.ID, 100)
	assert.Equal(t, s.Name, "pulsar")

	defer consumer.Close()
}

func TestProtoSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	// create producer
	psProducer, err := NewProtoSchemaWithValidation(protoSchemaDef, nil)
	assert.Nil(t, err)
	assert.NotNil(t, psProducer)
	psProducer = NewProtoSchema(protoSchemaDef, nil)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  "proto",
		Schema: psProducer,
	})
	assert.Nil(t, err)

	if _, err := producer.Send(context.Background(), &ProducerMessage{
		Value: &pb.Test{
			Num: 100,
			Msf: "pulsar",
		},
	}); err != nil {
		log.Fatal(err)
	}

	//create consumer
	unobj := pb.Test{}
	psConsumer := NewProtoSchema(protoSchemaDef, nil)
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "proto",
		SubscriptionName:            "sub-1",
		Schema:                      psConsumer,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&unobj)
	assert.Nil(t, err)
	assert.Equal(t, unobj.Num, int32(100))
	assert.Equal(t, unobj.Msf, "pulsar")
	defer consumer.Close()
}

func TestProtoNativeSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	topic := "proto-native"

	// create producer
	psProducer := NewProtoNativeSchemaWithMessage(&pb.Test{}, nil)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: psProducer,
	})
	assert.Nil(t, err)

	names := []string{
		"name-a",
		"name-b",
		"name-c",
	}
	if _, err := producer.Send(context.Background(), &ProducerMessage{
		Value: &pb.Test{
			Num: 100,
			Msf: "pulsar",
			Foo: &pb.Foo{
				Name:  "foo name",
				Value: 200,
				Names: names,
			},
			HiContent: &pb.HiContent{
				Id:      300,
				Content: "hi content",
			},
		},
	}); err != nil {
		log.Fatal(err)
	}

	//create consumer
	unobj := pb.Test{}
	psConsumer := NewProtoNativeSchemaWithMessage(&pb.Test{}, nil)
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            "sub-1",
		Schema:                      psConsumer,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&unobj)
	assert.Nil(t, err)
	assert.Equal(t, unobj.Num, int32(100))
	assert.Equal(t, unobj.Msf, "pulsar")
	assert.Equal(t, unobj.Foo.Name, "foo name")
	assert.Equal(t, unobj.Foo.Value, int32(200))
	assert.Equal(t, unobj.Foo.Names, names)
	assert.Equal(t, unobj.HiContent.Id, int32(300))
	assert.Equal(t, unobj.HiContent.Content, "hi content")
	consumer.Ack(msg)
	defer consumer.Close()
}

func TestAvroSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	// create producer
	asProducer, err := NewAvroSchemaWithValidation(exampleSchemaDef, nil)
	assert.Nil(t, err)
	assert.NotNil(t, asProducer)
	asProducer = NewAvroSchema(exampleSchemaDef, nil)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  "avro-topic",
		Schema: asProducer,
	})
	assert.Nil(t, err)
	if _, err := producer.Send(context.Background(), &ProducerMessage{
		Value: testAvro{
			ID:   100,
			Name: "pulsar",
		},
	}); err != nil {
		log.Fatal(err)
	}

	//create consumer
	unobj := testAvro{}

	asConsumer := NewAvroSchema(exampleSchemaDef, nil)
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "avro-topic",
		SubscriptionName:            "sub-1",
		Schema:                      asConsumer,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&unobj)
	assert.Nil(t, err)
	assert.Equal(t, unobj.ID, 100)
	assert.Equal(t, unobj.Name, "pulsar")
	defer consumer.Close()
}

func TestHTTPLookupAvroSchema(t *testing.T) {
	client := createHTTPLookupClient()
	defer client.Close()

	// create producer
	asProducer, err := NewAvroSchemaWithValidation(exampleSchemaDef, nil)
	assert.Nil(t, err)
	assert.NotNil(t, asProducer)
	asProducer = NewAvroSchema(exampleSchemaDef, nil)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  "httpLookup-avro-topic",
		Schema: asProducer,
	})
	assert.Nil(t, err)
	if _, err := producer.Send(context.Background(), &ProducerMessage{
		Value: testAvro{
			ID:   100,
			Name: "pulsar",
		},
	}); err != nil {
		log.Fatal(err)
	}

	//create consumer
	unobj := testAvro{}

	asConsumer := NewAvroSchema(exampleSchemaDef, nil)
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "httpLookup-avro-topic",
		SubscriptionName:            "sub-1",
		Schema:                      asConsumer,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&unobj)
	assert.Nil(t, err)
	assert.Equal(t, unobj.ID, 100)
	assert.Equal(t, unobj.Name, "pulsar")
	defer consumer.Close()
}

func TestStringSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	ssProducer := NewStringSchema(nil)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  "strTopic",
		Schema: ssProducer,
	})
	assert.Nil(t, err)
	if _, err := producer.Send(context.Background(), &ProducerMessage{
		Value: "hello pulsar",
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	var res *string
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "strTopic",
		SubscriptionName:            "sub-2",
		Schema:                      NewStringSchema(nil),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)

	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, *res, "hello pulsar")

	defer consumer.Close()
}

func TestInt8Schema(t *testing.T) {
	client := createClient()
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  "int8Topic1",
		Schema: NewInt8Schema(nil),
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if _, err := producer.Send(ctx, &ProducerMessage{
		Value: int8(1),
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "int8Topic1",
		SubscriptionName:            "sub-2",
		Schema:                      NewInt8Schema(nil),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)

	var res int8
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, res, int8(1))

	defer consumer.Close()
}

func TestInt16Schema(t *testing.T) {
	client := createClient()
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  "int16Topic",
		Schema: NewInt16Schema(nil),
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if _, err := producer.Send(ctx, &ProducerMessage{
		Value: int16(1),
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "int16Topic",
		SubscriptionName:            "sub-2",
		Schema:                      NewInt16Schema(nil),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)

	var res int16
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, res, int16(1))
	defer consumer.Close()
}

func TestInt32Schema(t *testing.T) {
	client := createClient()
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  "int32Topic1",
		Schema: NewInt32Schema(nil),
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if _, err := producer.Send(ctx, &ProducerMessage{
		Value: int32(1),
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "int32Topic1",
		SubscriptionName:            "sub-2",
		Schema:                      NewInt32Schema(nil),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)

	var res int32
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, res, int32(1))
	defer consumer.Close()
}

func TestInt64Schema(t *testing.T) {
	client := createClient()
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  "int64Topic",
		Schema: NewInt64Schema(nil),
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if _, err := producer.Send(ctx, &ProducerMessage{
		Value: int64(1),
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "int64Topic",
		SubscriptionName:            "sub-2",
		Schema:                      NewInt64Schema(nil),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)

	var res int64
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, res, int64(1))
	defer consumer.Close()
}

func TestFloatSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  "floatTopic",
		Schema: NewFloatSchema(nil),
	})
	assert.Nil(t, err)
	if _, err := producer.Send(context.Background(), &ProducerMessage{
		Value: float32(1),
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "floatTopic",
		SubscriptionName:            "sub-2",
		Schema:                      NewFloatSchema(nil),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)

	var res float32
	msg, err := consumer.Receive(context.Background())
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, res, float32(1))
	defer consumer.Close()
}

func TestDoubleSchema(t *testing.T) {
	client := createClient()
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:  "doubleTopic",
		Schema: NewDoubleSchema(nil),
	})
	assert.Nil(t, err)
	ctx := context.Background()
	if _, err := producer.Send(ctx, &ProducerMessage{
		Value: float64(1),
	}); err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       "doubleTopic",
		SubscriptionName:            "sub-2",
		Schema:                      NewDoubleSchema(nil),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)

	var res float64
	msg, err := consumer.Receive(ctx)
	assert.Nil(t, err)
	err = msg.GetSchemaValue(&res)
	assert.Nil(t, err)
	assert.Equal(t, res, float64(1))
	defer consumer.Close()
}

// TestBytesSchemaOnTypedTopic checks that a BYTES producer and consumer can use a topic that already
// has a typed schema, like the Java client's Schema.BYTES does. BYTES sends no schema information.
func TestBytesSchemaOnTypedTopic(t *testing.T) {
	client := createClient()
	defer client.Close()

	topic := fmt.Sprintf("issue-1600-bytes-on-typed-%d", time.Now().UnixNano())

	// Register a STRING schema on the topic.
	stringProducer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: NewStringSchema(nil),
	})
	require.NoError(t, err)
	_, err = stringProducer.Send(context.Background(), &ProducerMessage{Value: "typed"})
	require.NoError(t, err)
	stringProducer.Close()

	// A BYTES producer must be accepted on the typed topic.
	bytesProducer, err := client.CreateProducer(ProducerOptions{
		Topic:  topic,
		Schema: NewBytesSchema(nil),
	})
	require.NoError(t, err)
	defer bytesProducer.Close()
	_, err = bytesProducer.Send(context.Background(), &ProducerMessage{Value: []byte("raw-bytes")})
	require.NoError(t, err)

	// A BYTES consumer must be accepted on the typed topic.
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            "bytes-sub",
		Schema:                      NewBytesSchema(nil),
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	require.NoError(t, err)
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, want := range []string{"typed", "raw-bytes"} {
		msg, err := consumer.Receive(ctx)
		require.NoError(t, err)
		assert.Equal(t, want, string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func TestIsUntypedSchema(t *testing.T) {
	tests := []struct {
		name   string
		info   *SchemaInfo
		expect bool
	}{
		{"nil", nil, false},
		{"bytes", NewBytesSchema(nil).GetSchemaInfo(), true},
		{"none", &SchemaInfo{Type: NONE}, true},
		{"string", NewStringSchema(nil).GetSchemaInfo(), false},
		{"json", &SchemaInfo{Type: JSON}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, isUntypedSchema(tt.info))
		})
	}
}

// TestDisableMultiSchemaRejectsUntypedOverride checks that a producer with DisableMultiSchema rejects a
// per-message schema of another type even when both schemas have an empty definition, so a BYTES
// message (which carries no schema version) is never batched with a typed one, in either order.
func TestDisableMultiSchemaRejectsUntypedOverride(t *testing.T) {
	client := createClient()
	defer client.Close()

	for _, bytesFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("bytesFirst=%v", bytesFirst), func(t *testing.T) {
			topic := fmt.Sprintf("issue-1600-disable-multi-schema-%d", time.Now().UnixNano())
			producer, err := client.CreateProducer(ProducerOptions{
				Topic:                   topic,
				Schema:                  NewInt32Schema(nil),
				DisableMultiSchema:      true,
				BatchingMaxPublishDelay: time.Minute,
				BatchingMaxMessages:     100,
			})
			require.NoError(t, err)
			defer producer.Close()

			var wg sync.WaitGroup
			var typedErr, bytesErr error
			sendTyped := func() {
				wg.Add(1)
				producer.SendAsync(context.Background(), &ProducerMessage{Value: int32(1)},
					func(_ MessageID, _ *ProducerMessage, err error) { typedErr = err; wg.Done() })
			}
			sendBytes := func() {
				wg.Add(1)
				producer.SendAsync(context.Background(),
					&ProducerMessage{Value: []byte("raw"), Schema: NewBytesSchema(nil)},
					func(_ MessageID, _ *ProducerMessage, err error) { bytesErr = err; wg.Done() })
			}
			if bytesFirst {
				sendBytes()
				sendTyped()
			} else {
				sendTyped()
				sendBytes()
			}
			require.NoError(t, producer.Flush())
			wg.Wait()

			assert.NoError(t, typedErr)
			assert.ErrorIs(t, bytesErr, ErrSchema)
		})
	}
}

func TestSchemaInfoHashIncludesType(t *testing.T) {
	int32Info := NewInt32Schema(nil).GetSchemaInfo()
	bytesInfo := NewBytesSchema(nil).GetSchemaInfo()
	stringInfo := NewStringSchema(nil).GetSchemaInfo()
	assert.Equal(t, int32Info.Schema, bytesInfo.Schema)
	assert.NotEqual(t, int32Info.hash(), bytesInfo.hash())
	assert.NotEqual(t, int32Info.hash(), stringInfo.hash())
	assert.Equal(t, int32Info.hash(), NewInt32Schema(nil).GetSchemaInfo().hash())
}
