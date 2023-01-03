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
			Value: fmt.Sprintf(valuePrefix + key),
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
