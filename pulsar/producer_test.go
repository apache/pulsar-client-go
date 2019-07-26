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
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/util"
	"github.com/stretchr/testify/assert"

	log "github.com/sirupsen/logrus"
)

func TestSimpleProducer(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	for i := 0; i < 10; i++ {
		err = producer.Send(context.Background(), &ProducerMessage{
			Payload: []byte("hello"),
		})

		assert.NoError(t, err)
	}

	err = producer.Close()
	assert.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
}

func TestProducerAsyncSend(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic:                   newTopicName(),
		BatchingMaxPublishDelay: 1 * time.Second,
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	wg := sync.WaitGroup{}
	wg.Add(10)
	errors := util.NewBlockingQueue(10)

	for i := 0; i < 10; i++ {
		producer.SendAsync(context.Background(), &ProducerMessage{
			Payload: []byte("hello"),
		}, func(id MessageID, message *ProducerMessage, e error) {
			if e != nil {
				log.WithError(e).Error("Failed to publish")
				errors.Put(e)
			} else {
				log.Info("Published message ", id)
			}
			wg.Done()
		})

		assert.NoError(t, err)
	}

	producer.Flush()

	wg.Wait()

	assert.Equal(t, 0, errors.Size())

	err = producer.Close()
	assert.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
}

func TestProducerCompression(t *testing.T) {

	type testProvider struct {
		name            string
		compressionType CompressionType
	}

	var providers = []testProvider{
		{"zlib", ZLib},
		{"lz4", LZ4},
		{"zstd", ZSTD},
	}

	for _, p := range providers {
		t.Run(p.name, func(t *testing.T) {
			client, err := NewClient(ClientOptions{
				URL: serviceURL,
			})
			assert.NoError(t, err)

			producer, err := client.CreateProducer(ProducerOptions{
				Topic:           newTopicName(),
				CompressionType: p.compressionType,
			})

			assert.NoError(t, err)
			assert.NotNil(t, producer)

			for i := 0; i < 10; i++ {
				err = producer.Send(context.Background(), &ProducerMessage{
					Payload: []byte("hello"),
				})

				assert.NoError(t, err)
			}

			err = producer.Close()
			assert.NoError(t, err)

			err = client.Close()
			assert.NoError(t, err)
		})
	}
}

func TestProducerLastSequenceID(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	assert.Equal(t, int64(-1), producer.LastSequenceID())

	for i := 0; i < 10; i++ {
		err = producer.Send(context.Background(), &ProducerMessage{
			Payload: []byte("hello"),
		})

		assert.NoError(t, err)
		assert.Equal(t, int64(i), producer.LastSequenceID())
	}

	err = producer.Close()
	assert.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
}
