//
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
//

package pulsar

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// When the broker closes a consumer (e.g. on topic unload) it does not answer
// acks racing the close, so ack-with-response used to block for the full
// operation timeout before reconnecting.
func TestAckWithResponseSurvivesBrokerClosingConsumer(t *testing.T) {
	topic := newTopicName()

	client, err := NewClient(ClientOptions{
		URL: lookupURL,
	})
	assert.NoError(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{Topic: topic})
	assert.NoError(t, err)
	const numMessages = 10000
	for i := 0; i < numMessages; i++ {
		_, err = producer.Send(context.Background(), &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		})
		assert.NoError(t, err)
	}
	producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            "ack-broker-close",
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
		AckWithResponse:             true,
		ReceiverQueueSize:           1000,
	})
	assert.NoError(t, err)
	defer consumer.Close()

	var wg sync.WaitGroup
	var consumed atomic.Int64
	var slowAcks atomic.Int64
	stop := make(chan struct{})
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				msg, err := consumer.Receive(ctx)
				if err != nil {
					cancel()
					continue
				}
				start := time.Now()
				_ = consumer.AckIDList([]MessageID{msg.ID()})
				latency := time.Since(start)
				cancel()
				if latency > 20*time.Second {
					slowAcks.Add(1)
				}
				consumed.Add(1)
			}
		}()
	}

	time.Sleep(700 * time.Millisecond)
	unloadURL := adminURL + "/admin/v2/persistent/public/default/" + topic + "/unload"
	req, err := http.NewRequest(http.MethodPut, unloadURL, nil)
	assert.NoError(t, err)
	resp, err := (&http.Client{Transport: &http.Transport{Proxy: nil}}).Do(req)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, resp.StatusCode, 200)
	assert.Less(t, resp.StatusCode, 300)
	_ = resp.Body.Close()

	deadline := time.After(25 * time.Second)
	for {
		if consumed.Load() >= numMessages {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("consumed only %d/%d messages within 25s",
				consumed.Load(), numMessages)
		case <-time.After(100 * time.Millisecond):
		}
	}
	close(stop)
	wg.Wait()

	assert.Zero(t, slowAcks.Load(),
		"acks stalled instead of failing fast while the broker closed the consumer")
}
