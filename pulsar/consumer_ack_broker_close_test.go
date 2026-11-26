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
	"bytes"
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
// operation timeout before reconnecting. Once the consumer has re-subscribed
// its acks must go through again, even when the pool hands back the connection
// the consumer was closed on.
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
	const numTailMessages = 200
	const tailPrefix = "tail-"
	for i := 0; i < numMessages; i++ {
		_, err = producer.Send(context.Background(), &ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		})
		assert.NoError(t, err)
	}

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
	var tailAcks, tailAckErrs atomic.Int64
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
				ackErr := consumer.AckIDList([]MessageID{msg.ID()})
				latency := time.Since(start)
				cancel()
				if latency > 20*time.Second {
					slowAcks.Add(1)
				}
				// Tail messages sit behind the first numMessages in the
				// backlog, so they are only delivered once the consumer has
				// re-subscribed.
				if bytes.HasPrefix(msg.Payload(), []byte(tailPrefix)) {
					if ackErr != nil {
						tailAckErrs.Add(1)
					} else {
						tailAcks.Add(1)
					}
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

	// The producer reconnects to the new owner on its own. Messages published
	// after the unload land behind the first numMessages, so the consumer only
	// gets them once it has re-subscribed.
	for i := 0; i < numTailMessages; i++ {
		_, err = producer.Send(context.Background(), &ProducerMessage{
			Payload: []byte(fmt.Sprintf("%s%d", tailPrefix, i)),
		})
		assert.NoError(t, err, "tail message %d was not published", i)
	}
	producer.Close()

	deadline := time.After(25 * time.Second)
	timedOut := false
	for (consumed.Load() < numMessages || tailAcks.Load()+tailAckErrs.Load() < numTailMessages) && !timedOut {
		select {
		case <-deadline:
			timedOut = true
		case <-time.After(100 * time.Millisecond):
		}
	}
	// Stop and join the workers even on timeout: Fatalf would skip this,
	// the deferred consumer.Close() makes Receive fail immediately, and the
	// workers would spin for the rest of the test suite.
	close(stop)
	wg.Wait()

	if timedOut {
		t.Fatalf("consumed %d/%d messages and %d/%d tail messages within 25s",
			consumed.Load(), numMessages, tailAcks.Load()+tailAckErrs.Load(), numTailMessages)
	}

	assert.Zero(t, slowAcks.Load(),
		"acks stalled instead of failing fast while the broker closed the consumer")
	assert.Zero(t, tailAckErrs.Load(),
		"acks failed after the consumer re-subscribed on the connection it was closed on")
}
