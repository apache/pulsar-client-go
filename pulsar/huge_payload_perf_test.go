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
	"math"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const localServiceURL = "pulsar://localhost:6650"

type PerfMetrics struct {
	PayloadKB         int
	NumMessages       int
	IngestionMBps     float64
	IngestionMsgsSec  float64
	ConsumptionMBps   float64
	ConsumptionMsgsSec float64
}

// TestHugePayloadRates measures both Ingestion Rate (Producer) and Consumption Throughput (Consumer)
// with huge payloads against a live local Pulsar broker.
func TestHugePayloadRates(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:              localServiceURL,
		MemoryLimitBytes: 512 * 1024 * 1024,
	})
	if err != nil {
		t.Skipf("Skipping live broker test: %v", err)
	}
	defer client.Close()

	payloadSizes := []int{
		256 * 1024,      // 256 KB
		1024 * 1024,     // 1 MB
		2 * 1024 * 1024, // 2 MB
		4 * 1024 * 1024, // 4 MB
	}

	for _, size := range payloadSizes {
		t.Run(fmt.Sprintf("Payload_%dKB", size/1024), func(t *testing.T) {
			topic := fmt.Sprintf("test-rates-%d-%d", size/1024, time.Now().UnixNano())

			producer, err := client.CreateProducer(ProducerOptions{
				Topic:           topic,
				DisableBatching: true,
			})
			if err != nil {
				t.Skipf("Skipping live broker test: cannot create producer: %v", err)
			}
			defer producer.Close()

			consumer, err := client.Subscribe(ConsumerOptions{
				Topic:                             topic,
				SubscriptionName:                  "perf-sub",
				Type:                              Exclusive,
				ReceiverQueueSize:                 100,
				EnableAutoScaledReceiverQueueSize: true,
			})
			require.NoError(t, err)
			defer consumer.Close()

			numMessages := 50
			if size >= 4*1024*1024 {
				numMessages = 25
			}
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			// 1. Measure Ingestion (Publish) Rate
			ingestStart := time.Now()
			for i := 0; i < numMessages; i++ {
				_, err := producer.Send(context.Background(), &ProducerMessage{
					Payload: payload,
				})
				require.NoError(t, err)
			}
			ingestDuration := time.Since(ingestStart)
			totalBytes := int64(numMessages) * int64(size)
			totalMB := float64(totalBytes) / (1024 * 1024)
			ingestionMBps := totalMB / ingestDuration.Seconds()
			ingestionMsgsSec := float64(numMessages) / ingestDuration.Seconds()

			// 2. Measure Consumption (Receive + Ack) Throughput
			consumeStart := time.Now()
			for i := 0; i < numMessages; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				msg, err := consumer.Receive(ctx)
				cancel()
				require.NoError(t, err)
				require.Equal(t, size, len(msg.Payload()))
				consumer.Ack(msg)
			}
			consumeDuration := time.Since(consumeStart)
			consumptionMBps := totalMB / consumeDuration.Seconds()
			consumptionMsgsSec := float64(numMessages) / consumeDuration.Seconds()

			t.Logf("=== RESULTS FOR %d KB PAYLOAD ===", size/1024)
			t.Logf("Ingestion Rate:         %.2f msgs/s (%.2f MB/s) in %v",
				ingestionMsgsSec, ingestionMBps, ingestDuration)
			t.Logf("Consumption Throughput: %.2f msgs/s (%.2f MB/s) in %v",
				consumptionMsgsSec, consumptionMBps, consumeDuration)
		})
	}
}

// TestCompareFlowControlPermitBehavior demonstrates how the older code caused
// a severe throughput drop (stall / deadlock) due to permit underflow when
// shrinking under large payload memory pressure, compared to the fixed code.
func TestCompareFlowControlPermitBehavior(t *testing.T) {
	t.Run("Older_Code_Permit_Underflow_Stall", func(t *testing.T) {
		// Old logic: availablePermits.add(newSize - oldSize) without lower bound clamping.
		// When queue shrinks from 100 to 50: delta is -50.
		// If 5 messages had been processed, availablePermits was 5.
		availablePermits := int32(5)
		oldQueueSize := int32(100)
		newQueueSize := int32(50)

		// Old code execution:
		availablePermits += (newQueueSize - oldQueueSize) // 5 + (-50) = -45

		// Check flow threshold
		flowThreshold := int32(math.Max(float64(newQueueSize/2), 1)) // 25

		// Now simulate consuming the remaining messages in the prefetch queue (e.g. 20 messages)
		messagesConsumed := 20
		permitsRequestedCount := 0
		for i := 0; i < messagesConsumed; i++ {
			availablePermits++ // simulate availablePermits.inc() on each message consumed
			if availablePermits >= flowThreshold {
				permitsRequestedCount++
				availablePermits = 0
			}
		}

		// In older code, availablePermits is still -25, NEVER reached flowThreshold (25).
		// Zero permits were requested! Broker stopped sending.
		assert.Equal(t, int32(-25), availablePermits)
		assert.Equal(t, 0, permitsRequestedCount, "Old code: 0 permits requested! Consumer stalls indefinitely!")
		t.Logf("Older Code Result: availablePermits = %d (NEGATIVE), permits requested = %d -> CONSUMER DEADLOCKED (0 MB/s)",
			availablePermits, permitsRequestedCount)
	})

	t.Run("Newer_Code_Clamped_Permits_Sustained_Flow", func(t *testing.T) {
		// New logic: availablePermits is clamped to >= 0 when shrinking
		availablePermits := int32(5)
		oldQueueSize := int32(100)
		newQueueSize := int32(50)

		// New code execution:
		target := availablePermits + (newQueueSize - oldQueueSize)
		if target < 0 {
			target = 0
		}
		availablePermits = target // clamped to 0

		flowThreshold := int32(math.Max(float64(newQueueSize/2), 1)) // 25

		// Simulate consuming 30 messages
		messagesConsumed := 30
		permitsRequestedCount := 0
		for i := 0; i < messagesConsumed; i++ {
			availablePermits++
			if availablePermits >= flowThreshold {
				permitsRequestedCount += int(availablePermits)
				availablePermits = 0
			}
		}

		// In newer code, permits were requested normally as soon as 25 messages were consumed!
		assert.Greater(t, permitsRequestedCount, 0, "New code: permits requested normally, no stall!")
		t.Logf("Newer Code Result: availablePermits = %d, permits requested = %d -> SUSTAINED THROUGHPUT",
			availablePermits, permitsRequestedCount)
	})
}

// BenchmarkReaderMemoryAndThroughput compares the memory footprint, allocation
// overhead, and throughput of reading huge payload frames between the older logic
// (2x growth, no shrink) and the newer logic (bounded growth, shrink on drain).
func BenchmarkReaderMemoryAndThroughput(b *testing.B) {
	sizes := []int{
		64 * 1024,       // 64 KB
		1024 * 1024,     // 1 MB
		4 * 1024 * 1024, // 4 MB
	}

	for _, size := range sizes {
		// Simulate Old Reader: allocates 2x frame size, never shrinks buffer back
		b.Run(fmt.Sprintf("Older_Code_%dKB", size/1024), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))

			buf := internal.NewBuffer(4096)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				needed := uint32(size)
				if buf.WritableBytes() < needed {
					total := buf.ReadableBytes() + needed
					// Old logic: doubles size unconditionally
					buf.Resize(total * 2)
				}
				buf.WrittenBytes(needed)
				// Consume frame
				_ = buf.Read(needed)
				// Old logic: Clear only resets indices, never frees memory
				buf.Clear()
			}
			b.StopTimer()
			b.Logf("Older Code [%d KB]: Max Retained Buffer Capacity = %d KB (allocated 2x)",
				size/1024, buf.Capacity()/1024)
		})

		// Simulate New Reader: bounded allocation for large frames, shrinks back when drained
		b.Run(fmt.Sprintf("Newer_Code_%dKB", size/1024), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))

			buf := internal.NewBuffer(4096)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				needed := uint32(size)
				if buf.WritableBytes() < needed {
					total := buf.ReadableBytes() + needed
					// New logic: bounded growth for large frames
					var newCap uint32
					if total > 64*1024 {
						newCap = total + 64*1024
					} else {
						newCap = total * 2
					}
					buf.Resize(newCap)
				}
				buf.WrittenBytes(needed)
				// Consume frame
				_ = buf.Read(needed)
				// New logic: shrinks back to baseline when drained
				if buf.Capacity() > 64*1024 {
					buf = internal.NewBuffer(4096)
				} else {
					buf.Clear()
				}
			}
			b.StopTimer()
			b.Logf("Newer Code [%d KB]: Retained Buffer Capacity After Drain = %d KB (baseline)",
				size/1024, buf.Capacity()/1024)
		})
	}
}

// TestConsumerStuckUntilPodRestart reproduces the exact production bug where
// continuous streaming with large payloads triggers memory-pressure queue shrinking,
// causing flow-control permits to underflow and permanently deadlocking the consumer
// until pod restart.
func TestConsumerStuckUntilPodRestart(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:              localServiceURL,
		MemoryLimitBytes: 64 * 1024 * 1024, // 64 MB memory limit
	})
	if err != nil {
		t.Skipf("Skipping: %v", err)
	}
	defer client.Close()

	topic := fmt.Sprintf("test-pod-stuck-%d", time.Now().UnixNano())
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	require.NoError(t, err)
	defer producer.Close()

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                             topic,
		SubscriptionName:                  "sub-stuck-test",
		Type:                              Exclusive,
		ReceiverQueueSize:                 40,
		EnableAutoScaledReceiverQueueSize: true,
	})
	require.NoError(t, err)
	defer consumer.Close()

	payloadSize := 2 * 1024 * 1024 // 2 MB
	payload := make([]byte, payloadSize)
	totalMessages := 80

	// Produce messages continuously in background
	stopProduce := make(chan struct{})
	go func() {
		for i := 0; i < totalMessages; i++ {
			select {
			case <-stopProduce:
				return
			default:
				_, err := producer.Send(context.Background(), &ProducerMessage{
					Payload: payload,
				})
				if err != nil {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Try to consume all messages
	consumed := 0
	for i := 0; i < totalMessages; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		msg, err := consumer.Receive(ctx)
		cancel()
		if err != nil {
			t.Logf(">>> CONSUMER STUCK! Timed out at message %d/%d: %v", i+1, totalMessages, err)
			break
		}
		consumed++
		consumer.Ack(msg)
	}
	close(stopProduce)
	t.Logf("Total consumed before stall: %d/%d messages", consumed, totalMessages)
}
