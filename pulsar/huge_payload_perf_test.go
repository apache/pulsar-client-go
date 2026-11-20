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
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/stretchr/testify/require"
)

const localServiceURL = "pulsar://localhost:6650"

type PerfMetrics struct {
	PayloadKB          int
	NumMessages        int
	IngestionMBps      float64
	IngestionMsgsSec   float64
	ConsumptionMBps    float64
	ConsumptionMsgsSec float64
}

// TestHugePayloadRates measures both Ingestion Rate (Producer) and Consumption Throughput (Consumer)
// with huge payloads against a live local Pulsar broker.
func TestHugePayloadRates(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large payload throughput test in short mode")
	}

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

// TestConsumerStuckUntilPodRestart reproduces the production scenario where
// continuous streaming with large payloads triggers memory-pressure queue shrinking,
// and verifies that flow-control permits do not underflow, allowing all messages to be consumed.
func TestConsumerStuckUntilPodRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping live broker test in short mode")
	}

	client, err := NewClient(ClientOptions{
		URL:              localServiceURL,
		MemoryLimitBytes: 16 * 1024 * 1024, // 16 MB memory limit to trigger queue shrinking quickly
	})
	if err != nil {
		t.Skipf("Skipping live broker test: %v", err)
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
		ReceiverQueueSize:                 20,
		EnableAutoScaledReceiverQueueSize: true,
	})
	require.NoError(t, err)
	defer consumer.Close()

	payloadSize := 1024 * 1024 // 1 MB
	payload := make([]byte, payloadSize)
	totalMessages := 30

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
				time.Sleep(2 * time.Millisecond)
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
	t.Logf("Total consumed: %d/%d messages", consumed, totalMessages)
	require.Equal(t, totalMessages, consumed, "consumer stalled before receiving all messages")
}
