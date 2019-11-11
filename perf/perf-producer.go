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

package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/beefsack/go-rate"
	"github.com/bmizerany/perks/quantile"
	"github.com/spf13/cobra"

	log "github.com/sirupsen/logrus"

	"github.com/apache/pulsar-client-go/pulsar"
)

// ProduceArgs define the parameters required by produce
type ProduceArgs struct {
	Topic              string
	Rate               int
	BatchingTimeMillis int
	MessageSize        int
	ProducerQueueSize  int
}

func newProducerCommand() *cobra.Command {
	produceArgs := ProduceArgs{}
	cmd := &cobra.Command{
		Use:   "produce ",
		Short: "Produce on a topic and measure performance",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			stop := stopCh()
			if FlagProfile {
				RunProfiling(stop)
			}
			produceArgs.Topic = args[0]
			produce(&produceArgs, stop)
		},
	}

	// add flags
	flags := cmd.Flags()
	flags.IntVarP(&produceArgs.Rate, "rate", "r", 100,
		"Publish rate. Set to 0 to go unthrottled")
	flags.IntVarP(&produceArgs.BatchingTimeMillis, "batching-time", "b", 1,
		"Batching grouping time in millis")
	flags.IntVarP(&produceArgs.MessageSize, "size", "s", 1024,
		"Message size")
	flags.IntVarP(&produceArgs.ProducerQueueSize, "queue-size", "q", 1000,
		"Produce queue size")

	return cmd
}

func produce(produceArgs *ProduceArgs, stop <-chan struct{}) {
	b, _ := json.MarshalIndent(clientArgs, "", "  ")
	log.Info("Client config: ", string(b))
	b, _ = json.MarshalIndent(produceArgs, "", "  ")
	log.Info("Producer config: ", string(b))

	client, err := NewClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   produceArgs.Topic,
		MaxPendingMessages:      produceArgs.ProducerQueueSize,
		BatchingMaxPublishDelay: time.Millisecond * time.Duration(produceArgs.BatchingTimeMillis),
		SendTimeout:             0,
		BlockIfQueueFull:        true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	ctx := context.Background()

	payload := make([]byte, produceArgs.MessageSize)

	ch := make(chan float64)

	go func() {
		var rateLimiter *rate.RateLimiter
		if produceArgs.Rate > 0 {
			rateLimiter = rate.New(produceArgs.Rate, time.Second)
		}

		for {
			select {
			case <-stop:
				return
			default:
			}

			if rateLimiter != nil {
				rateLimiter.Wait()
			}

			start := time.Now()

			producer.SendAsync(ctx, &pulsar.ProducerMessage{
				Payload: payload,
			}, func(msgID pulsar.MessageID, message *pulsar.ProducerMessage, e error) {
				if e != nil {
					log.WithError(e).Fatal("Failed to publish")
				}

				latency := time.Since(start).Seconds()
				ch <- latency
			})
		}
	}()

	// Print stats of the publish rate and latencies
	tick := time.NewTicker(10 * time.Second)
	q := quantile.NewTargeted(0.90, 0.95, 0.99, 0.999, 1.0)
	messagesPublished := 0

	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			messageRate := float64(messagesPublished) / float64(10)
			log.Infof(`Stats - Publish rate: %6.1f msg/s - %6.1f Mbps - Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f`,
				messageRate,
				messageRate*float64(produceArgs.MessageSize)/1024/1024*8,
				q.Query(0.5)*1000,
				q.Query(0.95)*1000,
				q.Query(0.99)*1000,
				q.Query(0.999)*1000,
				q.Query(1.0)*1000,
			)

			q.Reset()
			messagesPublished = 0
		case latency := <-ch:
			messagesPublished++
			q.Insert(latency)
		}
	}
}
