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
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	log "github.com/sirupsen/logrus"

	"github.com/apache/pulsar-client-go/pulsar"
)

// ConsumeArgs define the parameters required by consume
type ConsumeArgs struct {
	Topic               string
	SubscriptionName    string
	ReceiverQueueSize   int
	EnableBatchIndexAck bool
}

func newConsumerCommand() *cobra.Command {
	consumeArgs := ConsumeArgs{}
	cmd := &cobra.Command{
		Use:   "consume <topic>",
		Short: "Consume from topic",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			stop := stopCh()
			if FlagProfile {
				RunProfiling(stop)
			}
			consumeArgs.Topic = args[0]
			consume(&consumeArgs, stop)
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&consumeArgs.SubscriptionName, "subscription", "s", "sub", "Subscription name")
	flags.IntVarP(&consumeArgs.ReceiverQueueSize, "receiver-queue-size", "r", 1000, "Receiver queue size")
	flags.BoolVar(&consumeArgs.EnableBatchIndexAck, "enable-batch-index-ack", false, "Whether to enable batch index ACK")

	return cmd
}

func consume(consumeArgs *ConsumeArgs, stop <-chan struct{}) {
	b, _ := json.MarshalIndent(clientArgs, "", "  ")
	log.Info("Client config: ", string(b))
	b, _ = json.MarshalIndent(consumeArgs, "", "  ")
	log.Info("Consumer config: ", string(b))

	client, err := NewClient()

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                          consumeArgs.Topic,
		SubscriptionName:               consumeArgs.SubscriptionName,
		EnableBatchIndexAcknowledgment: consumeArgs.EnableBatchIndexAck,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	// keep message stats
	msgReceived := int64(0)
	bytesReceived := int64(0)

	// Print stats of the consume rate
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

	for {
		select {
		case cm, ok := <-consumer.Chan():
			if !ok {
				return
			}
			msgReceived++
			bytesReceived += int64(len(cm.Message.Payload()))
			consumer.Ack(cm.Message)
		case <-tick.C:
			currentMsgReceived := atomic.SwapInt64(&msgReceived, 0)
			currentBytesReceived := atomic.SwapInt64(&bytesReceived, 0)
			msgRate := float64(currentMsgReceived) / float64(10)
			bytesRate := float64(currentBytesReceived) / float64(10)

			log.Infof(`Stats - Consume rate: %6.1f msg/s - %6.1f Mbps`,
				msgRate, bytesRate*8/1024/1024)
		case <-stop:
			return
		}
	}
}
