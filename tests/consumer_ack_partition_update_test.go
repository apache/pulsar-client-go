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

package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerAckIDListDuringPartitionUpdate(t *testing.T) {
	hook := newBlockingFailureInjectHook()

	topic := "persistent://public/default/ack-update-" + uuid.NewString()
	topicName, err := utils.GetTopicName(topic)
	require.NoError(t, err)
	var lastCreateErr error
	created := assert.Eventually(t, func() bool {
		lastCreateErr = testPulsar.Admin.Topics().Create(*topicName, 1)
		return lastCreateErr == nil
	}, 60*time.Second, time.Second)
	require.Truef(t, created, "last error: %v", lastCreateErr)
	require.NoError(t, lastCreateErr)

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               testPulsar.BrokerURL,
		OperationTimeout:  30 * time.Second,
		FailureInjectHook: hook,
	})
	require.NoError(t, err)
	defer client.Close()

	var consumer pulsar.Consumer
	require.Eventually(t, func() bool {
		consumer, err = client.Subscribe(pulsar.ConsumerOptions{
			Topic:               topic,
			SubscriptionName:    "sub",
			AutoDiscoveryPeriod: time.Millisecond,
		})
		return err == nil
	}, 30*time.Second, time.Second)
	defer consumer.Close()

	require.Eventually(t, func() bool {
		err = testPulsar.Admin.Topics().Update(*topicName, 2)
		return err == nil
	}, 30*time.Second, time.Second)
	require.NoError(t, err)

	hook.waitUntilBlocked(t)

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	require.NoError(t, err)
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 20; i++ {
		_, err = producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	var msg pulsar.Message
	for {
		msg, err = consumer.Receive(ctx)
		require.NoError(t, err)
		if msg.ID().PartitionIdx() == 1 {
			break
		}
	}

	timer := time.AfterFunc(time.Second, hook.release)
	defer timer.Stop()

	require.NotPanics(t, func() {
		err = consumer.AckIDList([]pulsar.MessageID{msg.ID()})
	})
	require.NoError(t, err)
}
