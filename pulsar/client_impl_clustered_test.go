//go:build clustered

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

	"github.com/stretchr/testify/suite"
)

type clientClusteredTestSuite struct {
	suite.Suite
}

func ClientClusteredTestSuite(t *testing.T) {
	suite.Run(t, new(clientClusteredTestSuite))
}

func (suite *clientClusteredTestSuite) TestRetryWithMultipleHosts() {
	req := suite.req
	// Multi hosts included an unreached port and the actual port for verify retry logic
	client, err := NewClient(ClientOptions{
		URL: "pulsar://broker-1:6600,broker-1:6650",
	})
	req.NoError(err)
	defer client.Close()

	topic := "persistent://public/default/retry-multiple-hosts-" + generateRandomName()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})
	req.NoError(err)
	defer producer.Close()

	ctx := context.Background()
	var msgIDs [][]byte

	for i := 0; i < 10; i++ {
		if msgID, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			req.NoError(err)
		} else {
			req.NotNil(msgID)
			msgIDs = append(msgIDs, msgID.Serialize())
		}
	}

	req.Equal(10, len(msgIDs))

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            "retry-multi-hosts-sub",
		Type:                        Shared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	req.NoError(err)
	defer consumer.Close()

	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		req.NoError(err)
		req.Contains(msgIDs, msg.ID().Serialize())
		consumer.Ack(msg)
	}

	err = consumer.Unsubscribe()
	req.NoError(err)
}
