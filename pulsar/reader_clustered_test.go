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

type ReaderClusteredTestSuite struct {
	suite.Suite
}

func TestReaderClusteredTestSuite(t *testing.T) {
	suite.Run(t, new(ReaderClusteredTestSuite))
}

func (suite *ReaderClusteredTestSuite) TestReaderWithMultipleHosts() {
	req := suite.Require()

	// Multi hosts included an unreached port and the actual port for verify retry logic
	client, err := NewClient(ClientOptions{
		URL: "pulsar://broker-1:6600,broker-1:6650",
	})
	req.NoError(err)
	defer client.Close()

	topic := newTopicName()
	ctx := context.Background()

	// create producer
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	req.NoError(err)
	defer producer.Close()

	// send 10 messages
	for i := 0; i < 10; i++ {
		msgID, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		req.NoError(err)
		req.NotNil(msgID)
	}

	// create reader on 5th message (not included)
	reader, err := client.CreateReader(ReaderOptions{
		Topic:          topic,
		StartMessageID: EarliestMessageID(),
	})
	req.NoError(err)
	defer reader.Close()

	i := 0
	for reader.HasNext() {
		msg, err := reader.Next(context.Background())
		req.NoError(err)

		expectMsg := fmt.Sprintf("hello-%d", i)
		req.Equal([]byte(expectMsg), msg.Payload())

		i++
	}

	req.Equal(10, i)
}
