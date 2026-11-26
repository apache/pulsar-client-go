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
	"testing"
	"time"

	plog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOriginMessageIDPropertyConstants(t *testing.T) {
	assert.Equal(t, "ORIGIN_MESSAGE_ID", PropertyOriginMessageID)
	// SysPropertyOriginMessageID is deprecated but keeps its legacy value, because
	// older clients still look the origin id up under this key.
	assert.Equal(t, "ORIGIN_MESSAGE_IDY_TIME", SysPropertyOriginMessageID)
}

func TestReconsumeLaterWritesStandardAndLegacyOriginMessageID(t *testing.T) {
	rlq := &retryRouter{messageCh: make(chan RetryMessage, 1)}
	c := &consumer{
		topic: "persistent://public/default/origin-id",
		dlq:   &dlqRouter{policy: &DLQPolicy{MaxDeliveries: 3}, messageCh: make(chan ConsumerMessage, 1)},
		rlq:   rlq,
		log:   plog.DefaultNopLogger(),
	}
	c.consumers.Store([]*partitionConsumer{{}})

	msgID := newTrackingMessageID(10, 20, -1, 0, 0, nil)
	c.ReconsumeLater(&message{
		topic:      "persistent://public/default/origin-id",
		msgID:      msgID,
		properties: map[string]string{},
	}, time.Second)

	var rm RetryMessage
	select {
	case rm = <-rlq.Chan():
	default:
		require.FailNow(t, "ReconsumeLater did not route the message to the retry letter topic")
	}

	props := rm.producerMsg.Properties
	assert.Equal(t, msgID.messageID.String(), props[PropertyOriginMessageID])
	// older clients read the legacy key, so it must still be written
	assert.Equal(t, msgID.messageID.String(), props["ORIGIN_MESSAGE_IDY_TIME"])
	assert.Equal(t, "1", props[SysPropertyReconsumeTimes])
}
