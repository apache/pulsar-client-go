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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessageId(t *testing.T) {
	id := newMessageID(1, 2, 3, 4)
	bytes := id.Serialize()

	id2, err := DeserializeMessageID(bytes)
	assert.NoError(t, err)
	assert.NotNil(t, id2)

	assert.Equal(t, int64(1), id2.(messageID).ledgerID)
	assert.Equal(t, int64(2), id2.(messageID).entryID)
	assert.Equal(t, int32(3), id2.(messageID).batchIdx)
	assert.Equal(t, int32(4), id2.(messageID).partitionIdx)

	assert.Equal(t, int64(1), id2.LedgerId())
	assert.Equal(t, int64(2), id2.EntryId())
	assert.Equal(t, int32(3), id2.BatchIdx())
	assert.Equal(t, int32(4), id2.PartitionIdx())

	id, err = DeserializeMessageID(nil)
	assert.Error(t, err)
	assert.Nil(t, id)

	id, err = DeserializeMessageID(make([]byte, 0))
	assert.Error(t, err)
	assert.Nil(t, id)
}

func TestAckTracker(t *testing.T) {
	tracker := newAckTracker(1)
	assert.Equal(t, true, tracker.ack(0))

	// test 64
	tracker = newAckTracker(64)
	for i := 0; i < 64; i++ {
		if i < 63 {
			assert.Equal(t, false, tracker.ack(i))
		} else {
			assert.Equal(t, true, tracker.ack(i))
		}
	}
	assert.Equal(t, true, tracker.completed())

	// test large number 1000
	tracker = newAckTracker(1000)
	for i := 0; i < 1000; i++ {
		if i < 999 {
			assert.Equal(t, false, tracker.ack(i))
		} else {
			assert.Equal(t, true, tracker.ack(i))
		}

	}
	assert.Equal(t, true, tracker.completed())
}

func TestAckingMessageIDBatchOne(t *testing.T) {
	tracker := newAckTracker(1)
	msgID := newTrackingMessageID(1, 1, 0, 0, tracker)
	assert.Equal(t, true, msgID.ack())
	assert.Equal(t, true, tracker.completed())
}

func TestAckingMessageIDBatchTwo(t *testing.T) {
	tracker := newAckTracker(2)
	ids := []trackingMessageID{
		newTrackingMessageID(1, 1, 0, 0, tracker),
		newTrackingMessageID(1, 1, 1, 0, tracker),
	}

	assert.Equal(t, false, ids[0].ack())
	assert.Equal(t, true, ids[1].ack())
	assert.Equal(t, true, tracker.completed())

	// try reverse order
	tracker = newAckTracker(2)
	ids = []trackingMessageID{
		newTrackingMessageID(1, 1, 0, 0, tracker),
		newTrackingMessageID(1, 1, 1, 0, tracker),
	}
	assert.Equal(t, false, ids[1].ack())
	assert.Equal(t, true, ids[0].ack())
	assert.Equal(t, true, tracker.completed())
}
