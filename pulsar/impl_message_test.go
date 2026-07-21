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

	"github.com/bits-and-blooms/bitset"
	"github.com/stretchr/testify/assert"
)

func TestMessageId(t *testing.T) {
	id := newMessageID(1, 2, 3, 4, 5)
	bytes := id.Serialize()

	id2, err := DeserializeMessageID(bytes)
	assert.NoError(t, err)
	assert.NotNil(t, id2)

	assert.Equal(t, int64(1), id2.(*messageID).ledgerID)
	assert.Equal(t, int64(2), id2.(*messageID).entryID)
	assert.Equal(t, int32(3), id2.(*messageID).batchIdx)
	assert.Equal(t, int32(4), id2.(*messageID).partitionIdx)
	assert.Equal(t, int32(5), id2.(*messageID).batchSize)

	id, err = DeserializeMessageID(nil)
	assert.Error(t, err)
	assert.Nil(t, id)

	id, err = DeserializeMessageID(make([]byte, 0))
	assert.Error(t, err)
	assert.Nil(t, id)
}

func TestMessageIdGetFuncs(t *testing.T) {
	// test LedgerId,EntryId,BatchIdx,PartitionIdx
	id := newMessageID(1, 2, 3, 4, 5)
	assert.Equal(t, int64(1), id.LedgerID())
	assert.Equal(t, int64(2), id.EntryID())
	assert.Equal(t, int32(3), id.BatchIdx())
	assert.Equal(t, int32(4), id.PartitionIdx())
	assert.Equal(t, int32(5), id.BatchSize())
}

func TestAckTracker(t *testing.T) {
	tracker := newAckTracker(1, nil)
	assert.Equal(t, true, tracker.ack(0))

	// test 64
	tracker = newAckTracker(64, nil)
	for i := 0; i < 64; i++ {
		if i < 63 {
			assert.Equal(t, false, tracker.ack(i))
		} else {
			assert.Equal(t, true, tracker.ack(i))
		}
	}
	assert.Equal(t, true, tracker.completed())

	// test large number 1000
	tracker = newAckTracker(1000, nil)
	for i := 0; i < 1000; i++ {
		if i < 999 {
			assert.Equal(t, false, tracker.ack(i))
		} else {
			assert.Equal(t, true, tracker.ack(i))
		}

	}
	assert.Equal(t, true, tracker.completed())

	tracker = newAckTracker(1000, nil)
	for i := 0; i < 1000; i++ {
		if i < 999 {
			assert.False(t, tracker.ackCumulative(i))
			assert.False(t, tracker.completed())
		} else {
			assert.True(t, tracker.ackCumulative(i))
			assert.True(t, tracker.completed())
		}
	}

	// test large number 1000 cumulative
	tracker = newAckTracker(1000, nil)

	assert.Equal(t, true, tracker.ackCumulative(999))
	assert.Equal(t, true, tracker.completed())
}

func TestAckTrackerWithBatchIDs(t *testing.T) {
	// batchIDs mirrors the ack set the broker attaches when redelivering a partially-acked
	// batch: bits 1 and 2 set means only indexes 1 and 2 are still outstanding.
	newOutstanding12 := func() *bitset.BitSet {
		batchIDs := bitset.New(5)
		batchIDs.Set(1)
		batchIDs.Set(2)
		return batchIDs
	}

	tracker := newAckTracker(5, newOutstanding12())
	assert.False(t, tracker.completed())
	ackBitSet := tracker.getAckBitSet()
	assert.Equal(t, uint(2), ackBitSet.Count())
	assert.True(t, ackBitSet.Test(1))
	assert.True(t, ackBitSet.Test(2))

	// acking an index the broker already considers acked must not complete the tracker
	assert.False(t, tracker.ack(0))
	// acking the outstanding indexes completes the tracker
	assert.False(t, tracker.ack(1))
	assert.True(t, tracker.ack(2))
	assert.True(t, tracker.completed())
	assert.Nil(t, tracker.toAckSet())

	// the tracker must own a copy: mutating the caller's bitset must not leak into it
	batchIDs := newOutstanding12()
	tracker = newAckTracker(5, batchIDs)
	batchIDs.Set(3)
	assert.False(t, tracker.ack(1))
	assert.True(t, tracker.ack(2))
	assert.True(t, tracker.completed())

	// cumulative ack covering the outstanding indexes completes the tracker
	tracker = newAckTracker(5, newOutstanding12())
	assert.True(t, tracker.ackCumulative(2))
	assert.True(t, tracker.completed())

	// nil batchIDs keeps the previous behavior: every index starts outstanding
	tracker = newAckTracker(5, nil)
	assert.Equal(t, uint(5), tracker.getAckBitSet().Count())
	assert.False(t, tracker.completed())
}

func TestAckingMessageIDBatchOne(t *testing.T) {
	tracker := newAckTracker(1, nil)
	msgID := newTrackingMessageID(1, 1, 0, 0, 0, tracker)
	assert.Equal(t, true, msgID.ack())
	assert.Equal(t, true, tracker.completed())
}

func TestAckingMessageIDBatchTwo(t *testing.T) {
	tracker := newAckTracker(2, nil)
	ids := []*trackingMessageID{
		newTrackingMessageID(1, 1, 0, 0, 0, tracker),
		newTrackingMessageID(1, 1, 1, 0, 0, tracker),
	}

	assert.Equal(t, false, ids[0].ack())
	assert.Equal(t, true, ids[1].ack())
	assert.Equal(t, true, tracker.completed())

	// try reverse order
	tracker = newAckTracker(2, nil)
	ids = []*trackingMessageID{
		newTrackingMessageID(1, 1, 0, 0, 0, tracker),
		newTrackingMessageID(1, 1, 1, 0, 0, tracker),
	}
	assert.Equal(t, false, ids[1].ack())
	assert.Equal(t, true, ids[0].ack())
	assert.Equal(t, true, tracker.completed())
}

func TestMessageStringOnMessage(t *testing.T) {
	id := newMessageID(1, 2, -1, 4, 0)

	assert.Equal(t, "1:2:4", id.String())
}

func TestMessageStringOnBatchMessage(t *testing.T) {
	id := newMessageID(1, 2, 3, 4, 5)

	assert.Equal(t, "1:2:4:3", id.String())
}
