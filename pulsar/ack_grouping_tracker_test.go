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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/stretchr/testify/assert"
)

func TestNoCacheTracker(t *testing.T) {
	tests := []AckGroupingOptions{
		{
			MaxSize: 0,
			MaxTime: 10 * time.Hour,
		},
		{
			MaxSize: 1,
			MaxTime: 10 * time.Hour,
		},
	}
	for _, option := range tests {
		t.Run(fmt.Sprintf("TestAckImmediately_size_%v_time_%vs", option.MaxSize, option.MaxTime.Seconds()),
			func(t *testing.T) {
				ledgerID0 := int64(-1)
				ledgerID1 := int64(-1)
				tracker := newAckGroupingTracker(&option,
					func(id MessageID) { ledgerID0 = id.LedgerID() },
					func(id MessageID) { ledgerID1 = id.LedgerID() },
					nil)

				tracker.add(&messageID{ledgerID: 1})
				assert.Equal(t, atomic.LoadInt64(&ledgerID0), int64(1))
				tracker.addCumulative(&messageID{ledgerID: 2})
				assert.Equal(t, atomic.LoadInt64(&ledgerID1), int64(2))
			})
	}
}

type mockAcker struct {
	cumulativeLedgerID int64
	sync.Mutex
	ledgerIDs []int64
}

func (a *mockAcker) ack(ids []*pb.MessageIdData) {
	defer a.Unlock()
	a.Lock()
	for _, id := range ids {
		a.ledgerIDs = append(a.ledgerIDs, int64(*id.LedgerId))
	}
}

func (a *mockAcker) ackCumulative(id MessageID) {
	atomic.StoreInt64(&a.cumulativeLedgerID, id.LedgerID())
}

func (a *mockAcker) getLedgerIDs() []int64 {
	defer a.Unlock()
	a.Lock()

	sort.Slice(a.ledgerIDs, func(i, j int) bool { return a.ledgerIDs[i] < a.ledgerIDs[j] })
	return a.ledgerIDs
}

func (a *mockAcker) getCumulativeLedgerID() int64 {
	return atomic.LoadInt64(&a.cumulativeLedgerID)
}

func (a *mockAcker) reset() {
	a.ledgerIDs = make([]int64, 0)
	a.cumulativeLedgerID = int64(0)
}

func TestCachedTracker(t *testing.T) {
	var acker mockAcker
	tracker := newAckGroupingTracker(&AckGroupingOptions{MaxSize: 3, MaxTime: 0}, nil,
		func(id MessageID) { acker.ackCumulative(id) }, func(ids []*pb.MessageIdData) { acker.ack(ids) })

	tracker.add(&messageID{ledgerID: 1})
	tracker.add(&messageID{ledgerID: 2})
	for i := 1; i <= 2; i++ {
		assert.True(t, tracker.isDuplicate(&messageID{ledgerID: int64(i)}))
	}
	assert.Equal(t, 0, len(acker.getLedgerIDs()))
	tracker.add(&messageID{ledgerID: 3})
	assert.Eventually(t, func() bool { return len(acker.getLedgerIDs()) > 0 },
		10*time.Millisecond, 2*time.Millisecond)
	assert.Equal(t, []int64{1, 2, 3}, acker.getLedgerIDs())
	for i := 1; i <= 3; i++ {
		assert.False(t, tracker.isDuplicate(&messageID{ledgerID: int64(i)}))
	}

	tracker.add(&messageID{ledgerID: 4})
	// 4 won't be added because the cache is not full
	assert.Equal(t, []int64{1, 2, 3}, acker.getLedgerIDs())

	assert.False(t, tracker.isDuplicate(&messageID{ledgerID: 5}))
	tracker.addCumulative(&messageID{ledgerID: 5})
	for i := 0; i <= 5; i++ {
		assert.True(t, tracker.isDuplicate(&messageID{ledgerID: int64(i)}))
	}
	assert.Equal(t, int64(5), acker.getCumulativeLedgerID())
	assert.False(t, tracker.isDuplicate(&messageID{ledgerID: int64(6)}))

	tracker.flush()
	assert.Eventually(t, func() bool { return len(acker.getLedgerIDs()) > 3 },
		10*time.Millisecond, 2*time.Millisecond)
	assert.Equal(t, []int64{1, 2, 3, 4}, acker.getLedgerIDs())
}

func TestTimedTrackerIndividualAck(t *testing.T) {
	var acker mockAcker
	// MaxSize: 1000, MaxTime: 100ms
	tracker := newAckGroupingTracker(nil, nil,
		func(id MessageID) { acker.ackCumulative(id) }, func(ids []*pb.MessageIdData) { acker.ack(ids) })

	expected := make([]int64, 0)
	for i := 0; i < 999; i++ {
		tracker.add(&messageID{ledgerID: int64(i)})
		expected = append(expected, int64(i))
	}
	assert.Equal(t, 0, len(acker.getLedgerIDs()))

	// case 1: flush because the tracker timed out
	assert.Eventually(t, func() bool { return len(acker.getLedgerIDs()) == 999 },
		150*time.Millisecond, 10*time.Millisecond)
	assert.Equal(t, expected, acker.getLedgerIDs())

	// case 2: flush because cache is full
	time.Sleep(50) // see case 3
	acker.reset()
	expected = append(expected, 999)
	for i := 0; i < 1001; i++ {
		tracker.add(&messageID{ledgerID: int64(i)})
	}
	assert.Equal(t, expected, acker.getLedgerIDs())

	// case 3: flush will reset the timer
	start := time.Now()
	assert.Eventually(t, func() bool { return len(acker.getLedgerIDs()) > 1000 },
		150*time.Millisecond, 10*time.Millisecond)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, int64(100), "elapsed", elapsed)
	assert.Equal(t, append(expected, 1000), acker.getLedgerIDs())
}

func TestTimedTrackerCumulativeAck(t *testing.T) {
	var acker mockAcker
	// MaxTime is 100ms
	tracker := newAckGroupingTracker(nil, nil, func(id MessageID) { acker.ackCumulative(id) }, nil)

	// case 1: flush because of the timeout
	tracker.addCumulative(&messageID{ledgerID: 1})
	assert.NotEqual(t, int64(1), acker.getCumulativeLedgerID())
	assert.Eventually(t, func() bool { return acker.getCumulativeLedgerID() == int64(1) },
		150*time.Millisecond, 10*time.Millisecond)
	assert.Equal(t, int64(1), acker.getCumulativeLedgerID())

	// case 2: flush manually
	tracker.addCumulative(&messageID{ledgerID: 2})
	tracker.flush()
	assert.Equal(t, int64(2), acker.getCumulativeLedgerID())

	// case 3: older MessageID cannot be acknowledged
	tracker.addCumulative(&messageID{ledgerID: 1})
	tracker.flush()
	assert.Equal(t, int64(2), acker.getCumulativeLedgerID())
}

func TestTimedTrackerIsDuplicate(t *testing.T) {
	tracker := newAckGroupingTracker(nil, func(id MessageID) {}, func(id MessageID) {},
		func(id []*pb.MessageIdData) {})

	tracker.add(&messageID{batchIdx: 0, batchSize: 3})
	tracker.add(&messageID{batchIdx: 2, batchSize: 3})
	assert.True(t, tracker.isDuplicate(&messageID{batchIdx: 0, batchSize: 3}))
	assert.False(t, tracker.isDuplicate(&messageID{batchIdx: 1, batchSize: 3}))
	assert.True(t, tracker.isDuplicate(&messageID{batchIdx: 2, batchSize: 3}))

	tracker.flush()
	assert.False(t, tracker.isDuplicate(&messageID{batchIdx: 0, batchSize: 3}))
	assert.False(t, tracker.isDuplicate(&messageID{batchIdx: 1, batchSize: 3}))
	assert.False(t, tracker.isDuplicate(&messageID{batchIdx: 2, batchSize: 3}))
}

func TestDuplicateAfterClose(t *testing.T) {
	var acker mockAcker
	tracker := newAckGroupingTracker(&AckGroupingOptions{MaxSize: 3, MaxTime: 0}, nil,
		func(id MessageID) { acker.ackCumulative(id) }, func(ids []*pb.MessageIdData) { acker.ack(ids) })

	tracker.add(&messageID{ledgerID: 1})
	assert.True(t, tracker.isDuplicate(&messageID{ledgerID: 1}))

	tracker.close()
	assert.False(t, tracker.isDuplicate(&messageID{ledgerID: 1}))
}

func TestCloseFlushWithoutTimer(t *testing.T) {
	var acker mockAcker
	tracker := newAckGroupingTracker(
		&AckGroupingOptions{MaxSize: 3, MaxTime: 0},
		nil,
		func(id MessageID) { acker.ackCumulative(id) },
		func(ids []*pb.MessageIdData) { acker.ack(ids) },
	)

	// case 1: message will not be acked because the cache is not full
	tracker.add(&messageID{ledgerID: 1})
	tracker.add(&messageID{ledgerID: 2})
	assert.True(t, tracker.isDuplicate(&messageID{ledgerID: int64(1)}))
	assert.True(t, tracker.isDuplicate(&messageID{ledgerID: int64(2)}))
	assert.Equal(t, 0, len(acker.getLedgerIDs()))

	// case 2: tracker close so that all messages are flushed and acked
	tracker.close()
	assert.False(t, tracker.isDuplicate(&messageID{ledgerID: 1}))
	assert.False(t, tracker.isDuplicate(&messageID{ledgerID: 2}))
	assert.Equal(t, []int64{1, 2}, acker.getLedgerIDs())
}

func TestCloseFlushWithTimer(t *testing.T) {
	var acker mockAcker
	tracker := newAckGroupingTracker(
		&AckGroupingOptions{MaxSize: 1000, MaxTime: 10 * time.Second},
		nil,
		func(id MessageID) { acker.ackCumulative(id) },
		func(ids []*pb.MessageIdData) { acker.ack(ids) },
	)

	// case 1: messages are not acked because the cache is not full
	tracker.add(&messageID{ledgerID: 1})
	tracker.add(&messageID{ledgerID: 2})
	assert.True(t, tracker.isDuplicate(&messageID{ledgerID: int64(1)}))
	assert.True(t, tracker.isDuplicate(&messageID{ledgerID: int64(2)}))
	assert.Equal(t, 0, len(acker.getLedgerIDs()))

	// case 2: tracker close so that all messages are flushed and acked
	tracker.close()
	assert.False(t, tracker.isDuplicate(&messageID{ledgerID: 1}))
	assert.False(t, tracker.isDuplicate(&messageID{ledgerID: 2}))
	assert.Equal(t, []int64{1, 2}, acker.getLedgerIDs())
}

func TestTrackerPendingAcks(t *testing.T) {
	m := make(map[uint64][]int64)
	tracker := newAckGroupingTracker(&AckGroupingOptions{MaxSize: 3, MaxTime: 0}, nil, nil,
		func(ids []*pb.MessageIdData) {
			for _, id := range ids {
				m[*id.LedgerId] = id.AckSet
			}
		})
	tracker.add(&messageID{ledgerID: 0, batchIdx: 0, batchSize: 30})
	for i := 0; i < 10; i++ {
		tracker.add(&messageID{ledgerID: 1, batchIdx: int32(i), batchSize: 10})
	}
	assert.Equal(t, 0, len(m)) // the number of entries is 2, so it's not flushed
	tracker.flush()
	assert.Equal(t, 2, len(m))

	ackSet, found := m[0]
	assert.True(t, found)
	assert.Greater(t, len(ackSet), 0)

	ackSet, found = m[1]
	assert.True(t, found)
	assert.Equal(t, 0, len(ackSet)) // all messages in the batch are acknowledged
}
