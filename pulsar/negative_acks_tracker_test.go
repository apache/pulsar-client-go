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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/assert"
)

const testNackDelay = 300 * time.Millisecond

var testNackPrecisionBit = ptr(defaultNackPrecisionBit)

type nackMockedConsumer struct {
	ch     chan messageID
	closed bool
	lock   sync.Mutex
}

func newNackMockedConsumer(nackBackoffPolicy NackBackoffPolicy) *nackMockedConsumer {
	t := &nackMockedConsumer{
		ch: make(chan messageID, 10),
	}
	go func() {
		// since the client ticks at an interval of delay / 3
		// wait another interval to ensure we get all messages
		// And we wait another 100 milliseconds to reduce the flaky
		if nackBackoffPolicy == nil {
			time.Sleep(testNackDelay + 200*time.Millisecond)
		} else {
			time.Sleep(nackBackoffPolicy.Next(1) + 200*time.Millisecond)
		}
		t.lock.Lock()
		defer t.lock.Unlock()
		t.closed = true
		close(t.ch)
	}()
	return t
}

func (nmc *nackMockedConsumer) Redeliver(msgIDs []messageID) {
	nmc.lock.Lock()
	defer nmc.lock.Unlock()
	if nmc.closed {
		return
	}
	for _, id := range msgIDs {
		nmc.ch <- id
	}
}

func sortMessageIDs(msgIDs []messageID) []messageID {
	sort.Slice(msgIDs, func(i, j int) bool {
		return msgIDs[i].ledgerID < msgIDs[j].entryID
	})
	return msgIDs
}

func (nmc *nackMockedConsumer) Wait() <-chan messageID {
	return nmc.ch
}

func TestNacksTracker(t *testing.T) {
	nmc := newNackMockedConsumer(nil)
	nacks := newNegativeAcksTracker(nmc, testNackDelay, nil, log.DefaultNopLogger(), testNackPrecisionBit)

	nacks.Add(&messageID{
		ledgerID: 1,
		entryID:  1,
		batchIdx: 1,
	})

	nacks.Add(&messageID{
		ledgerID: 2,
		entryID:  2,
		batchIdx: 1,
	})

	msgIDs := make([]messageID, 0)
	for id := range nmc.Wait() {
		msgIDs = append(msgIDs, id)
	}
	msgIDs = sortMessageIDs(msgIDs)

	assert.Equal(t, 2, len(msgIDs))
	assert.Equal(t, int64(1), msgIDs[0].ledgerID)
	assert.Equal(t, int64(1), msgIDs[0].entryID)
	assert.Equal(t, int64(2), msgIDs[1].ledgerID)
	assert.Equal(t, int64(2), msgIDs[1].entryID)

	nacks.Close()
	// allow multiple Close without panicing
	nacks.Close()
}

func TestNacksWithBatchesTracker(t *testing.T) {
	nmc := newNackMockedConsumer(nil)
	nacks := newNegativeAcksTracker(nmc, testNackDelay, nil, log.DefaultNopLogger(), testNackPrecisionBit)

	nacks.Add(&messageID{
		ledgerID: 1,
		entryID:  1,
		batchIdx: 1,
	})

	nacks.Add(&messageID{
		ledgerID: 1,
		entryID:  1,
		batchIdx: 2,
	})

	nacks.Add(&messageID{
		ledgerID: 1,
		entryID:  1,
		batchIdx: 3,
	})

	nacks.Add(&messageID{
		ledgerID: 2,
		entryID:  2,
		batchIdx: 1,
	})

	msgIDs := make([]messageID, 0)
	for id := range nmc.Wait() {
		msgIDs = append(msgIDs, id)
	}
	msgIDs = sortMessageIDs(msgIDs)

	assert.Equal(t, 2, len(msgIDs))
	assert.Equal(t, int64(1), msgIDs[0].ledgerID)
	assert.Equal(t, int64(1), msgIDs[0].entryID)
	assert.Equal(t, int64(2), msgIDs[1].ledgerID)
	assert.Equal(t, int64(2), msgIDs[1].entryID)

	nacks.Close()
}

func TestNacksTrackerDrainsAllExpiredBucketsAtOnce(t *testing.T) {
	// The tracker scans on a fixed tick, so every entry that is already due
	// must be redelivered by that single scan. Removing entries while
	// iterating the ordered map used to make the scan skip the bucket that
	// followed the removed one, delaying it by one full tick.
	nacks := newNegativeAcksTracker(newNackMockedConsumer(nil), time.Hour, nil,
		log.DefaultNopLogger(), ptr(int64(0)))
	defer nacks.Close()

	now := time.Now()
	nacks.Lock()
	for i := int64(1); i <= 32; i++ {
		nacks.negativeAcks.Put(now.Add(-time.Duration(i)*time.Millisecond),
			map[ledgerID]*roaring64.Bitmap{
				i: roaring64.BitmapOf(uint64(i)),
			})
	}
	// Not due yet, must be kept in the tracker.
	futureKey := now.Add(time.Minute)
	nacks.negativeAcks.Put(futureKey, map[ledgerID]*roaring64.Bitmap{
		100: roaring64.BitmapOf(100),
	})
	nacks.Unlock()

	msgIDs := nacks.drainExpired(now)

	ledgers := make([]int64, 0, len(msgIDs))
	for _, id := range msgIDs {
		ledgers = append(ledgers, id.ledgerID)
	}
	sort.Slice(ledgers, func(i, j int) bool { return ledgers[i] < ledgers[j] })
	expected := make([]int64, 0, 32)
	for i := int64(1); i <= 32; i++ {
		expected = append(expected, i)
	}
	assert.Equal(t, expected, ledgers)

	nacks.Lock()
	defer nacks.Unlock()
	assert.Equal(t, 1, nacks.negativeAcks.Size())
	_, exists := nacks.negativeAcks.Get(futureKey)
	assert.True(t, exists)
}

func TestNackBackoffTracker(t *testing.T) {
	nmc := newNackMockedConsumer(new(defaultNackBackoffPolicy))
	nacks := newNegativeAcksTracker(nmc, testNackDelay, new(defaultNackBackoffPolicy), log.DefaultNopLogger(),
		testNackPrecisionBit)

	nacks.AddMessage(new(mockMessage1))
	nacks.AddMessage(new(mockMessage2))

	msgIDs := make([]messageID, 0)
	for id := range nmc.Wait() {
		msgIDs = append(msgIDs, id)
	}
	msgIDs = sortMessageIDs(msgIDs)

	assert.Equal(t, 2, len(msgIDs))
	assert.Equal(t, int64(1), msgIDs[0].ledgerID)
	assert.Equal(t, int64(1), msgIDs[0].entryID)
	assert.Equal(t, int64(2), msgIDs[1].ledgerID)
	assert.Equal(t, int64(2), msgIDs[1].entryID)

	nacks.Close()
	// allow multiple Close without panicing
	nacks.Close()
}

type mockMessage1 struct {
	properties map[string]string
}

func (msg *mockMessage1) Topic() string {
	return ""
}

func (msg *mockMessage1) Properties() map[string]string {
	return msg.properties
}

func (msg *mockMessage1) Payload() []byte {
	return nil
}

func (msg *mockMessage1) IsNullValue() bool {
	return false
}

func (msg *mockMessage1) ID() MessageID {
	return &messageID{
		ledgerID: 1,
		entryID:  1,
		batchIdx: 1,
	}
}

func (msg *mockMessage1) PublishTime() time.Time {
	return time.Time{}
}

func (msg *mockMessage1) EventTime() time.Time {
	return time.Time{}
}

func (msg *mockMessage1) Key() string {
	return ""
}

func (msg *mockMessage1) OrderingKey() string {
	return ""
}

func (msg *mockMessage1) RedeliveryCount() uint32 {
	return 0
}

func (msg *mockMessage1) IsReplicated() bool {
	return false
}

func (msg *mockMessage1) GetReplicatedFrom() string {
	return ""
}

func (msg *mockMessage1) GetSchemaValue(_ interface{}) error {
	return nil
}

func (msg *mockMessage1) ProducerName() string {
	return ""
}

func (msg *mockMessage1) SchemaVersion() []byte {
	return nil
}

func (msg *mockMessage1) GetEncryptionContext() *EncryptionContext {
	return &EncryptionContext{}
}

func (msg *mockMessage1) Index() *uint64 {
	return nil
}

func (msg *mockMessage1) BrokerPublishTime() *time.Time {
	return nil
}

type mockMessage2 struct {
	properties map[string]string
}

func (msg *mockMessage2) Topic() string {
	return ""
}

func (msg *mockMessage2) Properties() map[string]string {
	return msg.properties
}

func (msg *mockMessage2) Payload() []byte {
	return nil
}

func (msg *mockMessage2) IsNullValue() bool {
	return false
}

func (msg *mockMessage2) ID() MessageID {
	return &messageID{
		ledgerID: 2,
		entryID:  2,
		batchIdx: 1,
	}
}

func (msg *mockMessage2) PublishTime() time.Time {
	return time.Time{}
}

func (msg *mockMessage2) EventTime() time.Time {
	return time.Time{}
}

func (msg *mockMessage2) Key() string {
	return ""
}

func (msg *mockMessage2) OrderingKey() string {
	return ""
}

func (msg *mockMessage2) RedeliveryCount() uint32 {
	return 0
}

func (msg *mockMessage2) IsReplicated() bool {
	return false
}

func (msg *mockMessage2) GetReplicatedFrom() string {
	return ""
}

func (msg *mockMessage2) GetSchemaValue(_ interface{}) error {
	return nil
}

func (msg *mockMessage2) SchemaVersion() []byte {
	return nil
}

func (msg *mockMessage2) ProducerName() string {
	return ""
}

func (msg *mockMessage2) GetEncryptionContext() *EncryptionContext {
	return &EncryptionContext{}
}

func (msg *mockMessage2) Index() *uint64 {
	return nil
}

func (msg *mockMessage2) BrokerPublishTime() *time.Time {
	return nil
}

// fixedNackBackoffPolicy always returns the same delay, used to simulate a
// NackBackoffPolicy that returns a tiny or zero delay.
type fixedNackBackoffPolicy time.Duration

func (p fixedNackBackoffPolicy) Next(_ uint32) time.Duration {
	return time.Duration(p)
}

func TestNacksTrackerTinyDelayDoesNotPanic(t *testing.T) {
	// A delay below 3ns used to make delay/3 zero, which made time.NewTicker panic.
	for _, delay := range []time.Duration{time.Nanosecond, 2 * time.Nanosecond, 0, -time.Second} {
		t.Run(delay.String(), func(t *testing.T) {
			nmc := newNackMockedConsumer(nil)
			var nacks *negativeAcksTracker
			assert.NotPanics(t, func() {
				nacks = newNegativeAcksTracker(nmc, delay, nil, log.DefaultNopLogger(), testNackPrecisionBit)
			})
			defer nacks.Close()

			nacks.Add(&messageID{ledgerID: 1, entryID: 1})

			select {
			case id := <-nmc.Wait():
				assert.Equal(t, int64(1), id.ledgerID)
				assert.Equal(t, int64(1), id.entryID)
			case <-time.After(5 * time.Second):
				t.Fatal("message was not redelivered")
			}
		})
	}
}

func TestNacksTrackerBackoffReturningZeroDoesNotPanic(t *testing.T) {
	// A NackBackoffPolicy returning 0 (redeliver immediately) used to panic in the constructor.
	for _, policy := range []NackBackoffPolicy{fixedNackBackoffPolicy(0), fixedNackBackoffPolicy(time.Nanosecond)} {
		nmc := newNackMockedConsumer(policy)
		var nacks *negativeAcksTracker
		assert.NotPanics(t, func() {
			nacks = newNegativeAcksTracker(nmc, time.Minute, policy, log.DefaultNopLogger(), testNackPrecisionBit)
		})

		nacks.AddMessage(new(mockMessage1))

		select {
		case id := <-nmc.Wait():
			assert.Equal(t, int64(1), id.ledgerID)
		case <-time.After(5 * time.Second):
			t.Fatal("message was not redelivered")
		}
		nacks.Close()
	}
}
