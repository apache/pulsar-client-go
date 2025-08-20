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
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/emirpasic/gods/trees/avltree"
	log "github.com/apache/pulsar-client-go/pulsar/log"
)

type redeliveryConsumer interface {
	Redeliver(msgIDs []messageID)
}

type LedgerId = int64

type negativeAcksTracker struct {
	sync.Mutex

	doneCh       chan interface{}
	doneOnce     sync.Once
	negativeAcks *avltree.Tree
	nackPrecisionBit int64
	rc           redeliveryConsumer
	nackBackoff  NackBackoffPolicy
	tick         *time.Ticker
	delay        time.Duration
	log          log.Logger
}

func newNegativeAcksTracker(rc redeliveryConsumer, delay time.Duration,
	nackBackoffPolicy NackBackoffPolicy, logger log.Logger, nackPrecisionBit int64) *negativeAcksTracker {

	t := &negativeAcksTracker{
		doneCh:       make(chan interface{}),
		negativeAcks: avltree.NewWith(func(a, b interface{}) int {
			// compare time.Time
			timeA := a.(time.Time)
			timeB := b.(time.Time)
			if timeA.Before(timeB) {
				return -1
			}
			if timeA.After(timeB) {
				return 1
			}
			return 0
    	}),
		rc:           rc,
		nackBackoff:  nackBackoffPolicy,
		log:          logger,
		nackPrecisionBit: nackPrecisionBit,
	}

	if nackBackoffPolicy != nil {
		firstDelayForNackBackoff := nackBackoffPolicy.Next(1)
		t.delay = firstDelayForNackBackoff
	} else {
		t.delay = delay
	}

	t.tick = time.NewTicker(t.delay / 3)

	go t.track()
	return t
}

func trimLowerBit(ts int64, precisionBit int64) int64 {
	if precisionBit <= 0 {
		return ts
	}
	mask := ^((int64(1) << precisionBit) - 1)
	return ts & mask
}

func (t *negativeAcksTracker) Add(msgID *messageID) {
	// Always clear up the batch index since we want to track the nack
	// for the entire batch
	batchMsgID := messageID{
		ledgerID: msgID.ledgerID,
		entryID:  msgID.entryID,
		batchIdx: 0,
	}

	t.Lock()
	defer t.Unlock()

	targetTime := time.Now().Add(t.delay)
	trimmedTime := time.UnixMilli(trimLowerBit(targetTime.UnixMilli(), t.nackPrecisionBit))
	// try get trimmedTime
	value, exists := t.negativeAcks.Get(trimmedTime)
	if !exists {
		newMap := make(map[LedgerId]*roaring64.Bitmap)
		t.negativeAcks.Put(trimmedTime, newMap)
		value = newMap
	}
	bitmapMap := value.(map[LedgerId]*roaring64.Bitmap)
	if _, exists := bitmapMap[batchMsgID.ledgerID]; !exists {
		bitmapMap[batchMsgID.ledgerID] = roaring64.NewBitmap()
	}
	bitmapMap[batchMsgID.ledgerID].Add(uint64(batchMsgID.entryID))
}

func (t *negativeAcksTracker) AddMessage(msg Message) {
	nackBackoffDelay := t.nackBackoff.Next(msg.RedeliveryCount())

	msgID := msg.ID()

	// Always clear up the batch index since we want to track the nack
	// for the entire batch
	batchMsgID := messageID{
		ledgerID: msgID.LedgerID(),
		entryID:  msgID.EntryID(),
		batchIdx: 0,
	}

	t.Lock()
	defer t.Unlock()

	targetTime := time.Now().Add(nackBackoffDelay)
	trimmedTime := time.UnixMilli(trimLowerBit(targetTime.UnixMilli(), t.nackPrecisionBit))
	// try get trimmedTime
	value, exists := t.negativeAcks.Get(trimmedTime)
	if !exists {
		newMap := make(map[LedgerId]*roaring64.Bitmap)
		t.negativeAcks.Put(trimmedTime, newMap)
		value = newMap
	}
	bitmapMap := value.(map[LedgerId]*roaring64.Bitmap)
	if _, exists := bitmapMap[batchMsgID.ledgerID]; !exists {
		bitmapMap[batchMsgID.ledgerID] = roaring64.NewBitmap()
	}
	bitmapMap[batchMsgID.ledgerID].Add(uint64(batchMsgID.entryID))
}

func (t *negativeAcksTracker) track() {
	for {
		select {
		case <-t.doneCh:
			t.log.Debug("Closing nack tracker")
			return

		case <-t.tick.C:
			{
				now := time.Now()
				msgIDs := make([]messageID, 0)

				t.Lock()

				iterator := t.negativeAcks.Iterator()
				for iterator.Next() {
					targetTime := iterator.Key().(time.Time)
					// because use ordered map, so we can early break 
					if targetTime.After(now) {
						break
					}

					ledgerMap := iterator.Value().(map[LedgerId]*roaring64.Bitmap)
					for ledgerID, entrySet := range ledgerMap {
						for _, entryID := range entrySet.ToArray() {
							msgID := messageID{
								ledgerID: ledgerID,
								entryID:  int64(entryID),
								batchIdx: 0,
							}
							msgIDs = append(msgIDs, msgID)
						}
					}

					// Safe deletion during iteration
					t.negativeAcks.Remove(targetTime)
				}

				t.Unlock()

				if len(msgIDs) > 0 {
					t.rc.Redeliver(msgIDs)
				}
			}
		}
	}
}

func (t *negativeAcksTracker) Close() {
	// allow Close() to be invoked multiple times by consumer_partition to avoid panic
	t.doneOnce.Do(func() {
		t.tick.Stop()
		t.doneCh <- nil
	})
}
