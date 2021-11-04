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

	log "github.com/apache/pulsar-client-go/pulsar/log"
)

type redeliveryConsumer interface {
	Redeliver(msgIds []messageID)
}

type negativeAcksTracker struct {
	sync.Mutex

	doneCh       chan interface{}
	doneOnce     sync.Once
	negativeAcks map[messageID]time.Time
	rc           redeliveryConsumer
	nackBackoff  NackBackoffPolicy
	trackFlag    bool
	delay        time.Duration
	log          log.Logger
}

func newNegativeAcksTracker(rc redeliveryConsumer, delay time.Duration,
	nackBackoffPolicy NackBackoffPolicy, logger log.Logger) *negativeAcksTracker {

	t := new(negativeAcksTracker)

	// When using NackBackoffPolicy, the delay time needs to be calculated based on the RedeliveryCount field in
	// the CommandMessage, so for the original default Nack() logic, we still keep the negativeAcksTracker created
	// when we open a gorutine to execute the logic of `t.track()`. But for the NackBackoffPolicy method, we need
	// to execute the logic of `t.track()` when AddMessage().
	if nackBackoffPolicy != nil {
		t = &negativeAcksTracker{
			doneCh:       make(chan interface{}),
			negativeAcks: make(map[messageID]time.Time),
			nackBackoff:  nackBackoffPolicy,
			rc:           rc,
			log:          logger,
		}
	} else {
		t = &negativeAcksTracker{
			doneCh:       make(chan interface{}),
			negativeAcks: make(map[messageID]time.Time),
			rc:           rc,
			nackBackoff:  nil,
			delay:        delay,
			log:          logger,
		}

		go t.track(time.NewTicker(t.delay / 3))
	}
	return t
}

func (t *negativeAcksTracker) Add(msgID messageID) {
	// Always clear up the batch index since we want to track the nack
	// for the entire batch
	batchMsgID := messageID{
		ledgerID: msgID.ledgerID,
		entryID:  msgID.entryID,
		batchIdx: 0,
	}

	t.Lock()
	defer t.Unlock()

	_, present := t.negativeAcks[batchMsgID]
	if present {
		// The batch is already being tracked
		return
	}

	targetTime := time.Now().Add(t.delay)
	t.negativeAcks[batchMsgID] = targetTime
}

func (t *negativeAcksTracker) AddMessage(msg Message) {
	nackBackoffDelay := t.nackBackoff.Next(msg.RedeliveryCount())
	t.delay = time.Duration(nackBackoffDelay)

	// Use trackFlag to avoid opening a new gorutine to execute `t.track()` every AddMessage.
	// In fact, we only need to execute it once.
	if !t.trackFlag {
		go t.track(time.NewTicker(t.delay / 3))
		t.trackFlag = true
	}

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

	_, present := t.negativeAcks[batchMsgID]
	if present {
		// The batch is already being tracked
		return
	}

	targetTime := time.Now().Add(time.Duration(nackBackoffDelay))
	t.negativeAcks[batchMsgID] = targetTime
}

func (t *negativeAcksTracker) track(ticker *time.Ticker) {
	for {
		select {
		case <-t.doneCh:
			t.log.Debug("Closing nack tracker")
			return

		case <-ticker.C:
			{
				now := time.Now()
				msgIds := make([]messageID, 0)

				t.Lock()

				for msgID, targetTime := range t.negativeAcks {
					t.log.Debugf("MsgId: %v -- targetTime: %v -- now: %v", msgID, targetTime, now)
					if targetTime.Before(now) {
						t.log.Debugf("Adding MsgId: %v", msgID)
						msgIds = append(msgIds, msgID)
						delete(t.negativeAcks, msgID)
					}
				}

				t.Unlock()

				if len(msgIds) > 0 {
					t.rc.Redeliver(msgIds)
				}
			}
		}
	}
}

func (t *negativeAcksTracker) Close() {
	// allow Close() to be invoked multiple times by consumer_partition to avoid panic
	t.doneOnce.Do(func() {
		t.doneCh <- nil
	})
}
