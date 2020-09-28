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
	"math"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type timeSector map[messageID]interface{}

type unackedMessageTracker struct {
	sync.Mutex
	tick *time.Ticker

	// Consumer delegate trackingMessageID's acker to do Redelivery
	sectors      []timeSector
	msgID2Acker  map[messageID]acker
	msgID2Sector map[messageID]timeSector

	doneCh chan interface{}
}

func newUnackedMessageTracker(ackTimeout time.Duration) *unackedMessageTracker {
	t := &unackedMessageTracker{
		tick:         time.NewTicker(minAckTimeoutTickTime),
		msgID2Sector: make(map[messageID]timeSector),
		msgID2Acker:  make(map[messageID]acker),
		doneCh:       make(chan interface{}),
	}
	n := int(math.Ceil(float64(ackTimeout) / float64(minAckTimeoutTickTime)))
	for i := 0; i <= n; i++ {
		t.sectors = append(t.sectors, make(timeSector))
	}

	go t.track()
	return t
}

func (t *unackedMessageTracker) add(msgID trackingMessageID) {
	// do not add each item in batch message into tracker
	mid := msgID.messageID
	mid.batchIdx = 0
	t.Lock()
	defer t.Unlock()

	tailSector := t.sectors[len(t.sectors)-1]
	if _, ok := t.msgID2Sector[mid]; !ok {
		tailSector[mid] = nil
		t.msgID2Acker[mid] = msgID.consumer
		t.msgID2Sector[mid] = tailSector
	}
}

func (t *unackedMessageTracker) remove(msgID messageID) {
	msgID.batchIdx = 0
	t.Lock()
	defer t.Unlock()
	if sector, ok := t.msgID2Sector[msgID]; ok {
		delete(sector, msgID)
		delete(t.msgID2Acker, msgID)
		delete(t.msgID2Sector, msgID)
	}
}

func (t *unackedMessageTracker) track() {
	for {
		select {
		case <-t.doneCh:
			log.Debug("Closing unacked tracker")
			return
		case <-t.tick.C:
			t.Lock()

			headSector := t.sectors[0]
			t.sectors = t.sectors[1:] // dequeue
			n := len(headSector)
			msgIDs := make([]messageID, 0, n)
			if n > 0 {
				log.Warnf("%d messages have timed-out", n)
				for msgID := range headSector {
					msgIDs = append(msgIDs, msgID)
					delete(t.msgID2Sector, msgID)
				}
			}
			headSector = make(map[messageID]interface{})
			t.sectors = append(t.sectors, headSector) // enqueue

			t.Unlock()

			if len(msgIDs) > 0 {
				acker2ids := make(map[acker][]messageID)
				t.Lock()
				for _, id := range msgIDs {
					if acker, ok := t.msgID2Acker[id]; ok {
						acker2ids[acker] = append(acker2ids[acker], id)
						delete(t.msgID2Acker, id)
					}
				}
				t.Unlock()

				for acker, ids := range acker2ids {
					acker.Redeliver(ids)
				}
			}
		}
	}
}

func (t *unackedMessageTracker) Close() {
	t.doneCh <- nil
}

func (t *unackedMessageTracker) size() int {
	t.Lock()
	defer t.Unlock()
	return len(t.msgID2Sector)
}
