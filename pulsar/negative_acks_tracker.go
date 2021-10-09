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

	"github.com/apache/pulsar-client-go/pulsar/internal"
	log "github.com/apache/pulsar-client-go/pulsar/log"
)

type redeliveryConsumer interface {
	Redeliver(msgIds []messageID)
}

type negativeAcksTracker struct {
	sync.Mutex

	doneOnce sync.Once
	rc       redeliveryConsumer
	log      log.Logger
	msgIds   []messageID
	tw       *internal.TimeWheel
}

const (
	defaultCheckNegativeAcksBatchKey = "negative_acks_check_batch_key"

	negativeAcksbatchSize          = 1024
	checkNegativeAcksBatchinterval = time.Second * 5
)

func newNegativeAcksTracker(rc redeliveryConsumer, logger log.Logger) *negativeAcksTracker {
	tw, _ := internal.NewTimeWheel(time.Second*1, 1024)
	t := &negativeAcksTracker{
		rc:     rc,
		log:    logger,
		msgIds: make([]messageID, 0),
		tw:     tw,
	}

	t.tw.Start()
	t.tw.Add(checkNegativeAcksBatchinterval, defaultCheckNegativeAcksBatchKey, t.checkBatch)

	return t
}

func (t *negativeAcksTracker) Add(msgID messageID, negativeAckDelay time.Duration) {
	// Always clear up the batch index since we want to track the nack
	// for the entire batch
	batchMsgID := messageID{
		ledgerID: msgID.ledgerID,
		entryID:  msgID.entryID,
		batchIdx: 0,
	}

	t.tw.Add(negativeAckDelay, batchMsgID, func() {
		t.Lock()
		t.msgIds = append(t.msgIds, batchMsgID)
		if len(t.msgIds) >= negativeAcksbatchSize {
			t.rc.Redeliver(t.msgIds)
			t.msgIds = make([]messageID, 0)
		}
		t.Unlock()
	})
}

func (t *negativeAcksTracker) Remove(msgID messageID) {
	batchMsgID := messageID{
		ledgerID: msgID.ledgerID,
		entryID:  msgID.entryID,
		batchIdx: 0,
	}

	t.tw.Remove(batchMsgID)
}

func (t *negativeAcksTracker) checkBatch() {
	t.Lock()
	if len(t.msgIds) > 0 {
		t.rc.Redeliver(t.msgIds)
		t.msgIds = make([]messageID, 0)
	}
	t.Unlock()

	t.tw.Add(checkNegativeAcksBatchinterval, defaultCheckNegativeAcksBatchKey, t.checkBatch)
}

func (t *negativeAcksTracker) Close() {
	// allow Close() to be invoked multiple times by consumer_partition to avoid panic
	t.doneOnce.Do(func() {
		t.tw.Stop()
	})
}
