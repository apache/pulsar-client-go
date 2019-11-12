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
	"sort"
	"sync"
	"testing"
	"time"
)

type nackMockedConsumer struct {
	sync.Mutex

	msgIds []messageID
}

func (nmc *nackMockedConsumer) Redeliver(msgIds []messageID) {
	nmc.Lock()
	if nmc.msgIds == nil {
		nmc.msgIds = msgIds
		sort.Slice(msgIds, func(i, j int) bool {
			return msgIds[i].ledgerID < msgIds[j].entryID
		})
	}
	nmc.Unlock()
}

func TestNacksTracker(t *testing.T) {
	nmc := &nackMockedConsumer{}
	nacks := newNegativeAcksTracker(nmc, 1 * time.Second)

	nacks.Add(&messageID{
		ledgerID:     1,
		entryID:      1,
		batchIdx:     1,
	})

	nacks.Add(&messageID{
		ledgerID:     2,
		entryID:      2,
		batchIdx:     1,
	})

	time.Sleep(2 * time.Second)

	assert.Equal(t, 2, len(nmc.msgIds))
	assert.Equal(t, int64(1), nmc.msgIds[0].ledgerID)
	assert.Equal(t, int64(1), nmc.msgIds[0].entryID)
	assert.Equal(t, int64(2), nmc.msgIds[1].ledgerID)
	assert.Equal(t, int64(2), nmc.msgIds[1].entryID)

	nacks.Close()
}

func TestNacksWithBatchesTracker(t *testing.T) {
	nmc := &nackMockedConsumer{}
	nacks := newNegativeAcksTracker(nmc, 1 * time.Second)

	nacks.Add(&messageID{
		ledgerID:     1,
		entryID:      1,
		batchIdx:     1,
	})

	nacks.Add(&messageID{
		ledgerID:     1,
		entryID:      1,
		batchIdx:     2,
	})

	nacks.Add(&messageID{
		ledgerID:     1,
		entryID:      1,
		batchIdx:     3,
	})

	nacks.Add(&messageID{
		ledgerID:     2,
		entryID:      2,
		batchIdx:     1,
	})

	time.Sleep(2 * time.Second)

	assert.Equal(t, 2, len(nmc.msgIds))
	assert.Equal(t, int64(1), nmc.msgIds[0].ledgerID)
	assert.Equal(t, int64(1), nmc.msgIds[0].entryID)
	assert.Equal(t, int64(2), nmc.msgIds[1].ledgerID)
	assert.Equal(t, int64(2), nmc.msgIds[1].entryID)

	nacks.Close()

	time.Sleep(1 * time.Second)
}