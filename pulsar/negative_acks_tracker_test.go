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

	"github.com/stretchr/testify/assert"

	"github.com/apache/pulsar-client-go/pulsar/log"
)

const testNackDelay = 300 * time.Millisecond

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

func (nmc *nackMockedConsumer) Redeliver(msgIds []messageID) {
	nmc.lock.Lock()
	defer nmc.lock.Unlock()
	if nmc.closed {
		return
	}
	for _, id := range msgIds {
		nmc.ch <- id
	}
}

func sortMessageIds(msgIds []messageID) []messageID {
	sort.Slice(msgIds, func(i, j int) bool {
		return msgIds[i].ledgerID < msgIds[j].entryID
	})
	return msgIds
}

func (nmc *nackMockedConsumer) Wait() <-chan messageID {
	return nmc.ch
}

func TestNacksTracker(t *testing.T) {
	nmc := newNackMockedConsumer(nil)
	nacks := newNegativeAcksTracker(nmc, testNackDelay, nil, log.DefaultNopLogger())

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

	msgIds := make([]messageID, 0)
	for id := range nmc.Wait() {
		msgIds = append(msgIds, id)
	}
	msgIds = sortMessageIds(msgIds)

	assert.Equal(t, 2, len(msgIds))
	assert.Equal(t, int64(1), msgIds[0].ledgerID)
	assert.Equal(t, int64(1), msgIds[0].entryID)
	assert.Equal(t, int64(2), msgIds[1].ledgerID)
	assert.Equal(t, int64(2), msgIds[1].entryID)

	nacks.Close()
	// allow multiple Close without panicing
	nacks.Close()
}

func TestNacksWithBatchesTracker(t *testing.T) {
	nmc := newNackMockedConsumer(nil)
	nacks := newNegativeAcksTracker(nmc, testNackDelay, nil, log.DefaultNopLogger())

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

	msgIds := make([]messageID, 0)
	for id := range nmc.Wait() {
		msgIds = append(msgIds, id)
	}
	msgIds = sortMessageIds(msgIds)

	assert.Equal(t, 2, len(msgIds))
	assert.Equal(t, int64(1), msgIds[0].ledgerID)
	assert.Equal(t, int64(1), msgIds[0].entryID)
	assert.Equal(t, int64(2), msgIds[1].ledgerID)
	assert.Equal(t, int64(2), msgIds[1].entryID)

	nacks.Close()
}

func TestNackBackoffTracker(t *testing.T) {
	nmc := newNackMockedConsumer(new(defaultNackBackoffPolicy))
	nacks := newNegativeAcksTracker(nmc, testNackDelay, new(defaultNackBackoffPolicy), log.DefaultNopLogger())

	nacks.AddMessage(new(mockMessage1))
	nacks.AddMessage(new(mockMessage2))

	msgIds := make([]messageID, 0)
	for id := range nmc.Wait() {
		msgIds = append(msgIds, id)
	}
	msgIds = sortMessageIds(msgIds)

	assert.Equal(t, 2, len(msgIds))
	assert.Equal(t, int64(1), msgIds[0].ledgerID)
	assert.Equal(t, int64(1), msgIds[0].entryID)
	assert.Equal(t, int64(2), msgIds[1].ledgerID)
	assert.Equal(t, int64(2), msgIds[1].entryID)

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

func (msg *mockMessage1) GetSchemaValue(v interface{}) error {
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

func (msg *mockMessage2) GetSchemaValue(v interface{}) error {
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
