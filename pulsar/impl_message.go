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
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/apache/pulsar-client-go/pkg/pb"
)

type messageID struct {
	ledgerID     int64
	entryID      int64
	batchIdx     int
	partitionIdx int

	tracker  *ackTracker
	consumer acker
}

func (id *messageID) Ack() {
	if id.consumer == nil {
		return
	}
	if id.ack() {
		id.consumer.AckID(id)
	}
}

func (id *messageID) Nack() {
	if id.consumer == nil {
		return
	}
	id.consumer.NackID(id)
}

func (id *messageID) ack() bool {
	if id.tracker != nil && id.batchIdx > -1 {
		return id.tracker.ack(id.batchIdx)
	}
	return true
}

func (id *messageID) Serialize() []byte {
	msgID := &pb.MessageIdData{
		LedgerId:   proto.Uint64(uint64(id.ledgerID)),
		EntryId:    proto.Uint64(uint64(id.entryID)),
		BatchIndex: proto.Int(id.batchIdx),
		Partition:  proto.Int(id.partitionIdx),
	}
	data, _ := proto.Marshal(msgID)
	return data
}

func deserializeMessageID(data []byte) (MessageID, error) {
	msgID := &pb.MessageIdData{}
	err := proto.Unmarshal(data, msgID)
	if err != nil {
		return nil, err
	}
	id := newMessageID(
		int64(msgID.GetLedgerId()),
		int64(msgID.GetEntryId()),
		int(msgID.GetBatchIndex()),
		int(msgID.GetPartition()),
	)
	return id, nil
}

func newMessageID(ledgerID int64, entryID int64, batchIdx int, partitionIdx int) MessageID {
	return &messageID{
		ledgerID:     ledgerID,
		entryID:      entryID,
		batchIdx:     batchIdx,
		partitionIdx: partitionIdx,
	}
}

func newTrackingMessageID(ledgerID int64, entryID int64, batchIdx int, partitionIdx int,
	tracker *ackTracker) *messageID {
	return &messageID{
		ledgerID:     ledgerID,
		entryID:      entryID,
		batchIdx:     batchIdx,
		partitionIdx: partitionIdx,
		tracker:      tracker,
	}
}

func timeFromUnixTimestampMillis(timestamp uint64) time.Time {
	ts := int64(timestamp) * int64(time.Millisecond)
	seconds := ts / int64(time.Second)
	nanos := ts - (seconds * int64(time.Second))
	return time.Unix(seconds, nanos)
}

func timeToUnixTimestampMillis(t time.Time) uint64 {
	nanos := t.UnixNano()
	millis := nanos / int64(time.Millisecond)
	return uint64(millis)
}

type message struct {
	publishTime time.Time
	eventTime   time.Time
	key         string
	payLoad     []byte
	msgID       MessageID
	properties  map[string]string
	topic       string
}

func (msg *message) Topic() string {
	return msg.topic
}

func (msg *message) Properties() map[string]string {
	return msg.properties
}

func (msg *message) Payload() []byte {
	return msg.payLoad
}

func (msg *message) ID() MessageID {
	return msg.msgID
}

func (msg *message) PublishTime() time.Time {
	return msg.publishTime
}

func (msg *message) EventTime() time.Time {
	return msg.eventTime
}

func (msg *message) Key() string {
	return msg.key
}

func newAckTracker(size int) *ackTracker {
	var batchIDs *big.Int
	if size <= 64 {
		shift := uint32(64 - size)
		setBits := ^uint64(0) >> shift
		batchIDs = new(big.Int).SetUint64(setBits)
	} else {
		batchIDs, _ = new(big.Int).SetString(strings.Repeat("1", size), 2)
	}
	return &ackTracker{
		size:     size,
		batchIDs: batchIDs,
	}
}

type ackTracker struct {
	sync.Mutex
	size     int
	batchIDs *big.Int
}

func (t *ackTracker) ack(batchID int) bool {
	if batchID < 0 {
		return true
	}
	t.Lock()
	defer t.Unlock()
	t.batchIDs = t.batchIDs.SetBit(t.batchIDs, batchID, 0)
	return len(t.batchIDs.Bits()) == 0
}

func (t *ackTracker) completed() bool {
	t.Lock()
	defer t.Unlock()
	return len(t.batchIDs.Bits()) == 0
}
