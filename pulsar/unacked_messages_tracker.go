//
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
//

package pulsar

import (
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal/pb"
	"github.com/golang/protobuf/proto"

	set "github.com/deckarep/golang-set"
	log "github.com/sirupsen/logrus"
)

type UnackedMessageTracker struct {
	cmu        sync.RWMutex // protects following
	currentSet set.Set
	oldOpenSet set.Set
	timeout    *time.Ticker

	pc  *partitionConsumer
	pcs []*partitionConsumer
}

func NewUnackedMessageTracker() *UnackedMessageTracker {
	unAckTracker := &UnackedMessageTracker{
		currentSet: set.NewSet(),
		oldOpenSet: set.NewSet(),
	}

	return unAckTracker
}

func (t *UnackedMessageTracker) Size() int {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	return t.currentSet.Cardinality() + t.oldOpenSet.Cardinality()
}

func (t *UnackedMessageTracker) IsEmpty() bool {
	t.cmu.RLock()
	defer t.cmu.RUnlock()

	return t.currentSet.Cardinality() == 0 && t.oldOpenSet.Cardinality() == 0
}

func (t *UnackedMessageTracker) Add(id *pb.MessageIdData) bool {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	t.oldOpenSet.Remove(id)
	return t.currentSet.Add(id)
}

func (t *UnackedMessageTracker) Remove(id *pb.MessageIdData) {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	t.currentSet.Remove(id)
	t.oldOpenSet.Remove(id)
}

func (t *UnackedMessageTracker) clear() {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	t.currentSet.Clear()
	t.oldOpenSet.Clear()
}

func (t *UnackedMessageTracker) toggle() {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	t.currentSet, t.oldOpenSet = t.oldOpenSet, t.currentSet
}

func (t *UnackedMessageTracker) isAckTimeout() bool {
	t.cmu.RLock()
	defer t.cmu.RUnlock()

	return !(t.oldOpenSet.Cardinality() == 0)
}

func (t *UnackedMessageTracker) lessThanOrEqual(id1, id2 pb.MessageIdData) bool {
	return id1.GetPartition() == id2.GetPartition() &&
		(id1.GetLedgerId() < id2.GetLedgerId() || id1.GetEntryId() <= id2.GetEntryId())
}

func (t *UnackedMessageTracker) RemoveMessagesTill(id pb.MessageIdData) int {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	counter := 0

	t.currentSet.Each(func(elem interface{}) bool {
		if t.lessThanOrEqual(elem.(pb.MessageIdData), id) {
			t.currentSet.Remove(elem)
			counter++
		}
		return true
	})

	t.oldOpenSet.Each(func(elem interface{}) bool {
		if t.lessThanOrEqual(elem.(pb.MessageIdData), id) {
			t.currentSet.Remove(elem)
			counter++
		}
		return true
	})

	return counter
}

func (t *UnackedMessageTracker) Start(ackTimeoutMillis int64) {
	t.cmu.Lock()
	defer t.cmu.Unlock()
	t.timeout = time.NewTicker((time.Duration(ackTimeoutMillis)) * time.Millisecond)

	go t.handlerCmd()
}

func (t *UnackedMessageTracker) handlerCmd() {
	for {
		select {
		case <-t.timeout.C:
			if t.isAckTimeout() {
				log.Debugf(" %d messages have timed-out", t.oldOpenSet.Cardinality())
				messageIds := make([]*pb.MessageIdData, 0)

				t.oldOpenSet.Each(func(i interface{}) bool {
					messageIds = append(messageIds, i.(*pb.MessageIdData))
					return false
				})

				log.Debugf("messageID length is:%d", len(messageIds))

				t.oldOpenSet.Clear()

				if t.pc != nil {
					requestID := t.pc.client.rpcClient.NewRequestID()
					cmd := &pb.CommandRedeliverUnacknowledgedMessages{
						ConsumerId: proto.Uint64(t.pc.consumerID),
						MessageIds: messageIds,
					}

					_, err := t.pc.client.rpcClient.RequestOnCnx(t.pc.conn, requestID,
						pb.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES, cmd)
					if err != nil {
						t.pc.log.WithError(err).Error("Failed to unsubscribe consumer")
						return
					}

					log.Debugf("consumer:%v redeliver messages num:%d", t.pc.options.consumerName, len(messageIds))
				} else if t.pcs != nil {
					messageIdsMap := make(map[int32][]*pb.MessageIdData)
					for _, msgID := range messageIds {
						messageIdsMap[msgID.GetPartition()] = append(messageIdsMap[msgID.GetPartition()], msgID)
					}

					for index, subConsumer := range t.pcs {
						if messageIdsMap[int32(index)] != nil {
							requestID := subConsumer.client.rpcClient.NewRequestID()
							cmd := &pb.CommandRedeliverUnacknowledgedMessages{
								ConsumerId: proto.Uint64(subConsumer.consumerID),
								MessageIds: messageIdsMap[int32(index)],
							}

							_, err := subConsumer.client.rpcClient.RequestOnCnx(subConsumer.conn, requestID,
								pb.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES, cmd)
							if err != nil {
								subConsumer.log.WithError(err).Error("Failed to unsubscribe consumer")
								return
							}
						}
					}
				}
			}
		}
		t.toggle()
	}
}

func (t *UnackedMessageTracker) Stop() {
	if t.timeout != nil {
		t.timeout.Stop()
	}
	log.Debug("stop ticker ", t.timeout)

	t.clear()
}
