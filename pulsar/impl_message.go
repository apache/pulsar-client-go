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
	"github.com/apache/pulsar-client-go/pkg/pb"
	"github.com/golang/protobuf/proto"
)

type messageID struct {
	ledgerID     int64
	entryID      int64
	batchIdx     int
	partitionIdx int
}

func newMessageID(ledgerID int64, entryID int64, batchIdx int, partitionIdx int) MessageID {
	return &messageID{
		ledgerID:     ledgerID,
		entryID:      entryID,
		batchIdx:     batchIdx,
		partitionIdx: partitionIdx,
	}
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

func earliestMessageID() MessageID {
	return newMessageID(-1, -1, -1, -1)
}

const maxLong int64 = 0x7fffffffffffffff

func latestMessageID() MessageID {
	return newMessageID(maxLong, maxLong, -1, -1)
}
