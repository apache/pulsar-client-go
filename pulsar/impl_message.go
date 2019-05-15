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
	"github.com/golang/protobuf/proto"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
)

type messageId struct {
	ledgerID     int64
	entryID      int64
	batchIdx     int
	partitionIdx int
}

func newMessageId(ledgerID int64, entryID int64, batchIdx int, partitionIdx int) MessageID {
	return &messageId{
		ledgerID:     ledgerID,
		entryID:      entryID,
		batchIdx:     batchIdx,
		partitionIdx: partitionIdx,
	}
}

func (id *messageId) Serialize() []byte {
	msgId := &pb.MessageIdData{
		LedgerId:   proto.Uint64(uint64(id.ledgerID)),
		EntryId:    proto.Uint64(uint64(id.entryID)),
		BatchIndex: proto.Int(id.batchIdx),
		Partition:  proto.Int(id.partitionIdx),
	}
	data, _ := proto.Marshal(msgId)
	return data
}

func deserializeMessageId(data []byte) (MessageID, error) {
	msgId := &pb.MessageIdData{}
	err := proto.Unmarshal(data, msgId)
	if err != nil {
		return nil, err
	} else {
		id := newMessageId(
			int64(msgId.GetLedgerId()),
			int64(msgId.GetEntryId()),
			int(msgId.GetBatchIndex()),
			int(msgId.GetPartition()),
		)
		return id, nil
	}
}

func earliestMessageID() MessageID {
	return newMessageId(-1, -1, -1, -1)
}

const maxLong int64 = 0x7fffffffffffffff

func latestMessageID() MessageID {
	return newMessageId(maxLong, maxLong, -1, -1)
}
