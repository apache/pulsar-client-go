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
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/apache/pulsar-client-go/pkg/pb"
)

func TestUnackedMessageTracker(t *testing.T) {
	unAckTracker := NewUnackedMessageTracker()

	var msgIDs []*pb.MessageIdData

	for i := 0; i < 5; i++ {
		msgID := &pb.MessageIdData{
			LedgerId:   proto.Uint64(1),
			EntryId:    proto.Uint64(uint64(i)),
			Partition:  proto.Int32(-1),
			BatchIndex: proto.Int32(-1),
		}

		msgIDs = append(msgIDs, msgID)
	}

	for _, msgID := range msgIDs {
		ok := unAckTracker.Add(msgID)
		assert.True(t, ok)
	}

	flag := unAckTracker.IsEmpty()
	assert.False(t, flag)

	num := unAckTracker.Size()
	assert.Equal(t, num, 5)

	for index, msgID := range msgIDs {
		unAckTracker.Remove(msgID)
		assert.Equal(t, 4-index, unAckTracker.Size())
	}

	num = unAckTracker.Size()
	assert.Equal(t, num, 0)

	flag = unAckTracker.IsEmpty()
	assert.True(t, flag)
}
