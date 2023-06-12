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
	"context"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	proto "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/stretchr/testify/assert"
)

func TestNewPayloadReader(t *testing.T) {
	msgCtx := &MessagePayloadContext{}
	payload := make([]byte, 10)
	containMeta := false

	pr := NewPayloadReader(msgCtx, payload, containMeta)

	assert.Equal(t, msgCtx, pr.msgCtx)
	assert.Equal(t, payload, pr.buffer.ReadableSlice())
	assert.Equal(t, containMeta, pr.containMeta)
}

func TestReset(t *testing.T) {
	payload := make([]byte, 2)
	pr := NewPayloadReader(&MessagePayloadContext{}, payload, true)

	assert.Equal(t, payload, pr.buffer.ReadableSlice())
	pr.Reset(make([]byte, 0))

	assert.NotEqual(t, pr.buffer.ReadableSlice(), payload)
	assert.Equal(t, len(pr.buffer.ReadableSlice()), 0)
}

func TestTopic(t *testing.T) {
	msgCtx := &MessagePayloadContext{topic: "test1"}

	pr := NewPayloadReader(msgCtx, make([]byte, 2), false)

	assert.Equal(t, pr.Topic(), "test1")
}

func TestNext(t *testing.T) {
	var batchSize int32 = 10
	pr := &PayloadReader{
		buffer:        internal.NewBuffer(10),
		errorOccurred: true,
		msgCtx: &MessagePayloadContext{
			msgMetaData: &proto.MessageMetadata{NumMessagesInBatch: &batchSize},
		},
	}
	_, err := pr.Next(context.Background())

	assert.Equal(t, err, ErrNoMoreMessagesToRead)

	pr.errorOccurred = false

	_, err = pr.Next(context.Background())
	assert.Nil(t, err)

	pr.currentBatchIdx = 10

	_, err = pr.Next(context.Background())
	assert.Equal(t, err, ErrNoMoreMessagesToRead)
}

func TestHasNext(t *testing.T) {
	var batchSize int32 = 10
	pr := &PayloadReader{
		buffer:        internal.NewBuffer(10),
		errorOccurred: false,
		msgCtx: &MessagePayloadContext{
			msgMetaData: &proto.MessageMetadata{NumMessagesInBatch: &batchSize},
		},
	}

	hasNext := pr.HasNext()
	assert.True(t, hasNext)

	pr.errorOccurred = true
	hasNext = pr.HasNext()
	assert.False(t, hasNext)

	pr.errorOccurred = false
	pr.currentBatchIdx = 10
	hasNext = pr.HasNext()
	assert.False(t, hasNext)
}

func TestSeek(t *testing.T) {
	pr := &PayloadReader{}
	err := pr.Seek(&myMessageID{})
	assert.Equal(t, err, ErrNotImplemented)
}

func TestSeekByTime(t *testing.T) {
	pr := &PayloadReader{}
	err := pr.SeekByTime(time.Now())
	assert.Equal(t, err, ErrNotImplemented)
}

func TestReadAsSingleMessage(t *testing.T) {
	var batchSize int32 = 1
	payload := make([]byte, 10)
	pr := &PayloadReader{
		buffer: internal.NewBufferWrapper(payload),
		msgCtx: &MessagePayloadContext{
			topic:       "test1",
			msgMetaData: &proto.MessageMetadata{NumMessagesInBatch: &batchSize},
		},
	}
	msg, err := pr.readAsSingleMessage()
	assert.Nil(t, err)
	assert.Equal(t, msg.Payload(), payload)
	assert.Equal(t, msg.Topic(), "test1")
}

func TestReadNextBatchMessage(t *testing.T) {
	var batchSize int32 = 10
	pr := &PayloadReader{
		buffer: internal.NewBufferWrapper(batchMessage10),
		msgCtx: &MessagePayloadContext{
			msgID:       &messageID{},
			topic:       "test1",
			msgMetaData: &proto.MessageMetadata{NumMessagesInBatch: &batchSize},
			pc: &partitionConsumer{
				ackGroupingTracker: &timedAckGroupingTracker{
					lastCumulativeAck: &messageID{},
				},
			},
		},
		containMeta: true,
	}
	for i := 0; i < int(batchSize); i++ {
		msg, err := pr.readNextBatchMessage()
		assert.Nil(t, err)
		assert.Equal(t, len("hello"), len(msg.Payload()))
	}
}

// Message with batch of 10
// singe message metadata properties:<key:"a" value:"1" > properties:<key:"b" value:"2" >
// payload = "hello"
var batchMessage10 = []byte{
	0x00, 0x00, 0x00, 0x16, 0x0a,
	0x06, 0x0a, 0x01, 0x61, 0x12, 0x01, 0x31, 0x0a,
	0x06, 0x0a, 0x01, 0x62, 0x12, 0x01, 0x32, 0x18,
	0x05, 0x28, 0x05, 0x40, 0x00, 0x68, 0x65, 0x6c,
	0x6c, 0x6f, 0x00, 0x00, 0x00, 0x16, 0x0a, 0x06,
	0x0a, 0x01, 0x61, 0x12, 0x01, 0x31, 0x0a, 0x06,
	0x0a, 0x01, 0x62, 0x12, 0x01, 0x32, 0x18, 0x05,
	0x28, 0x05, 0x40, 0x01, 0x68, 0x65, 0x6c, 0x6c,
	0x6f, 0x00, 0x00, 0x00, 0x16, 0x0a, 0x06, 0x0a,
	0x01, 0x61, 0x12, 0x01, 0x31, 0x0a, 0x06, 0x0a,
	0x01, 0x62, 0x12, 0x01, 0x32, 0x18, 0x05, 0x28,
	0x05, 0x40, 0x02, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
	0x00, 0x00, 0x00, 0x16, 0x0a, 0x06, 0x0a, 0x01,
	0x61, 0x12, 0x01, 0x31, 0x0a, 0x06, 0x0a, 0x01,
	0x62, 0x12, 0x01, 0x32, 0x18, 0x05, 0x28, 0x05,
	0x40, 0x03, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00,
	0x00, 0x00, 0x16, 0x0a, 0x06, 0x0a, 0x01, 0x61,
	0x12, 0x01, 0x31, 0x0a, 0x06, 0x0a, 0x01, 0x62,
	0x12, 0x01, 0x32, 0x18, 0x05, 0x28, 0x05, 0x40,
	0x04, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00,
	0x00, 0x16, 0x0a, 0x06, 0x0a, 0x01, 0x61, 0x12,
	0x01, 0x31, 0x0a, 0x06, 0x0a, 0x01, 0x62, 0x12,
	0x01, 0x32, 0x18, 0x05, 0x28, 0x05, 0x40, 0x05,
	0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00,
	0x16, 0x0a, 0x06, 0x0a, 0x01, 0x61, 0x12, 0x01,
	0x31, 0x0a, 0x06, 0x0a, 0x01, 0x62, 0x12, 0x01,
	0x32, 0x18, 0x05, 0x28, 0x05, 0x40, 0x06, 0x68,
	0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x16,
	0x0a, 0x06, 0x0a, 0x01, 0x61, 0x12, 0x01, 0x31,
	0x0a, 0x06, 0x0a, 0x01, 0x62, 0x12, 0x01, 0x32,
	0x18, 0x05, 0x28, 0x05, 0x40, 0x07, 0x68, 0x65,
	0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x16, 0x0a,
	0x06, 0x0a, 0x01, 0x61, 0x12, 0x01, 0x31, 0x0a,
	0x06, 0x0a, 0x01, 0x62, 0x12, 0x01, 0x32, 0x18,
	0x05, 0x28, 0x05, 0x40, 0x08, 0x68, 0x65, 0x6c,
	0x6c, 0x6f, 0x00, 0x00, 0x00, 0x16, 0x0a, 0x06,
	0x0a, 0x01, 0x61, 0x12, 0x01, 0x31, 0x0a, 0x06,
	0x0a, 0x01, 0x62, 0x12, 0x01, 0x32, 0x18, 0x05,
	0x28, 0x05, 0x40, 0x09, 0x68, 0x65, 0x6c, 0x6c,
	0x6f,
}
