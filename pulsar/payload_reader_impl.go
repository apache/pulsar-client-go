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
	"errors"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
)

var (
	ErrNoMoreMessagesToRead = errors.New("no more messages to read")
	ErrNotImplemented       = errors.New("method not implemented")
)

// PayloadReader implements Reader interface and provide convinent methods
// for processing byte slice payload.
// In PIP-96, two methods(getMessageAt & asSingleMessage) are propsed for Java Client.
// Provide similar capablities using this reader
type PayloadReader struct {
	buffer internal.Buffer
	msgCtx *MessagePayloadContext
	// currentBatchIdx will be increased after every successful Next call
	currentBatchIdx int
	errorOccurred   bool
	containMeta     bool
}

func NewPayloadReader(msgCtx *MessagePayloadContext, payload []byte, containMeta bool) *PayloadReader {
	return &PayloadReader{
		buffer:      internal.NewBufferWrapper(payload),
		msgCtx:      msgCtx,
		containMeta: containMeta,
	}
}

func (pr *PayloadReader) Reset(data []byte) {
	pr.buffer = internal.NewBufferWrapper(data)
}

func (pr *PayloadReader) Topic() string {
	return pr.msgCtx.topic
}

func (pr *PayloadReader) Next(ctx context.Context) (Message, error) {
	if !(pr.currentBatchIdx < pr.msgCtx.GetNumMessages() && !pr.errorOccurred) {
		return nil, ErrNoMoreMessagesToRead
	}
	if pr.msgCtx.IsBatch() {
		return pr.readNextBatchMessage()
	}
	return pr.readAsSingleMessage()
}

func (pr *PayloadReader) HasNext() bool {
	return pr.currentBatchIdx < pr.msgCtx.GetNumMessages() && !pr.errorOccurred
}

func (pr *PayloadReader) Close() {
}

func (pr *PayloadReader) Seek(id MessageID) error {
	return ErrNotImplemented
}

func (pr *PayloadReader) SeekByTime(time time.Time) error {
	return ErrNotImplemented
}

func (pr *PayloadReader) readAsSingleMessage() (Message, error) {
	var messageIndex *uint64
	var brokerPublishTime *time.Time

	msg := &MessageImpl{
		publishTime:         timeFromUnixTimestampMillis(pr.msgCtx.msgMetaData.GetPublishTime()),
		eventTime:           timeFromUnixTimestampMillis(pr.msgCtx.msgMetaData.GetEventTime()),
		key:                 pr.msgCtx.msgMetaData.GetPartitionKey(),
		orderingKey:         string(pr.msgCtx.msgMetaData.OrderingKey),
		producerName:        pr.msgCtx.msgMetaData.GetProducerName(),
		payLoad:             pr.buffer.ReadableSlice(),
		msgID:               pr.msgCtx.msgID,
		properties:          internal.ConvertToStringMap(pr.msgCtx.msgMetaData.GetProperties()),
		topic:               pr.msgCtx.topic,
		replicationClusters: pr.msgCtx.msgMetaData.GetReplicateTo(),
		replicatedFrom:      pr.msgCtx.msgMetaData.GetReplicatedFrom(),
		redeliveryCount:     pr.msgCtx.redeliveryCount,
		schema:              pr.msgCtx.schema,
		schemaVersion:       pr.msgCtx.msgMetaData.GetSchemaVersion(),
		schemaInfoCache:     pr.msgCtx.schemaInfoCache,
		encryptionContext:   createEncryptionContext(pr.msgCtx.msgMetaData),
		index:               messageIndex,
		brokerPublishTime:   brokerPublishTime,
	}
	return msg, nil
}

func (pr *PayloadReader) readNextBatchMessage() (Message, error) {
	// avoid increasing pr.currentBatchIdx until it finishes successfully
	index := pr.currentBatchIdx + 1

	msgReader := internal.NewBatchMessageReader(pr.buffer)

	var singleMsgMeta *pb.SingleMessageMetadata
	var payload []byte
	var err error
	if pr.containMeta {
		singleMsgMeta, payload, err = msgReader.ReadMessage()
		if err != nil || payload == nil {
			pr.errorOccurred = true
			return nil, err
		}
	} else {
		payload = pr.buffer.ReadableSlice()
		singleMsgMeta = &pb.SingleMessageMetadata{}
	}

	if pr.msgCtx.ackSet != nil && !pr.msgCtx.ackSet.Test(uint(index)) {
		pr.msgCtx.logger.Debugf("Ignoring message from %vth message, which has been acknowledged", index)
		return nil, nil
	}

	trackingMsgID := newTrackingMessageID(
		pr.msgCtx.msgID.LedgerID(),
		pr.msgCtx.msgID.EntryID(),
		int32(index),
		pr.msgCtx.msgID.PartitionIdx(),
		int32(pr.msgCtx.GetNumMessages()),
		newAckTracker(uint(pr.msgCtx.GetNumMessages())))
	trackingMsgID.consumer = pr.msgCtx.pc

	if pr.msgCtx.pc.messageShouldBeDiscarded(trackingMsgID) {
		pr.msgCtx.pc.AckID(trackingMsgID)
		return nil, nil
	}

	var msgID MessageID
	isChunkedMsg := pr.msgCtx.msgMetaData.GetNumChunksFromMsg() > 1
	if isChunkedMsg {
		ctx := pr.msgCtx.pc.chunkedMsgCtxMap.get(pr.msgCtx.msgMetaData.GetUuid())
		if ctx == nil {
			// chunkedMsgCtxMap has closed because of consumer closed
			pr.msgCtx.pc.log.Warnf("get chunkedMsgCtx for chunk with uuid %s failed because consumer has closed",
				pr.msgCtx.msgMetaData.Uuid)
			return nil, nil
		}
		cmid := newChunkMessageID(ctx.firstChunkID(), ctx.lastChunkID())
		// set the consumer so we know how to ack the message id
		cmid.consumer = pr.msgCtx.pc
		// clean chunkedMsgCtxMap
		pr.msgCtx.pc.chunkedMsgCtxMap.remove(pr.msgCtx.msgMetaData.GetUuid())
		pr.msgCtx.pc.unAckChunksTracker.add(cmid, ctx.chunkedMsgIDs)
		msgID = cmid
	} else {
		msgID = trackingMsgID
	}

	if pr.msgCtx.pc.ackGroupingTracker.isDuplicate(msgID) {
		return nil, nil
	}

	msgIndex := getMessageIndex(pr.msgCtx.brokerMetaData, index+1, pr.msgCtx.GetNumMessages())
	brokerPublishTime := getBrokerPublishTime(pr.msgCtx.brokerMetaData)
	msg := &MessageImpl{
		publishTime:         timeFromUnixTimestampMillis(pr.msgCtx.msgMetaData.GetPublishTime()),
		eventTime:           timeFromUnixTimestampMillis(singleMsgMeta.GetEventTime()),
		key:                 singleMsgMeta.GetPartitionKey(),
		producerName:        pr.msgCtx.msgMetaData.GetProducerName(),
		properties:          internal.ConvertToStringMap(singleMsgMeta.GetProperties()),
		topic:               pr.msgCtx.topic,
		msgID:               trackingMsgID,
		payLoad:             payload,
		schema:              pr.msgCtx.schema,
		replicationClusters: pr.msgCtx.msgMetaData.GetReplicateTo(),
		replicatedFrom:      pr.msgCtx.msgMetaData.GetReplicatedFrom(),
		redeliveryCount:     pr.msgCtx.redeliveryCount,
		schemaVersion:       pr.msgCtx.msgMetaData.GetSchemaVersion(),
		schemaInfoCache:     pr.msgCtx.schemaInfoCache,
		orderingKey:         string(singleMsgMeta.GetOrderingKey()),
		encryptionContext:   createEncryptionContext(pr.msgCtx.msgMetaData),
		brokerPublishTime:   &brokerPublishTime,
		index:               &msgIndex,
	}
	pr.currentBatchIdx = pr.currentBatchIdx + 1

	return msg, nil
}
