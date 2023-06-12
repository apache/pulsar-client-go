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
	"errors"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/bits-and-blooms/bitset"
)

var (
	ErrBatchDeSerialization = errors.New("failed to deserialize")
)

type MessagePayloadProcessor interface {
	//Passing payload as byte slice instead of explosing internal Buffer
	Process(ctx *MessagePayloadContext, payload []byte, schema Schema, consume ConsumeMessages) error
}

// MessagePayloadContext only carries the nessesary data to process the message
type MessagePayloadContext struct {
	batch           bool
	topic           string
	redeliveryCount uint32
	msgID           MessageID
	startMsgID      MessageID
	ackSet          *bitset.BitSet
	schema          Schema
	schemaInfoCache *schemaInfoCache
	msgMetaData     *pb.MessageMetadata
	brokerMetaData  *pb.BrokerEntryMetadata
	pc              *partitionConsumer
	logger          log.Logger
}

func (ctx MessagePayloadContext) GetTopic() string {
	return ctx.topic
}

func (ctx MessagePayloadContext) IsBatch() bool {
	return ctx.batch
}

func (ctx MessagePayloadContext) GetNumMessages() int {
	if ctx.msgMetaData.NumMessagesInBatch == nil {
		return 0
	}
	return int(*ctx.msgMetaData.NumMessagesInBatch)
}

func (ctx MessagePayloadContext) GetProperty(key string) (value string, found bool) {
	for _, kv := range ctx.msgMetaData.Properties {
		if *kv.Key == key {
			return *kv.Value, true
		}
	}
	return "", false
}

func (ctx MessagePayloadContext) GetProperties() map[string]string {
	return toStringMap(ctx.msgMetaData.Properties)
}

// ConsumeMessages will be used to dispatch messages from Processor
type ConsumeMessages func([]Message)
