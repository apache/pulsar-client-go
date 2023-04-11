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
	"fmt"
	"time"

	pkgerrors "github.com/pkg/errors"

	"google.golang.org/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
)

// NewUnexpectedErrMsg instantiates an ErrUnexpectedMsg error.
// Optionally provide a list of IDs associated with the message
// for additional context in the error message.
func newUnexpectedErrMsg(msgType pb.BaseCommand_Type, ids ...interface{}) *unexpectedErrMsg {
	return &unexpectedErrMsg{
		msgType: msgType,
		ids:     ids,
	}
}

// UnexpectedErrMsg is returned when an unexpected message is received.
type unexpectedErrMsg struct {
	msgType pb.BaseCommand_Type
	ids     []interface{}
}

// Error satisfies the error interface.
func (e *unexpectedErrMsg) Error() string {
	msg := fmt.Sprintf("received unexpected message of type %q", e.msgType.String())
	for _, id := range e.ids {
		msg += fmt.Sprintf(" consumerID=%v", id)
	}
	return msg
}

func validateTopicNames(topics ...string) ([]*internal.TopicName, error) {
	tns := make([]*internal.TopicName, len(topics))
	for i, t := range topics {
		tn, err := internal.ParseTopicName(t)
		if err != nil {
			return nil, pkgerrors.Wrapf(err, "invalid topic name: %s", t)
		}
		tns[i] = tn
	}
	return tns, nil
}

func toKeyValues(metadata map[string]string) []*pb.KeyValue {
	kvs := make([]*pb.KeyValue, 0, len(metadata))
	for k, v := range metadata {
		kv := &pb.KeyValue{
			Key:   proto.String(k),
			Value: proto.String(v),
		}
		kvs = append(kvs, kv)
	}

	return kvs
}

func toStringMap(metadata []*pb.KeyValue) map[string]string {
	m := make(map[string]string, len(metadata))
	for _, kv := range metadata {
		m[*kv.Key] = *kv.Value
	}
	return m
}

func isPriorBatchIndex(index int, messageID MessageID) bool {
	return index < int(messageID.BatchIdx())
}

func getMessageIndex(brokerMetaData *pb.BrokerEntryMetadata, index int, batchSize int) uint64 {
	var messageIndex uint64
	if brokerMetaData != nil && brokerMetaData.GetIndex() != 0 {
		messageIndex = brokerMetaData.GetIndex() - uint64(batchSize) + uint64(index) + 1
	}
	return messageIndex
}

func getBrokerPublishTime(brokerMetaData *pb.BrokerEntryMetadata) time.Time {
	var brokerPublishTime time.Time
	if brokerMetaData.GetBrokerTimestamp() != 0 {
		brokerPublishTime = timeFromUnixTimestampMillis(brokerMetaData.GetBrokerTimestamp())
	}
	return brokerPublishTime
}
