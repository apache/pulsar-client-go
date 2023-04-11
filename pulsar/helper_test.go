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
	"time"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/stretchr/testify/assert"
)

func TestToKeyValues(t *testing.T) {
	meta := map[string]string{
		"key1": "value1",
	}

	kvs := toKeyValues(meta)
	assert.Equal(t, 1, len(kvs))
	assert.Equal(t, "key1", *kvs[0].Key)
	assert.Equal(t, "value1", *kvs[0].Value)

	meta = map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	kvs = toKeyValues(meta)
	assert.Equal(t, 2, len(kvs))
	for _, kv := range kvs {
		v := meta[*kv.Key]
		assert.Equal(t, v, *kv.Value)
	}
}

func TestToStringMap(t *testing.T) {
	key1, key2, value1, value2 := "key1", "key2", "value1", "value2"
	dataAsKeyValues := []*pb.KeyValue{
		&pb.KeyValue{
			Key:   &key1,
			Value: &value1,
		}, &pb.KeyValue{
			Key:   &key2,
			Value: &value2,
		},
	}
	expectedMap := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	actualMap := toStringMap(dataAsKeyValues)
	assert.Equal(t, expectedMap, actualMap)
}

func TestIsPriorBatchIndex(t *testing.T) {
	msgID := messageID{
		batchIdx: 30,
	}
	isPrior := isPriorBatchIndex(20, &msgID)
	assert.True(t, isPrior)
	isPrior = isPriorBatchIndex(34, &msgID)
	assert.False(t, isPrior)
}

func TestGetMessageIndex(t *testing.T) {
	var index uint64 = 30
	batchSize := 10
	msgIdx := getMessageIndex(&pb.BrokerEntryMetadata{
		Index: &index,
	}, 2, batchSize)

	assert.Equal(t, uint64(23), msgIdx)
}

func TestGetBrokerPublishTime(t *testing.T) {
	timestamp := uint64(1646983036054)
	publishedTime := getBrokerPublishTime(&pb.BrokerEntryMetadata{BrokerTimestamp: &timestamp})
	expectedTimeInLocal := time.Date(2022, time.March, 11, 7, 17, 16, 54000000, time.UTC).Local()
	assert.Equal(t, expectedTimeInLocal, publishedTime)
}
