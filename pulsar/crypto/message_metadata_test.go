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

package crypto

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
)

func TestGetEncryptionKeys(t *testing.T) {
	msgMetadata := &pb.MessageMetadata{}
	name1 := "key-1"
	value1 := []byte{1, 2, 3, 4}

	name2 := "key-2"
	value2 := []byte{4, 3, 2, 1}

	name3 := "key-3"
	value3 := []byte{6, 7, 8, 9}

	msgMetadata.EncryptionKeys = append(msgMetadata.EncryptionKeys, &pb.EncryptionKeys{
		Key:   &name1,
		Value: value1,
		Metadata: []*pb.KeyValue{
			{Key: &name1, Value: &name1},
		},
	},
		&pb.EncryptionKeys{
			Key:   &name2,
			Value: value2,
			Metadata: []*pb.KeyValue{
				{Key: &name1, Value: &name1},
				{Key: &name2, Value: &name2},
			},
		},
		&pb.EncryptionKeys{
			Key:   &name3,
			Value: value3,
		},
	)

	expected := []EncryptionKeyInfo{
		{
			name: name1,
			key:  value1,
			metadata: map[string]string{
				"key-1": "key-1",
			},
		},
		{
			name: name2,
			key:  value2,
			metadata: map[string]string{
				"key-1": "key-1",
				"key-2": "key-2",
			},
		},
		{
			name: name3,
			key:  value3,
		},
	}

	msgMetadataSupplier := NewMessageMetadataSupplier(msgMetadata)
	actual := msgMetadataSupplier.EncryptionKeys()

	assert.EqualValues(t, expected, actual)
}

func TestUpsertEncryptionKey(t *testing.T) {
	msgMetadata := &pb.MessageMetadata{}
	key1 := "key-1"
	value1 := []byte{1, 2, 3, 4}

	keyInfo := NewEncryptionKeyInfo(key1, value1, map[string]string{"key-1": "value-1"})

	expected := []EncryptionKeyInfo{*keyInfo}

	msgMetadataSupplier := NewMessageMetadataSupplier(msgMetadata)

	msgMetadataSupplier.UpsertEncryptionKey(*keyInfo)

	// try to add same key again
	msgMetadataSupplier.UpsertEncryptionKey(*keyInfo)

	actual := msgMetadataSupplier.EncryptionKeys()

	assert.EqualValues(t, expected, actual)
}

func TestEncryptionParam(t *testing.T) {
	msgMetadata := &pb.MessageMetadata{}

	expected := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}

	msgMetadataSupplier := NewMessageMetadataSupplier(msgMetadata)
	msgMetadataSupplier.SetEncryptionParam(expected)

	assert.EqualValues(t, expected, msgMetadataSupplier.EncryptionParam())
}
