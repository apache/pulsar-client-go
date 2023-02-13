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

package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTopicName(t *testing.T) {
	topic, err := ParseTopicName("persistent://my-tenant/my-ns/my-topic")

	assert.Nil(t, err)
	assert.Equal(t, "persistent://my-tenant/my-ns/my-topic", topic.Name)
	assert.Equal(t, "my-tenant", topic.Tenant)
	assert.Equal(t, "my-tenant/my-ns", topic.Namespace)
	assert.Equal(t, -1, topic.Partition)

	topic, err = ParseTopicName("my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://public/default/my-topic", topic.Name)
	assert.Equal(t, "public", topic.Tenant)
	assert.Equal(t, "public/default", topic.Namespace)
	assert.Equal(t, "my-topic", topic.Topic)
	assert.Equal(t, -1, topic.Partition)

	topic, err = ParseTopicName("my-tenant/my-namespace/my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://my-tenant/my-namespace/my-topic", topic.Name)
	assert.Equal(t, "my-tenant", topic.Tenant)
	assert.Equal(t, "my-tenant/my-namespace", topic.Namespace)
	assert.Equal(t, "my-topic", topic.Topic)
	assert.Equal(t, -1, topic.Partition)

	topic, err = ParseTopicName("non-persistent://my-tenant/my-namespace/my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "non-persistent://my-tenant/my-namespace/my-topic", topic.Name)
	assert.Equal(t, "my-tenant", topic.Tenant)
	assert.Equal(t, "my-tenant/my-namespace", topic.Namespace)
	assert.Equal(t, "my-topic", topic.Topic)
	assert.Equal(t, -1, topic.Partition)

	topic, err = ParseTopicName("my-topic-partition-5")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://public/default/my-topic-partition-5", topic.Name)
	assert.Equal(t, "public", topic.Tenant)
	assert.Equal(t, "public/default", topic.Namespace)
	assert.Equal(t, "my-topic-partition-5", topic.Topic)
	assert.Equal(t, 5, topic.Partition)

	// V1 topic name
	topic, err = ParseTopicName("persistent://my-tenant/my-cluster/my-ns/my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://my-tenant/my-cluster/my-ns/my-topic", topic.Name)
	assert.Equal(t, "my-tenant", topic.Tenant)
	assert.Equal(t, "my-tenant/my-cluster/my-ns", topic.Namespace)
	assert.Equal(t, "my-topic", topic.Topic)
	assert.Equal(t, -1, topic.Partition)

	topic, err = ParseTopicName("my-tenant/my-cluster/my-ns/my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://my-tenant/my-cluster/my-ns/my-topic", topic.Name)
	assert.Equal(t, "my-tenant", topic.Tenant)
	assert.Equal(t, "my-tenant/my-cluster/my-ns", topic.Namespace)
	assert.Equal(t, "my-topic", topic.Topic)
	assert.Equal(t, -1, topic.Partition)
}

func TestParseTopicNameErrors(t *testing.T) {
	_, err := ParseTopicName("invalid://my-tenant/my-ns/my-topic")
	assert.NotNil(t, err)

	_, err = ParseTopicName("invalid://my-tenant/my-ns/my-topic-partition-xyz")
	assert.NotNil(t, err)

	_, err = ParseTopicName("my-tenant/my-ns/my-topic-partition-xyz/invalid")
	assert.NotNil(t, err)

	_, err = ParseTopicName("persistent://my-tenant")
	assert.NotNil(t, err)

	_, err = ParseTopicName("persistent://my-tenant/my-namespace")
	assert.NotNil(t, err)

	_, err = ParseTopicName("persistent://my-tenant/my-cluster/my-ns/my-topic-partition-xyz/invalid")
	assert.NotNil(t, err)
}

func TestTopicNameWithoutPartitionPart(t *testing.T) {
	tests := []struct {
		tn       *TopicName
		expected string
	}{
		{
			tn:       &TopicName{Name: "persistent://public/default/my-topic", Partition: -1},
			expected: "persistent://public/default/my-topic",
		},
		{
			tn:       &TopicName{Name: "persistent://public/default/my-topic-partition-0", Partition: 0},
			expected: "persistent://public/default/my-topic",
		},
		{
			tn:       nil,
			expected: "",
		},
	}
	for _, test := range tests {
		assert.Equal(t, test.expected, TopicNameWithoutPartitionPart(test.tn))
	}
}

func TestIsV2TopicName(t *testing.T) {
	topic, err := ParseTopicName("persistent://my-tenant/my-ns/my-topic")

	assert.Nil(t, err)
	assert.True(t, IsV2TopicName(topic))

	topic, err = ParseTopicName("my-topic")
	assert.Nil(t, err)
	assert.True(t, IsV2TopicName(topic))

	topic, err = ParseTopicName("my-tenant/my-namespace/my-topic")
	assert.Nil(t, err)
	assert.True(t, IsV2TopicName(topic))

	topic, err = ParseTopicName("non-persistent://my-tenant/my-namespace/my-topic")
	assert.Nil(t, err)
	assert.True(t, IsV2TopicName(topic))

	topic, err = ParseTopicName("my-topic-partition-5")
	assert.Nil(t, err)
	assert.True(t, IsV2TopicName(topic))

	// V1 topic name
	topic, err = ParseTopicName("persistent://my-tenant/my-cluster/my-ns/my-topic")
	assert.Nil(t, err)
	assert.False(t, IsV2TopicName(topic))

	topic, err = ParseTopicName("my-tenant/my-cluster/my-ns/my-topic")
	assert.Nil(t, err)
	assert.False(t, IsV2TopicName(topic))
}

func TestGetTopicRestPath(t *testing.T) {
	topic, err := ParseTopicName("persistent://my-tenant/my-ns/my-topic")

	assert.Nil(t, err)
	assert.Equal(t, "persistent/my-tenant/my-ns/my-topic", GetTopicRestPath(topic))

	topic, err = ParseTopicName("my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "persistent/public/default/my-topic", GetTopicRestPath(topic))

	topic, err = ParseTopicName("my-tenant/my-namespace/my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "persistent/my-tenant/my-namespace/my-topic", GetTopicRestPath(topic))

	topic, err = ParseTopicName("non-persistent://my-tenant/my-namespace/my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "non-persistent/my-tenant/my-namespace/my-topic", GetTopicRestPath(topic))

	topic, err = ParseTopicName("my-topic-partition-5")
	assert.Nil(t, err)
	assert.Equal(t, "persistent/public/default/my-topic-partition-5", GetTopicRestPath(topic))

	// V1 topic name
	topic, err = ParseTopicName("persistent://my-tenant/my-cluster/my-ns/my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "persistent/my-tenant/my-cluster/my-ns/my-topic", GetTopicRestPath(topic))

	topic, err = ParseTopicName("my-tenant/my-cluster/my-ns/my-topic")
	assert.Nil(t, err)
	assert.Equal(t, "persistent/my-tenant/my-cluster/my-ns/my-topic", GetTopicRestPath(topic))
}
