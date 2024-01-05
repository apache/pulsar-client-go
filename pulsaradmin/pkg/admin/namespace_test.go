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

package admin

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptr(n int) *int {
	return &n
}

func TestSetTopicAutoCreation(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	tests := []struct {
		name      string
		namespace string
		config    utils.TopicAutoCreationConfig
		errReason string
	}{
		{
			name:      "Set partitioned type topic auto creation",
			namespace: "public/default",
			config: utils.TopicAutoCreationConfig{
				Allow:      true,
				Type:       utils.Partitioned,
				Partitions: ptr(3),
			},
			errReason: "",
		},
		{
			name:      "Set partitioned type topic auto creation without partitions",
			namespace: "public/default",
			config: utils.TopicAutoCreationConfig{
				Allow: true,
				Type:  utils.Partitioned,
			},
			errReason: "Invalid configuration for autoTopicCreationOverride. the detail is [defaultNumPartitions] cannot be null when the type is partitioned.",
		},
		{
			name:      "Set partitioned type topic auto creation with partitions < 1",
			namespace: "public/default",
			config: utils.TopicAutoCreationConfig{
				Allow:      true,
				Type:       utils.Partitioned,
				Partitions: ptr(-1),
			},
			errReason: "Invalid configuration for autoTopicCreationOverride. the detail is [defaultNumPartitions] cannot be less than 1 for partition type.",
		},
		{
			name:      "Set non-partitioned type topic auto creation",
			namespace: "public/default",
			config: utils.TopicAutoCreationConfig{
				Allow: true,
				Type:  utils.NonPartitioned,
			},
			errReason: "",
		},
		{
			name:      "Set non-partitioned type topic auto creation with partitions",
			namespace: "public/default",
			config: utils.TopicAutoCreationConfig{
				Allow:      true,
				Type:       utils.NonPartitioned,
				Partitions: ptr(3),
			},
			errReason: "Invalid configuration for autoTopicCreationOverride. the detail is [defaultNumPartitions] is not allowed to be set when the type is non-partition.",
		},
		{
			name:      "Disable topic auto creation",
			namespace: "public/default",
			config: utils.TopicAutoCreationConfig{
				Allow: false,
			},
			errReason: "",
		},
		{
			name:      "Set topic auto creation on a non-exist namespace",
			namespace: "public/nonexist",
			config: utils.TopicAutoCreationConfig{
				Allow: true,
				Type:  utils.NonPartitioned,
			},
			errReason: "Namespace does not exist",
		},
		{
			name:      "Set topic auto creation on a non-exist tenant",
			namespace: "non-exist/default",
			config: utils.TopicAutoCreationConfig{
				Allow: true,
				Type:  utils.NonPartitioned,
			},
			errReason: "Namespace does not exist",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			namespace, _ := utils.GetNamespaceName(tt.namespace)
			err := admin.Namespaces().SetTopicAutoCreation(*namespace, tt.config)
			if tt.errReason == "" {
				assert.Equal(t, nil, err)

				err = admin.Namespaces().RemoveTopicAutoCreation(*namespace)
				assert.Equal(t, nil, err)
			}
			if err != nil {
				restError := err.(rest.Error)
				assert.Equal(t, tt.errReason, restError.Reason)
			}
		})
	}
}

func TestGetTopicAutoCreation(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespace, _ := utils.GetNamespaceName("public/default")

	// set the topic auto creation config and get it
	err = admin.Namespaces().SetTopicAutoCreation(*namespace, utils.TopicAutoCreationConfig{
		Allow: true,
		Type:  utils.NonPartitioned,
	})
	assert.Equal(t, nil, err)
	topicAutoCreation, err := admin.Namespaces().GetTopicAutoCreation(*namespace)
	assert.Equal(t, nil, err)
	expected := utils.TopicAutoCreationConfig{
		Allow: true,
		Type:  utils.NonPartitioned,
	}
	assert.Equal(t, expected, *topicAutoCreation)

	// remove the topic auto creation config and get it
	err = admin.Namespaces().RemoveTopicAutoCreation(*namespace)
	assert.Equal(t, nil, err)

	topicAutoCreation, err = admin.Namespaces().GetTopicAutoCreation(*namespace)
	assert.Equal(t, nil, err)
	expected = utils.TopicAutoCreationConfig{
		Allow: false,
		Type:  "",
	}
	assert.Equal(t, expected, *topicAutoCreation)
}
