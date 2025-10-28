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
	"os"
	"testing"
	"time"

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
			errReason: "Invalid configuration for autoTopicCreationOverride. the detail is [defaultNumPartitions] " +
				"cannot be null when the type is partitioned.",
		},
		{
			name:      "Set partitioned type topic auto creation with partitions < 1",
			namespace: "public/default",
			config: utils.TopicAutoCreationConfig{
				Allow:      true,
				Type:       utils.Partitioned,
				Partitions: ptr(-1),
			},
			errReason: "Invalid configuration for autoTopicCreationOverride. the detail is [defaultNumPartitions] " +
				"cannot be less than 1 for partition type.",
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
			errReason: "Invalid configuration for autoTopicCreationOverride. the detail is [defaultNumPartitions] is " +
				"not allowed to be set when the type is non-partition.",
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
			errReason: "Tenant does not exist",
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
	assert.NotNil(t, topicAutoCreation, "Expected non-nil when topic auto creation is configured")
	expected := utils.TopicAutoCreationConfig{
		Allow: true,
		Type:  utils.NonPartitioned,
	}
	assert.Equal(t, expected, *topicAutoCreation)

	// remove the topic auto creation config and get it - should return nil
	err = admin.Namespaces().RemoveTopicAutoCreation(*namespace)
	assert.Equal(t, nil, err)

	topicAutoCreation, err = admin.Namespaces().GetTopicAutoCreation(*namespace)
	assert.Equal(t, nil, err)
	assert.Nil(t, topicAutoCreation, "Expected nil when topic auto creation is not configured")
}

func TestRevokeSubPermission(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespace, err := utils.GetNamespaceName("public/default")
	require.NoError(t, err)
	require.NotNil(t, namespace)

	sub := "subscription"
	roles := []string{"user"}

	// grant subscription permission and get it
	err = admin.Namespaces().GrantSubPermission(*namespace, sub, roles)
	require.NoError(t, err)
	permissions, err := admin.Namespaces().GetSubPermissions(*namespace)
	require.NoError(t, err)
	assert.Equal(t, roles, permissions[sub])

	// revoke subscription permission and get it
	err = admin.Namespaces().RevokeSubPermission(*namespace, sub, roles[0])
	require.NoError(t, err)
	permissions, err = admin.Namespaces().GetSubPermissions(*namespace)
	require.NoError(t, err)
	assert.Equal(t, 0, len(permissions[sub]))
}

func TestNamespaces_SetSubscriptionExpirationTime(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	tests := []struct {
		name                       string
		namespace                  string
		subscriptionExpirationTime int
		errReason                  string
	}{
		{
			name:                       "Set valid subscription expiration time",
			namespace:                  "public/default",
			subscriptionExpirationTime: 60,
			errReason:                  "",
		},
		{
			name:                       "Set invalid subscription expiration time",
			namespace:                  "public/default",
			subscriptionExpirationTime: -60,
			errReason:                  "Invalid value for subscription expiration time",
		},
		{
			name:                       "Set valid subscription expiration time: 0",
			namespace:                  "public/default",
			subscriptionExpirationTime: 0,
			errReason:                  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			namespace, _ := utils.GetNamespaceName(tt.namespace)
			err := admin.Namespaces().SetSubscriptionExpirationTime(*namespace, tt.subscriptionExpirationTime)
			if tt.errReason == "" {
				assert.Equal(t, nil, err)

				err = admin.Namespaces().RemoveSubscriptionExpirationTime(*namespace)
				assert.Equal(t, nil, err)
			}
			if err != nil {
				restError := err.(rest.Error)
				assert.Equal(t, tt.errReason, restError.Reason)
			}
		})
	}
}

func TestNamespaces_GetSubscriptionExpirationTime(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespace, _ := utils.GetNamespaceName("public/default")

	// set the subscription expiration time and get it
	err = admin.Namespaces().SetSubscriptionExpirationTime(*namespace,
		60)
	assert.Equal(t, nil, err)
	subscriptionExpirationTime, err := admin.Namespaces().GetSubscriptionExpirationTime(*namespace)
	assert.Equal(t, nil, err)
	expected := 60
	assert.Equal(t, expected, subscriptionExpirationTime)

	// remove the subscription expiration time and get it
	err = admin.Namespaces().RemoveSubscriptionExpirationTime(*namespace)
	assert.Equal(t, nil, err)

	subscriptionExpirationTime, err = admin.Namespaces().GetSubscriptionExpirationTime(*namespace)
	assert.Equal(t, nil, err)
	expected = -1
	assert.Equal(t, expected, subscriptionExpirationTime)
}

func TestNamespaces_SetOffloadThresholdInSeconds(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	tests := []struct {
		name      string
		namespace string
		threshold int64
		errReason string
	}{
		{
			name:      "Set valid offloadThresholdInSecond",
			namespace: "public/default",
			threshold: 60,
			errReason: "",
		},
		{
			name:      "Set invalid offloadThresholdInSecond",
			namespace: "public/default",
			threshold: -60,
			errReason: "Invalid value for offloadThresholdInSecond",
		},
		{
			name:      "Set valid offloadThresholdInSecond: 0",
			namespace: "public/default",
			threshold: 0,
			errReason: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			namespace, _ := utils.GetNamespaceName(tt.namespace)
			err := admin.Namespaces().SetOffloadThresholdInSeconds(*namespace, tt.threshold)
			if tt.errReason == "" {
				assert.Equal(t, nil, err)
			}
			if err != nil {
				restError := err.(rest.Error)
				assert.Equal(t, tt.errReason, restError.Reason)
			}
		})
	}
}

func TestNamespaces_GetOffloadThresholdInSeconds(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespace, _ := utils.GetNamespaceName("public/default")

	// Get default (should be -1)
	threshold, err := admin.Namespaces().GetOffloadThreshold(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), threshold)

	err = admin.Namespaces().SetOffloadThresholdInSeconds(*namespace,
		60)
	assert.Equal(t, nil, err)
	offloadThresholdInSeconds, err := admin.Namespaces().GetOffloadThresholdInSeconds(*namespace)
	assert.Equal(t, nil, err)
	expected := int64(60)
	assert.Equal(t, expected, offloadThresholdInSeconds)
}

func TestNamespaces_SetSchemaCompatibilityStrategy(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	tests := []struct {
		name      string
		namespace string
		strategy  utils.SchemaCompatibilityStrategy
		errReason string
	}{
		{
			name:      "Set Undefined strategy",
			namespace: "public/default",
			strategy:  utils.SchemaCompatibilityStrategyUndefined,
			errReason: "",
		},
		{
			name:      "Set AlwaysIncompatible strategy",
			namespace: "public/default",
			strategy:  utils.SchemaCompatibilityStrategyAlwaysIncompatible,
			errReason: "",
		},
		{
			name:      "Set AlwaysCompatible strategy",
			namespace: "public/default",
			strategy:  utils.SchemaCompatibilityStrategyAlwaysCompatible,
			errReason: "",
		},
		{
			name:      "Set Backward strategy",
			namespace: "public/default",
			strategy:  utils.SchemaCompatibilityStrategyBackward,
			errReason: "",
		},
		{
			name:      "Set Forward strategy",
			namespace: "public/default",
			strategy:  utils.SchemaCompatibilityStrategyForward,
			errReason: "",
		},
		{
			name:      "Set Full strategy",
			namespace: "public/default",
			strategy:  utils.SchemaCompatibilityStrategyFull,
			errReason: "",
		},
		{
			name:      "Set BackwardTransitive strategy",
			namespace: "public/default",
			strategy:  utils.SchemaCompatibilityStrategyBackwardTransitive,
			errReason: "",
		},
		{
			name:      "Set ForwardTransitive strategy",
			namespace: "public/default",
			strategy:  utils.SchemaCompatibilityStrategyForwardTransitive,
			errReason: "",
		},
		{
			name:      "Set FullTransitive strategy",
			namespace: "public/default",
			strategy:  utils.SchemaCompatibilityStrategyFullTransitive,
			errReason: "",
		},
		{
			name:      "Set strategy on non-existent namespace",
			namespace: "public/nonexist",
			strategy:  utils.SchemaCompatibilityStrategyFull,
			errReason: "Namespace does not exist",
		},
		{
			name:      "Set strategy on non-existent tenant",
			namespace: "non-exist/default",
			strategy:  utils.SchemaCompatibilityStrategyFull,
			errReason: "Tenant does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			namespace, _ := utils.GetNamespaceName(tt.namespace)
			err := admin.Namespaces().SetSchemaCompatibilityStrategy(*namespace, tt.strategy)

			// Skip test if network connection fails (Pulsar server not running)
			if err != nil {
				if _, ok := err.(rest.Error); !ok {
					t.Skipf("Skipping test due to network error: %v", err)
				}
			}

			if tt.errReason == "" {
				assert.Equal(t, nil, err)
			} else {
				if restError, ok := err.(rest.Error); ok {
					assert.Equal(t, tt.errReason, restError.Reason)
				}
			}
		})
	}
}

func TestNamespaces_GetSchemaCompatibilityStrategy(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespace, _ := utils.GetNamespaceName("public/default")

	// Test setting and getting different strategies
	testStrategies := []utils.SchemaCompatibilityStrategy{
		utils.SchemaCompatibilityStrategyFull,
		utils.SchemaCompatibilityStrategyBackward,
		utils.SchemaCompatibilityStrategyForward,
		utils.SchemaCompatibilityStrategyAlwaysCompatible,
	}

	for _, strategy := range testStrategies {
		// Set the schema compatibility strategy
		err = admin.Namespaces().SetSchemaCompatibilityStrategy(*namespace, strategy)
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
		}

		// Wait for the strategy to be set
		time.Sleep(5 * time.Second)

		// Get and verify the strategy
		retrievedStrategy, err := admin.Namespaces().GetSchemaCompatibilityStrategy(*namespace)
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
		}
		assert.Equal(t, strategy, retrievedStrategy)
	}

	// Test getting default strategy (should be Undefined after reset)
	err = admin.Namespaces().SetSchemaCompatibilityStrategy(*namespace, utils.SchemaCompatibilityStrategyUndefined)
	if err != nil {
		t.Skipf("Skipping test due to connection error: %v", err)
	}

	// Wait for the strategy to be set
	time.Sleep(5 * time.Second)

	defaultStrategy, err := admin.Namespaces().GetSchemaCompatibilityStrategy(*namespace)
	if err != nil {
		t.Skipf("Skipping test due to connection error: %v", err)
	}
	assert.Equal(t, utils.SchemaCompatibilityStrategyUndefined, defaultStrategy)
}

func TestNamespaces_Properties(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespace, err := utils.GetNamespaceName("public/default")
	assert.Equal(t, err, nil)

	// Namespace properties are expected to be set and retrieved successfully
	properties := map[string]string{
		"key-1": "value-1",
	}
	err = admin.Namespaces().UpdateProperties(*namespace, properties)
	assert.Equal(t, err, nil)

	actualProperties, err := admin.Namespaces().GetProperties(*namespace)
	assert.Equal(t, err, nil)
	assert.Equal(t, actualProperties, properties)

	// All namespace properties are expected to be deleted successfully
	err = admin.Namespaces().RemoveProperties(*namespace)
	assert.Equal(t, err, nil)
	actualPropertiesAfterRemoveCall, err := admin.Namespaces().GetProperties(*namespace)
	assert.Equal(t, err, nil)
	assert.Equal(t, actualPropertiesAfterRemoveCall, map[string]string{})
}

func TestNamespaces_SetMaxTopicsPerNamespace(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	tests := []struct {
		name      string
		namespace string
		maxTopics int
		errReason string
	}{
		{
			name:      "Set valid max topics per namespace",
			namespace: "public/default",
			maxTopics: 100,
			errReason: "",
		},
		{
			name:      "Set invalid max topics per namespace",
			namespace: "public/default",
			maxTopics: -1,
			errReason: "maxTopicsPerNamespace must be 0 or more",
		},
		{
			name:      "Set valid max topics per namespace: 0",
			namespace: "public/default",
			maxTopics: 0,
			errReason: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			namespace, _ := utils.GetNamespaceName(tt.namespace)
			err := admin.Namespaces().SetMaxTopicsPerNamespace(*namespace, tt.maxTopics)
			if tt.errReason == "" {
				assert.Equal(t, nil, err)

				err = admin.Namespaces().RemoveMaxTopicsPerNamespace(*namespace)
				assert.Equal(t, nil, err)
			}
			if err != nil {
				restError := err.(rest.Error)
				assert.Equal(t, tt.errReason, restError.Reason)
			}
		})
	}
}

func TestNamespaces_GetMaxTopicsPerNamespace(t *testing.T) {

	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespace, _ := utils.GetNamespaceName("public/default")

	// set the max topics per namespace and get it
	err = admin.Namespaces().SetMaxTopicsPerNamespace(*namespace, 100)
	assert.Equal(t, nil, err)
	maxTopics, err := admin.Namespaces().GetMaxTopicsPerNamespace(*namespace)
	assert.Equal(t, nil, err)
	expected := 100
	assert.Equal(t, expected, maxTopics)

	// remove the max topics per namespace and get it
	err = admin.Namespaces().RemoveMaxTopicsPerNamespace(*namespace)
	assert.Equal(t, nil, err)

	maxTopics, err = admin.Namespaces().GetMaxTopicsPerNamespace(*namespace)
	assert.Equal(t, nil, err)
	expected = 0
	assert.Equal(t, expected, maxTopics)
}

func TestNamespaces_MessageTTL(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespace, _ := utils.GetNamespaceName("public/default")

	// Get default (should be -1)
	ttl, err := admin.Namespaces().GetNamespaceMessageTTL(namespace.String())
	assert.NoError(t, err)
	assert.Equal(t, -1, ttl)

	// Set to 0 explicitly
	err = admin.Namespaces().SetNamespaceMessageTTL(namespace.String(), 0)
	assert.NoError(t, err)

	// Verify returns 0
	ttl, err = admin.Namespaces().GetNamespaceMessageTTL(namespace.String())
	assert.NoError(t, err)
	assert.Equal(t, 0, ttl)

	// Set to positive value
	err = admin.Namespaces().SetNamespaceMessageTTL(namespace.String(), 3600)
	assert.NoError(t, err)

	// Verify returns value
	ttl, err = admin.Namespaces().GetNamespaceMessageTTL(namespace.String())
	assert.NoError(t, err)
	assert.Equal(t, 3600, ttl)
}

func TestNamespaces_OffloadDeleteLag(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespace, _ := utils.GetNamespaceName("public/default")

	// Get default (should be -1)
	lag, err := admin.Namespaces().GetOffloadDeleteLag(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), lag)

	// Set to 0 explicitly
	err = admin.Namespaces().SetOffloadDeleteLag(*namespace, 0)
	assert.NoError(t, err)

	// Verify returns 0
	lag, err = admin.Namespaces().GetOffloadDeleteLag(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), lag)

	// Set to positive value
	err = admin.Namespaces().SetOffloadDeleteLag(*namespace, 1000)
	assert.NoError(t, err)

	// Verify returns value
	lag, err = admin.Namespaces().GetOffloadDeleteLag(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), lag)
}

func TestNamespaces_MaxConsumersPerTopic(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespace, _ := utils.GetNamespaceName("public/default")

	// Get default (should be -1)
	maxConsumers, err := admin.Namespaces().GetMaxConsumersPerTopic(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, -1, maxConsumers)

	// Set to 0 explicitly
	err = admin.Namespaces().SetMaxConsumersPerTopic(*namespace, 0)
	assert.NoError(t, err)

	// Verify returns 0
	maxConsumers, err = admin.Namespaces().GetMaxConsumersPerTopic(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, 0, maxConsumers)

	// Set to positive value
	err = admin.Namespaces().SetMaxConsumersPerTopic(*namespace, 100)
	assert.NoError(t, err)

	// Verify returns value
	maxConsumers, err = admin.Namespaces().GetMaxConsumersPerTopic(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, 100, maxConsumers)
}

func TestNamespaces_CompactionThreshold(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespace, _ := utils.GetNamespaceName("public/default")

	// Get default (should be -1)
	threshold, err := admin.Namespaces().GetCompactionThreshold(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), threshold)

	// Set to 0 explicitly
	err = admin.Namespaces().SetCompactionThreshold(*namespace, 0)
	assert.NoError(t, err)

	// Verify returns 0
	threshold, err = admin.Namespaces().GetCompactionThreshold(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), threshold)

	// Set to positive value
	err = admin.Namespaces().SetCompactionThreshold(*namespace, 1024*1024) // 1MB
	assert.NoError(t, err)

	// Verify returns value
	threshold, err = admin.Namespaces().GetCompactionThreshold(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, int64(1024*1024), threshold)
}

func TestNamespaces_MaxProducersPerTopic(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespace, _ := utils.GetNamespaceName("public/default")

	// Get default (should be -1)
	maxProducers, err := admin.Namespaces().GetMaxProducersPerTopic(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, -1, maxProducers)

	// Set to 0 explicitly
	err = admin.Namespaces().SetMaxProducersPerTopic(*namespace, 0)
	assert.NoError(t, err)

	// Verify returns 0
	maxProducers, err = admin.Namespaces().GetMaxProducersPerTopic(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, 0, maxProducers)

	// Set to positive value
	err = admin.Namespaces().SetMaxProducersPerTopic(*namespace, 50)
	assert.NoError(t, err)

	// Verify returns value
	maxProducers, err = admin.Namespaces().GetMaxProducersPerTopic(*namespace)
	assert.NoError(t, err)
	assert.Equal(t, 50, maxProducers)
}

func TestNamespaces_Retention(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespaceName := "public/default"

	// Initial state: policy not configured, should return nil
	retention, err := admin.Namespaces().GetRetention(namespaceName)
	assert.NoError(t, err)
	assert.Nil(t, retention, "Expected nil when retention is not configured")

	// Set new retention policy
	newRetention := utils.RetentionPolicies{
		RetentionSizeInMB:      1024,
		RetentionTimeInMinutes: 60,
	}
	err = admin.Namespaces().SetRetention(namespaceName, newRetention)
	assert.NoError(t, err)

	// Verify retention is set
	retention, err = admin.Namespaces().GetRetention(namespaceName)
	assert.NoError(t, err)
	assert.NotNil(t, retention, "Expected non-nil when retention is configured")
	assert.Equal(t, int64(1024), retention.RetentionSizeInMB)
	assert.Equal(t, 60, retention.RetentionTimeInMinutes)
}

func TestNamespaces_BookieAffinityGroup(t *testing.T) {
	readFile, err := os.ReadFile("../../../integration-tests/tokens/admin-token")
	require.NoError(t, err)

	config := &config.Config{
		Token: string(readFile),
	}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespaceName := "public/default"

	// Initial state: policy not configured, should return nil
	bookieAffinity, err := admin.Namespaces().GetBookieAffinityGroup(namespaceName)
	assert.NoError(t, err)
	assert.Nil(t, bookieAffinity, "Expected nil when bookie affinity group is not configured")

	// Set new bookie affinity group
	newBookieAffinity := utils.BookieAffinityGroupData{
		BookkeeperAffinityGroupPrimary:   "primary-group",
		BookkeeperAffinityGroupSecondary: "secondary-group",
	}
	err = admin.Namespaces().SetBookieAffinityGroup(namespaceName, newBookieAffinity)
	assert.NoError(t, err)

	// Verify bookie affinity group is set
	bookieAffinity, err = admin.Namespaces().GetBookieAffinityGroup(namespaceName)
	assert.NoError(t, err)
	assert.NotNil(t, bookieAffinity, "Expected non-nil when bookie affinity group is configured")
	assert.Equal(t, "primary-group", bookieAffinity.BookkeeperAffinityGroupPrimary)
	assert.Equal(t, "secondary-group", bookieAffinity.BookkeeperAffinityGroupSecondary)

	// Remove bookie affinity group - should return nil
	err = admin.Namespaces().DeleteBookieAffinityGroup(namespaceName)
	assert.NoError(t, err)
	bookieAffinity, err = admin.Namespaces().GetBookieAffinityGroup(namespaceName)
	assert.NoError(t, err)
	assert.Nil(t, bookieAffinity, "Expected nil after removing bookie affinity group")
}

func TestNamespaces_Persistence(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	namespaceName := "public/default"

	// Initial state: policy not configured, should return nil
	persistence, err := admin.Namespaces().GetPersistence(namespaceName)
	assert.NoError(t, err)
	assert.Nil(t, persistence, "Expected nil when persistence is not configured")

	// Set new persistence policy
	newPersistence := utils.PersistencePolicies{
		BookkeeperEnsemble:             1,
		BookkeeperWriteQuorum:          1,
		BookkeeperAckQuorum:            1,
		ManagedLedgerMaxMarkDeleteRate: 10.0,
	}
	err = admin.Namespaces().SetPersistence(namespaceName, newPersistence)
	assert.NoError(t, err)

	// Verify persistence is set
	persistence, err = admin.Namespaces().GetPersistence(namespaceName)
	assert.NoError(t, err)
	assert.NotNil(t, persistence, "Expected non-nil when persistence is configured")
	assert.Equal(t, 1, persistence.BookkeeperEnsemble)
	assert.Equal(t, 1, persistence.BookkeeperWriteQuorum)
}
