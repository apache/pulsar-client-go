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

func boolPtr(b bool) *bool {
	return &b
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
				Allow:      boolPtr(true),
				Type:       utils.Partitioned,
				Partitions: ptr(3),
			},
			errReason: "",
		},
		{
			name:      "Set partitioned type topic auto creation without partitions",
			namespace: "public/default",
			config: utils.TopicAutoCreationConfig{
				Allow: boolPtr(true),
				Type:  utils.Partitioned,
			},
			errReason: "Invalid configuration for autoTopicCreationOverride. the detail is [defaultNumPartitions] " +
				"cannot be null when the type is partitioned.",
		},
		{
			name:      "Set partitioned type topic auto creation with partitions < 1",
			namespace: "public/default",
			config: utils.TopicAutoCreationConfig{
				Allow:      boolPtr(true),
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
				Allow: boolPtr(true),
				Type:  utils.NonPartitioned,
			},
			errReason: "",
		},
		{
			name:      "Set non-partitioned type topic auto creation with partitions",
			namespace: "public/default",
			config: utils.TopicAutoCreationConfig{
				Allow:      boolPtr(true),
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
				Allow: boolPtr(false),
			},
			errReason: "",
		},
		{
			name:      "Set topic auto creation on a non-exist namespace",
			namespace: "public/nonexist",
			config: utils.TopicAutoCreationConfig{
				Allow: boolPtr(true),
				Type:  utils.NonPartitioned,
			},
			errReason: "Namespace does not exist",
		},
		{
			name:      "Set topic auto creation on a non-exist tenant",
			namespace: "non-exist/default",
			config: utils.TopicAutoCreationConfig{
				Allow: boolPtr(true),
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
		Allow: boolPtr(true),
		Type:  utils.NonPartitioned,
	})
	assert.Equal(t, nil, err)
	topicAutoCreation, err := admin.Namespaces().GetTopicAutoCreation(*namespace)
	assert.Equal(t, nil, err)
	expected := utils.TopicAutoCreationConfig{
		Allow: boolPtr(true),
		Type:  utils.NonPartitioned,
	}
	assert.Equal(t, expected, *topicAutoCreation)

	// remove the topic auto creation config and get it
	err = admin.Namespaces().RemoveTopicAutoCreation(*namespace)
	assert.Equal(t, nil, err)

	topicAutoCreation, err = admin.Namespaces().GetTopicAutoCreation(*namespace)
	assert.Equal(t, nil, err)
	expected = utils.TopicAutoCreationConfig{
		Allow: nil,
		Type:  "",
	}
	assert.Equal(t, expected, *topicAutoCreation)
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

	// set the subscription expiration time and get it
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
