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

package utils

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intPtr(value int) *int {
	return &value
}

func TestNewBatchingConfig(t *testing.T) {
	config := NewBatchingConfig()

	require.NotNil(t, config)
	assert.IsType(t, &BatchingConfig{}, config)

	// The broker applies these when a producer carries no batching configuration
	// (BatchingUtils.convertFromSpec(null)), so the constructor is a no-op change in behaviour.
	assert.True(t, config.Enabled)
	require.NotNil(t, config.BatchingMaxPublishDelayMs)
	assert.Equal(t, DefaultBatchingMaxPublishDelayMs, *config.BatchingMaxPublishDelayMs)

	assert.Nil(t, config.RoundRobinRouterBatchingPartitionSwitchFrequency)
	assert.Nil(t, config.BatchingMaxMessages)
	assert.Nil(t, config.BatchingMaxBytes)
	assert.Empty(t, config.BatchBuilder)
}

func TestBatchingConfigJSONSerialization(t *testing.T) {
	tests := []struct {
		name     string
		config   BatchingConfig
		expected string
	}{
		{
			// enabled carries no omitempty: BatchingUtils.convert() reads it as a primitive with no
			// fallback, so a payload omitting it can leave batching off.
			name:     "zero value still emits enabled",
			config:   BatchingConfig{},
			expected: `{"enabled":false}`,
		},
		{
			name:     "explicitly disabled",
			config:   BatchingConfig{Enabled: false},
			expected: `{"enabled":false}`,
		},
		{
			name:     "constructor defaults",
			config:   *NewBatchingConfig(),
			expected: `{"enabled":true,"batchingMaxPublishDelayMs":10}`,
		},
		{
			// Serialized faithfully, but the broker ignores a non-positive delay and falls back to
			// its 10ms default; Enabled is what turns batching off. Asserted so the wire format
			// stays honest about what the caller asked for.
			name: "explicit zero max publish delay is serialized",
			config: BatchingConfig{
				Enabled:                   true,
				BatchingMaxPublishDelayMs: intPtr(0),
			},
			expected: `{"enabled":true,"batchingMaxPublishDelayMs":0}`,
		},
		{
			name: "all fields",
			config: BatchingConfig{
				Enabled:                   true,
				BatchingMaxPublishDelayMs: intPtr(5),
				RoundRobinRouterBatchingPartitionSwitchFrequency: intPtr(20),
				BatchingMaxMessages: intPtr(100),
				BatchingMaxBytes:    intPtr(131072),
				BatchBuilder:        "KEY_BASED",
			},
			//nolint:lll
			expected: `{"enabled":true,"batchingMaxPublishDelayMs":5,"roundRobinRouterBatchingPartitionSwitchFrequency":20,"batchingMaxMessages":100,"batchingMaxBytes":131072,"batchBuilder":"KEY_BASED"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.config)
			require.NoError(t, err)
			assert.JSONEq(t, test.expected, string(data))
		})
	}
}

func TestBatchingConfigRoundTrip(t *testing.T) {
	original := BatchingConfig{
		Enabled:                   true,
		BatchingMaxPublishDelayMs: intPtr(0),
		BatchingMaxMessages:       intPtr(100),
		BatchBuilder:              "KEY_BASED",
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded BatchingConfig
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, original, decoded)
}

func TestBatchingConfigUnmarshalDistinguishesUnsetFromZero(t *testing.T) {
	var absent BatchingConfig
	require.NoError(t, json.Unmarshal([]byte(`{"enabled":true}`), &absent))
	assert.Nil(t, absent.BatchingMaxMessages, "an absent field must stay nil")

	var zero BatchingConfig
	require.NoError(t, json.Unmarshal([]byte(`{"enabled":true,"batchingMaxMessages":0}`), &zero))
	require.NotNil(t, zero.BatchingMaxMessages, "an explicit zero must not read as unset")
	assert.Equal(t, 0, *zero.BatchingMaxMessages)
}

func TestProducerConfigOmitsBatchingConfigWhenUnset(t *testing.T) {
	// Requests to brokers older than 4.1.0 must be byte-identical to before this field existed.
	data, err := json.Marshal(ProducerConfig{})
	require.NoError(t, err)
	assert.NotContains(t, string(data), "batchingConfig")
}

func TestProducerConfigIncludesBatchingConfigWhenSet(t *testing.T) {
	config := ProducerConfig{BatchingConfig: NewBatchingConfig()}

	data, err := json.Marshal(config)
	require.NoError(t, err)

	var decoded ProducerConfig
	require.NoError(t, json.Unmarshal(data, &decoded))

	require.NotNil(t, decoded.BatchingConfig)
	assert.True(t, decoded.BatchingConfig.Enabled)
	require.NotNil(t, decoded.BatchingConfig.BatchingMaxPublishDelayMs)
	assert.Equal(t, DefaultBatchingMaxPublishDelayMs, *decoded.BatchingConfig.BatchingMaxPublishDelayMs)
}
