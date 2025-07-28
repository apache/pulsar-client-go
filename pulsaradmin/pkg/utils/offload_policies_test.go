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
)

func TestNewOffloadPolicies(t *testing.T) {
	// Test that constructor returns non-nil pointer
	policies := NewOffloadPolicies()
	assert.NotNil(t, policies)

	// Test that default values are set correctly
	assert.Equal(t, 2, policies.ManagedLedgerOffloadMaxThreads)
	assert.Equal(t, int64(-1), policies.ManagedLedgerOffloadThresholdInBytes)
	assert.Equal(t, int64(14400000), policies.ManagedLedgerOffloadDeletionLagInMillis) // 4 hours
	assert.Equal(t, int64(-1), policies.ManagedLedgerOffloadAutoTriggerSizeThresholdBytes)

	// Test that map is initialized and empty
	assert.NotNil(t, policies.ManagedLedgerOffloadDriverMetadata)
	assert.Equal(t, 0, len(policies.ManagedLedgerOffloadDriverMetadata))

	// Test that other fields are zero values
	assert.Equal(t, "", policies.ManagedLedgerOffloadDriver)
	assert.Equal(t, "", policies.S3ManagedLedgerOffloadBucket)
	assert.Equal(t, "", policies.S3ManagedLedgerOffloadRegion)
	assert.Equal(t, "", policies.S3ManagedLedgerOffloadServiceEndpoint)
	assert.Equal(t, "", policies.S3ManagedLedgerOffloadCredentialID)
	assert.Equal(t, "", policies.S3ManagedLedgerOffloadCredentialSecret)
	assert.Equal(t, "", policies.S3ManagedLedgerOffloadRole)
	assert.Equal(t, "", policies.S3ManagedLedgerOffloadRoleSessionName)
	assert.Equal(t, "", policies.OffloadersDirectory)

	// Test that it returns pointer to struct
	assert.IsType(t, &OffloadPolicies{}, policies)
}

func TestOffloadPolicies_JSONSerialization(t *testing.T) {
	tests := []struct {
		name     string
		policies OffloadPolicies
		expected string
	}{
		//nolint:lll
		{
			name:     "Default constructor serialization (omitempty behavior)",
			policies: *NewOffloadPolicies(),
			expected: `{"managedLedgerOffloadMaxThreads":2,"managedLedgerOffloadThresholdInBytes":-1,"managedLedgerOffloadDeletionLagInMillis":14400000,"managedLedgerOffloadAutoTriggerSizeThresholdBytes":-1}`,
		},
		//nolint:lll
		{
			name: "Full object serialization",
			policies: OffloadPolicies{
				ManagedLedgerOffloadDriver:                        "s3",
				ManagedLedgerOffloadMaxThreads:                    4,
				ManagedLedgerOffloadThresholdInBytes:              1000000,
				ManagedLedgerOffloadDeletionLagInMillis:           7200000,
				ManagedLedgerOffloadAutoTriggerSizeThresholdBytes: 500000,
				S3ManagedLedgerOffloadBucket:                      "test-bucket",
				S3ManagedLedgerOffloadRegion:                      "us-west-2",
				S3ManagedLedgerOffloadServiceEndpoint:             "https://s3.us-west-2.amazonaws.com",
				S3ManagedLedgerOffloadCredentialID:                "access-key",
				S3ManagedLedgerOffloadCredentialSecret:            "secret-key",
				S3ManagedLedgerOffloadRole:                        "test-role",
				S3ManagedLedgerOffloadRoleSessionName:             "test-session",
				OffloadersDirectory:                               "/opt/offloaders",
				ManagedLedgerOffloadDriverMetadata:                map[string]string{"key1": "value1", "key2": "value2"},
			},
			expected: `{"managedLedgerOffloadDriver":"s3","managedLedgerOffloadMaxThreads":4,"managedLedgerOffloadThresholdInBytes":1000000,"managedLedgerOffloadDeletionLagInMillis":7200000,"managedLedgerOffloadAutoTriggerSizeThresholdBytes":500000,"s3ManagedLedgerOffloadBucket":"test-bucket","s3ManagedLedgerOffloadRegion":"us-west-2","s3ManagedLedgerOffloadServiceEndpoint":"https://s3.us-west-2.amazonaws.com","s3ManagedLedgerOffloadCredentialId":"access-key","s3ManagedLedgerOffloadCredentialSecret":"secret-key","s3ManagedLedgerOffloadRole":"test-role","s3ManagedLedgerOffloadRoleSessionName":"test-session","offloadersDirectory":"/opt/offloaders","managedLedgerOffloadDriverMetadata":{"key1":"value1","key2":"value2"}}`,
		},
		{
			name:     "Empty struct serialization (zero values omitted)",
			policies: OffloadPolicies{},
			expected: `{}`,
		},
		{
			name: "Non-empty map serialization",
			policies: OffloadPolicies{
				ManagedLedgerOffloadDriverMetadata: map[string]string{"key": "value"},
			},
			expected: `{"managedLedgerOffloadDriverMetadata":{"key":"value"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonData, err := json.Marshal(tt.policies)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, string(jsonData))
		})
	}
}

func TestOffloadPolicies_JSONDeserialization(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		expected OffloadPolicies
		wantErr  bool
	}{
		//nolint:lll
		{
			name:     "Complete JSON deserialization",
			jsonData: `{"managedLedgerOffloadDriver":"s3","managedLedgerOffloadMaxThreads":4,"managedLedgerOffloadThresholdInBytes":1000000,"managedLedgerOffloadDeletionLagInMillis":7200000,"managedLedgerOffloadAutoTriggerSizeThresholdBytes":500000,"s3ManagedLedgerOffloadBucket":"test-bucket","s3ManagedLedgerOffloadRegion":"us-west-2","s3ManagedLedgerOffloadServiceEndpoint":"https://s3.us-west-2.amazonaws.com","s3ManagedLedgerOffloadCredentialId":"access-key","s3ManagedLedgerOffloadCredentialSecret":"secret-key","s3ManagedLedgerOffloadRole":"test-role","s3ManagedLedgerOffloadRoleSessionName":"test-session","offloadersDirectory":"/opt/offloaders","managedLedgerOffloadDriverMetadata":{"key1":"value1","key2":"value2"}}`,
			expected: OffloadPolicies{
				ManagedLedgerOffloadDriver:                        "s3",
				ManagedLedgerOffloadMaxThreads:                    4,
				ManagedLedgerOffloadThresholdInBytes:              1000000,
				ManagedLedgerOffloadDeletionLagInMillis:           7200000,
				ManagedLedgerOffloadAutoTriggerSizeThresholdBytes: 500000,
				S3ManagedLedgerOffloadBucket:                      "test-bucket",
				S3ManagedLedgerOffloadRegion:                      "us-west-2",
				S3ManagedLedgerOffloadServiceEndpoint:             "https://s3.us-west-2.amazonaws.com",
				S3ManagedLedgerOffloadCredentialID:                "access-key",
				S3ManagedLedgerOffloadCredentialSecret:            "secret-key",
				S3ManagedLedgerOffloadRole:                        "test-role",
				S3ManagedLedgerOffloadRoleSessionName:             "test-session",
				OffloadersDirectory:                               "/opt/offloaders",
				ManagedLedgerOffloadDriverMetadata:                map[string]string{"key1": "value1", "key2": "value2"},
			},
			wantErr: false,
		},
		//nolint:lll
		{
			name:     "Partial JSON deserialization",
			jsonData: `{"managedLedgerOffloadDriver":"s3","managedLedgerOffloadMaxThreads":4,"s3ManagedLedgerOffloadBucket":"test-bucket"}`,
			expected: OffloadPolicies{
				ManagedLedgerOffloadDriver:     "s3",
				ManagedLedgerOffloadMaxThreads: 4,
				S3ManagedLedgerOffloadBucket:   "test-bucket",
				// Other fields should be zero values
			},
			wantErr: false,
		},
		{
			name:     "Empty JSON object",
			jsonData: `{}`,
			expected: OffloadPolicies{
				// All fields should be zero values
			},
			wantErr: false,
		},
		{
			name:     "JSON with metadata map",
			jsonData: `{"managedLedgerOffloadDriverMetadata":{"env":"prod","version":"1.0"}}`,
			expected: OffloadPolicies{
				ManagedLedgerOffloadDriverMetadata: map[string]string{"env": "prod", "version": "1.0"},
			},
			wantErr: false,
		},
		{
			name:     "JSON with extra fields ignored",
			jsonData: `{"managedLedgerOffloadDriver":"s3","extraField":"ignored","managedLedgerOffloadMaxThreads":4}`,
			expected: OffloadPolicies{
				ManagedLedgerOffloadDriver:     "s3",
				ManagedLedgerOffloadMaxThreads: 4,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var policies OffloadPolicies
			err := json.Unmarshal([]byte(tt.jsonData), &policies)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, policies)
			}
		})
	}
}

func TestOffloadPolicies_FieldModification(t *testing.T) {
	// Test field modification on constructor-created instance
	policies := NewOffloadPolicies()

	// Test modifying string fields
	policies.ManagedLedgerOffloadDriver = "s3"
	assert.Equal(t, "s3", policies.ManagedLedgerOffloadDriver)

	policies.S3ManagedLedgerOffloadBucket = "test-bucket"
	assert.Equal(t, "test-bucket", policies.S3ManagedLedgerOffloadBucket)

	// Test modifying int field
	policies.ManagedLedgerOffloadMaxThreads = 8
	assert.Equal(t, 8, policies.ManagedLedgerOffloadMaxThreads)

	// Test modifying int64 fields
	policies.ManagedLedgerOffloadThresholdInBytes = 5000000
	assert.Equal(t, int64(5000000), policies.ManagedLedgerOffloadThresholdInBytes)

	policies.ManagedLedgerOffloadDeletionLagInMillis = 3600000 // 1 hour
	assert.Equal(t, int64(3600000), policies.ManagedLedgerOffloadDeletionLagInMillis)

	// Test field modification on manually created instance
	manual := &OffloadPolicies{
		ManagedLedgerOffloadDriver:     "gcs",
		ManagedLedgerOffloadMaxThreads: 4,
		S3ManagedLedgerOffloadBucket:   "initial-bucket",
	}

	assert.Equal(t, "gcs", manual.ManagedLedgerOffloadDriver)
	assert.Equal(t, 4, manual.ManagedLedgerOffloadMaxThreads)
	assert.Equal(t, "initial-bucket", manual.S3ManagedLedgerOffloadBucket)

	// Modify the manually created instance
	manual.ManagedLedgerOffloadDriver = "azure"
	manual.ManagedLedgerOffloadMaxThreads = 6

	assert.Equal(t, "azure", manual.ManagedLedgerOffloadDriver)
	assert.Equal(t, 6, manual.ManagedLedgerOffloadMaxThreads)
	assert.Equal(t, "initial-bucket", manual.S3ManagedLedgerOffloadBucket)
}

func TestOffloadPolicies_MapOperations(t *testing.T) {
	policies := NewOffloadPolicies()

	// Test initial state of map
	assert.NotNil(t, policies.ManagedLedgerOffloadDriverMetadata)
	assert.Equal(t, 0, len(policies.ManagedLedgerOffloadDriverMetadata))

	// Test adding entries to map
	policies.ManagedLedgerOffloadDriverMetadata["key1"] = "value1"
	policies.ManagedLedgerOffloadDriverMetadata["key2"] = "value2"

	assert.Equal(t, 2, len(policies.ManagedLedgerOffloadDriverMetadata))
	assert.Equal(t, "value1", policies.ManagedLedgerOffloadDriverMetadata["key1"])
	assert.Equal(t, "value2", policies.ManagedLedgerOffloadDriverMetadata["key2"])

	// Test modifying existing entry
	policies.ManagedLedgerOffloadDriverMetadata["key1"] = "modified_value1"
	assert.Equal(t, "modified_value1", policies.ManagedLedgerOffloadDriverMetadata["key1"])

	// Test deleting entry
	delete(policies.ManagedLedgerOffloadDriverMetadata, "key2")
	assert.Equal(t, 1, len(policies.ManagedLedgerOffloadDriverMetadata))
	_, exists := policies.ManagedLedgerOffloadDriverMetadata["key2"]
	assert.False(t, exists)

	// Test with nil map (should not panic but won't work as expected)
	nilMapPolicies := &OffloadPolicies{}
	assert.Nil(t, nilMapPolicies.ManagedLedgerOffloadDriverMetadata)

	// Initialize the map manually
	nilMapPolicies.ManagedLedgerOffloadDriverMetadata = make(map[string]string)
	nilMapPolicies.ManagedLedgerOffloadDriverMetadata["test"] = "value"
	assert.Equal(t, "value", nilMapPolicies.ManagedLedgerOffloadDriverMetadata["test"])
}

func TestOffloadPolicies_InvalidJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
	}{
		{
			name:     "Invalid JSON syntax",
			jsonData: `{"managedLedgerOffloadDriver":"s3"`,
		},
		{
			name:     "Invalid int value",
			jsonData: `{"managedLedgerOffloadMaxThreads":"invalid"}`,
		},
		{
			name:     "Invalid int64 value",
			jsonData: `{"managedLedgerOffloadThresholdInBytes":"invalid"}`,
		},
		{
			name:     "Invalid JSON structure",
			jsonData: `[{"managedLedgerOffloadDriver":"s3"}]`,
		},
		{
			name:     "Invalid map structure",
			jsonData: `{"managedLedgerOffloadDriverMetadata":"invalid"}`,
		},
		{
			name:     "Completely invalid JSON",
			jsonData: `invalid json`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var policies OffloadPolicies
			err := json.Unmarshal([]byte(tt.jsonData), &policies)
			assert.Error(t, err)
		})
	}
}
