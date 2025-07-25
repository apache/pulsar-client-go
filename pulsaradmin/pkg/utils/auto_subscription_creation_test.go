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

func TestNewAutoSubscriptionCreationOverride(t *testing.T) {
	// Test that constructor returns non-nil pointer
	override := NewAutoSubscriptionCreationOverride()
	assert.NotNil(t, override)

	// Test that default value is set correctly
	assert.Equal(t, false, override.AllowAutoSubscriptionCreation)

	// Test that it returns pointer to struct
	assert.IsType(t, &AutoSubscriptionCreationOverride{}, override)
}

func TestAutoSubscriptionCreationOverride_JSONSerialization(t *testing.T) {
	tests := []struct {
		name     string
		override AutoSubscriptionCreationOverride
		expected string
	}{
		{
			name:     "False value serialization",
			override: AutoSubscriptionCreationOverride{AllowAutoSubscriptionCreation: false},
			expected: `{"allowAutoSubscriptionCreation":false}`,
		},
		{
			name:     "True value serialization",
			override: AutoSubscriptionCreationOverride{AllowAutoSubscriptionCreation: true},
			expected: `{"allowAutoSubscriptionCreation":true}`,
		},
		{
			name:     "Default constructor serialization",
			override: *NewAutoSubscriptionCreationOverride(),
			expected: `{"allowAutoSubscriptionCreation":false}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonData, err := json.Marshal(tt.override)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, string(jsonData))
		})
	}
}

func TestAutoSubscriptionCreationOverride_JSONDeserialization(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		expected AutoSubscriptionCreationOverride
		wantErr  bool
	}{
		{
			name:     "False value deserialization",
			jsonData: `{"allowAutoSubscriptionCreation":false}`,
			expected: AutoSubscriptionCreationOverride{AllowAutoSubscriptionCreation: false},
			wantErr:  false,
		},
		{
			name:     "True value deserialization",
			jsonData: `{"allowAutoSubscriptionCreation":true}`,
			expected: AutoSubscriptionCreationOverride{AllowAutoSubscriptionCreation: true},
			wantErr:  false,
		},
		{
			name:     "Empty JSON object",
			jsonData: `{}`,
			expected: AutoSubscriptionCreationOverride{AllowAutoSubscriptionCreation: false},
			wantErr:  false,
		},
		{
			name:     "Extra fields ignored",
			jsonData: `{"allowAutoSubscriptionCreation":true,"extraField":"ignored"}`,
			expected: AutoSubscriptionCreationOverride{AllowAutoSubscriptionCreation: true},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var override AutoSubscriptionCreationOverride
			err := json.Unmarshal([]byte(tt.jsonData), &override)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, override)
			}
		})
	}
}

func TestAutoSubscriptionCreationOverride_FieldModification(t *testing.T) {
	// Test field modification on constructor-created instance
	override := NewAutoSubscriptionCreationOverride()

	// Initial state should be false
	assert.Equal(t, false, override.AllowAutoSubscriptionCreation)

	// Modify to true
	override.AllowAutoSubscriptionCreation = true
	assert.Equal(t, true, override.AllowAutoSubscriptionCreation)

	// Modify back to false
	override.AllowAutoSubscriptionCreation = false
	assert.Equal(t, false, override.AllowAutoSubscriptionCreation)

	// Test field modification on manually created instance
	manual := &AutoSubscriptionCreationOverride{
		AllowAutoSubscriptionCreation: true,
	}
	assert.Equal(t, true, manual.AllowAutoSubscriptionCreation)

	manual.AllowAutoSubscriptionCreation = false
	assert.Equal(t, false, manual.AllowAutoSubscriptionCreation)
}

func TestAutoSubscriptionCreationOverride_InvalidJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
	}{
		{
			name:     "Invalid JSON syntax",
			jsonData: `{"allowAutoSubscriptionCreation":false`,
		},
		{
			name:     "Invalid boolean value",
			jsonData: `{"allowAutoSubscriptionCreation":"invalid"}`,
		},
		{
			name:     "Invalid JSON structure",
			jsonData: `[{"allowAutoSubscriptionCreation":true}]`,
		},
		{
			name:     "Completely invalid JSON",
			jsonData: `invalid json`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var override AutoSubscriptionCreationOverride
			err := json.Unmarshal([]byte(tt.jsonData), &override)
			assert.Error(t, err)
		})
	}
}
