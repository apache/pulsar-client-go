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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSchemaAutoUpdateCompatibilityStrategy(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected SchemaAutoUpdateCompatibilityStrategy
		wantErr  bool
	}{
		// Valid cases - exact match
		{
			name:     "AutoUpdateDisabled",
			input:    "AutoUpdateDisabled",
			expected: AutoUpdateDisabled,
			wantErr:  false,
		},
		{
			name:     "Backward",
			input:    "Backward",
			expected: Backward,
			wantErr:  false,
		},
		{
			name:     "Forward",
			input:    "Forward",
			expected: Forward,
			wantErr:  false,
		},
		{
			name:     "Full",
			input:    "Full",
			expected: Full,
			wantErr:  false,
		},
		{
			name:     "AlwaysCompatible",
			input:    "AlwaysCompatible",
			expected: AlwaysCompatible,
			wantErr:  false,
		},
		{
			name:     "BackwardTransitive",
			input:    "BackwardTransitive",
			expected: BackwardTransitive,
			wantErr:  false,
		},
		{
			name:     "ForwardTransitive",
			input:    "ForwardTransitive",
			expected: ForwardTransitive,
			wantErr:  false,
		},
		{
			name:     "FullTransitive",
			input:    "FullTransitive",
			expected: FullTransitive,
			wantErr:  false,
		},
		// Valid cases - case insensitive
		{
			name:     "autoupdatedisabled lowercase",
			input:    "autoupdatedisabled",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "BACKWARD uppercase",
			input:    "BACKWARD",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "Forward mixed case",
			input:    "forward",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "FULL uppercase",
			input:    "FULL",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "alwayscompatible lowercase",
			input:    "alwayscompatible",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "BACKWARDTRANSITIVE uppercase",
			input:    "BACKWARDTRANSITIVE",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "forwardtransitive lowercase",
			input:    "forwardtransitive",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "FULLTRANSITIVE uppercase",
			input:    "FULLTRANSITIVE",
			expected: "",
			wantErr:  true,
		},
		// Invalid cases
		{
			name:     "Invalid strategy",
			input:    "InvalidStrategy",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "Random string",
			input:    "RandomString",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "Partial match",
			input:    "Back",
			expected: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseSchemaAutoUpdateCompatibilityStrategy(tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Empty(t, result)
				assert.Contains(t, err.Error(), "Invalid schema auto update compatibility strategy")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParseSchemaCompatibilityStrategy(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected SchemaCompatibilityStrategy
		wantErr  bool
	}{
		// Valid cases - exact match
		{
			name:     "Undefined",
			input:    "UNDEFINED",
			expected: SchemaCompatibilityStrategyUndefined,
			wantErr:  false,
		},
		{
			name:     "AlwaysIncompatible",
			input:    "ALWAYS_INCOMPATIBLE",
			expected: SchemaCompatibilityStrategyAlwaysIncompatible,
			wantErr:  false,
		},
		{
			name:     "AlwaysCompatible",
			input:    "ALWAYS_COMPATIBLE",
			expected: SchemaCompatibilityStrategyAlwaysCompatible,
			wantErr:  false,
		},
		{
			name:     "Backward",
			input:    "BACKWARD",
			expected: SchemaCompatibilityStrategyBackward,
			wantErr:  false,
		},
		{
			name:     "Forward",
			input:    "FORWARD",
			expected: SchemaCompatibilityStrategyForward,
			wantErr:  false,
		},
		{
			name:     "Full",
			input:    "FULL",
			expected: SchemaCompatibilityStrategyFull,
			wantErr:  false,
		},
		{
			name:     "BackwardTransitive",
			input:    "BACKWARD_TRANSITIVE",
			expected: SchemaCompatibilityStrategyBackwardTransitive,
			wantErr:  false,
		},
		{
			name:     "ForwardTransitive",
			input:    "FORWARD_TRANSITIVE",
			expected: SchemaCompatibilityStrategyForwardTransitive,
			wantErr:  false,
		},
		{
			name:     "FullTransitive",
			input:    "FULL_TRANSITIVE",
			expected: SchemaCompatibilityStrategyFullTransitive,
			wantErr:  false,
		},
		// Valid cases - case insensitive
		{
			name:     "undefined lowercase",
			input:    "undefined",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "ALWAYSINCOMPATIBLE uppercase",
			input:    "ALWAYSINCOMPATIBLE",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "alwayscompatible lowercase",
			input:    "alwayscompatible",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "forward lowercase",
			input:    "forward",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "backwardtransitive lowercase",
			input:    "backwardtransitive",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "FORWARDTRANSITIVE uppercase",
			input:    "FORWARDTRANSITIVE",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "fulltransitive lowercase",
			input:    "fulltransitive",
			expected: "",
			wantErr:  true,
		},
		// Invalid cases
		{
			name:     "Invalid strategy",
			input:    "InvalidStrategy",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "Random string",
			input:    "RandomString",
			expected: "",
			wantErr:  true,
		},
		{
			name:     "Partial match",
			input:    "Back",
			expected: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseSchemaCompatibilityStrategy(tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Empty(t, result)
				assert.Contains(t, err.Error(), "Invalid schema compatibility strategy")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestSchemaAutoUpdateCompatibilityStrategy_String(t *testing.T) {
	tests := []struct {
		name     string
		strategy SchemaAutoUpdateCompatibilityStrategy
		expected string
	}{
		{
			name:     "AutoUpdateDisabled",
			strategy: AutoUpdateDisabled,
			expected: "AutoUpdateDisabled",
		},
		{
			name:     "Backward",
			strategy: Backward,
			expected: "Backward",
		},
		{
			name:     "Forward",
			strategy: Forward,
			expected: "Forward",
		},
		{
			name:     "Full",
			strategy: Full,
			expected: "Full",
		},
		{
			name:     "AlwaysCompatible",
			strategy: AlwaysCompatible,
			expected: "AlwaysCompatible",
		},
		{
			name:     "BackwardTransitive",
			strategy: BackwardTransitive,
			expected: "BackwardTransitive",
		},
		{
			name:     "ForwardTransitive",
			strategy: ForwardTransitive,
			expected: "ForwardTransitive",
		},
		{
			name:     "FullTransitive",
			strategy: FullTransitive,
			expected: "FullTransitive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.strategy.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSchemaCompatibilityStrategy_String(t *testing.T) {
	tests := []struct {
		name     string
		strategy SchemaCompatibilityStrategy
		expected string
	}{
		{
			name:     "Undefined",
			strategy: SchemaCompatibilityStrategyUndefined,
			expected: "UNDEFINED",
		},
		{
			name:     "AlwaysIncompatible",
			strategy: SchemaCompatibilityStrategyAlwaysIncompatible,
			expected: "ALWAYS_INCOMPATIBLE",
		},
		{
			name:     "AlwaysCompatible",
			strategy: SchemaCompatibilityStrategyAlwaysCompatible,
			expected: "ALWAYS_COMPATIBLE",
		},
		{
			name:     "Backward",
			strategy: SchemaCompatibilityStrategyBackward,
			expected: "BACKWARD",
		},
		{
			name:     "Forward",
			strategy: SchemaCompatibilityStrategyForward,
			expected: "FORWARD",
		},
		{
			name:     "Full",
			strategy: SchemaCompatibilityStrategyFull,
			expected: "FULL",
		},
		{
			name:     "BackwardTransitive",
			strategy: SchemaCompatibilityStrategyBackwardTransitive,
			expected: "BACKWARD_TRANSITIVE",
		},
		{
			name:     "ForwardTransitive",
			strategy: SchemaCompatibilityStrategyForwardTransitive,
			expected: "FORWARD_TRANSITIVE",
		},
		{
			name:     "FullTransitive",
			strategy: SchemaCompatibilityStrategyFullTransitive,
			expected: "FULL_TRANSITIVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.strategy.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}
