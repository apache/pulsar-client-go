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
			expected: AutoUpdateDisabled,
			wantErr:  false,
		},
		{
			name:     "BACKWARD uppercase",
			input:    "BACKWARD",
			expected: Backward,
			wantErr:  false,
		},
		{
			name:     "Forward mixed case",
			input:    "forward",
			expected: Forward,
			wantErr:  false,
		},
		{
			name:     "FULL uppercase",
			input:    "FULL",
			expected: Full,
			wantErr:  false,
		},
		{
			name:     "alwayscompatible lowercase",
			input:    "alwayscompatible",
			expected: AlwaysCompatible,
			wantErr:  false,
		},
		{
			name:     "BACKWARDTRANSITIVE uppercase",
			input:    "BACKWARDTRANSITIVE",
			expected: BackwardTransitive,
			wantErr:  false,
		},
		{
			name:     "forwardtransitive lowercase",
			input:    "forwardtransitive",
			expected: ForwardTransitive,
			wantErr:  false,
		},
		{
			name:     "FULLTRANSITIVE uppercase",
			input:    "FULLTRANSITIVE",
			expected: FullTransitive,
			wantErr:  false,
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
			input:    "Undefined",
			expected: SchemaCompatibilityStrategyUndefined,
			wantErr:  false,
		},
		{
			name:     "AlwaysIncompatible",
			input:    "AlwaysIncompatible",
			expected: SchemaCompatibilityStrategyAlwaysIncompatible,
			wantErr:  false,
		},
		{
			name:     "AlwaysCompatible",
			input:    "AlwaysCompatible",
			expected: SchemaCompatibilityStrategyAlwaysCompatible,
			wantErr:  false,
		},
		{
			name:     "Backward",
			input:    "Backward",
			expected: SchemaCompatibilityStrategyBackward,
			wantErr:  false,
		},
		{
			name:     "Forward",
			input:    "Forward",
			expected: SchemaCompatibilityStrategyForward,
			wantErr:  false,
		},
		{
			name:     "Full",
			input:    "Full",
			expected: SchemaCompatibilityStrategyFull,
			wantErr:  false,
		},
		{
			name:     "BackwardTransitive",
			input:    "BackwardTransitive",
			expected: SchemaCompatibilityStrategyBackwardTransitive,
			wantErr:  false,
		},
		{
			name:     "ForwardTransitive",
			input:    "ForwardTransitive",
			expected: SchemaCompatibilityStrategyForwardTransitive,
			wantErr:  false,
		},
		{
			name:     "FullTransitive",
			input:    "FullTransitive",
			expected: SchemaCompatibilityStrategyFullTransitive,
			wantErr:  false,
		},
		// Valid cases - case insensitive
		{
			name:     "undefined lowercase",
			input:    "undefined",
			expected: SchemaCompatibilityStrategyUndefined,
			wantErr:  false,
		},
		{
			name:     "ALWAYSINCOMPATIBLE uppercase",
			input:    "ALWAYSINCOMPATIBLE",
			expected: SchemaCompatibilityStrategyAlwaysIncompatible,
			wantErr:  false,
		},
		{
			name:     "alwayscompatible lowercase",
			input:    "alwayscompatible",
			expected: SchemaCompatibilityStrategyAlwaysCompatible,
			wantErr:  false,
		},
		{
			name:     "BACKWARD uppercase",
			input:    "BACKWARD",
			expected: SchemaCompatibilityStrategyBackward,
			wantErr:  false,
		},
		{
			name:     "forward lowercase",
			input:    "forward",
			expected: SchemaCompatibilityStrategyForward,
			wantErr:  false,
		},
		{
			name:     "FULL uppercase",
			input:    "FULL",
			expected: SchemaCompatibilityStrategyFull,
			wantErr:  false,
		},
		{
			name:     "backwardtransitive lowercase",
			input:    "backwardtransitive",
			expected: SchemaCompatibilityStrategyBackwardTransitive,
			wantErr:  false,
		},
		{
			name:     "FORWARDTRANSITIVE uppercase",
			input:    "FORWARDTRANSITIVE",
			expected: SchemaCompatibilityStrategyForwardTransitive,
			wantErr:  false,
		},
		{
			name:     "fulltransitive lowercase",
			input:    "fulltransitive",
			expected: SchemaCompatibilityStrategyFullTransitive,
			wantErr:  false,
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
			expected: "Undefined",
		},
		{
			name:     "AlwaysIncompatible",
			strategy: SchemaCompatibilityStrategyAlwaysIncompatible,
			expected: "AlwaysIncompatible",
		},
		{
			name:     "AlwaysCompatible",
			strategy: SchemaCompatibilityStrategyAlwaysCompatible,
			expected: "AlwaysCompatible",
		},
		{
			name:     "Backward",
			strategy: SchemaCompatibilityStrategyBackward,
			expected: "Backward",
		},
		{
			name:     "Forward",
			strategy: SchemaCompatibilityStrategyForward,
			expected: "Forward",
		},
		{
			name:     "Full",
			strategy: SchemaCompatibilityStrategyFull,
			expected: "Full",
		},
		{
			name:     "BackwardTransitive",
			strategy: SchemaCompatibilityStrategyBackwardTransitive,
			expected: "BackwardTransitive",
		},
		{
			name:     "ForwardTransitive",
			strategy: SchemaCompatibilityStrategyForwardTransitive,
			expected: "ForwardTransitive",
		},
		{
			name:     "FullTransitive",
			strategy: SchemaCompatibilityStrategyFullTransitive,
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
