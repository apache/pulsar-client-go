package pulsar

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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewKeySharedPolicySticky(t *testing.T) {
	ksp, err := NewKeySharedPolicySticky([]int{0, 10000, 10001, 19999, 30000, 59999})
	assert.Nil(t, err)
	assert.NotNil(t, ksp)
	assert.Equal(t, ksp.Mode, KeySharedPolicyModeSticky)
}

func TestValidateHashRanges(t *testing.T) {
	type args struct {
		hashRanges []int
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "normal hash ranges",
			args: args{
				hashRanges: []int{0, 10, 11, 19, 20, 1000, 2000, 3000, 10000, 20000},
			},
		},
		{
			name: "abnormal hash ranges over max range",
			args: args{
				hashRanges: []int{1, 300, 2000, 66666},
			},
			wantErr: true,
		},
		{
			name: "abnormal hash ranges x1 < x2",
			args: args{
				hashRanges: []int{10, 1, 2, 10},
			},
			wantErr: true,
		},
		{
			name: "abnormal hash ranges not in pairs",
			args: args{
				hashRanges: []int{1, 3, 5, 10, 11},
			},
			wantErr: true,
		},
		{
			name: "abnormal hash ranges overlap 1",
			args: args{
				hashRanges: []int{1, 3, 3, 6, 9, 12},
			},
			wantErr: true,
		},
		{
			name: "abnormal hash ranges overlap 2",
			args: args{
				hashRanges: []int{1, 8, 6, 12, 7, 10},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateHashRanges(tt.args.hashRanges); (err != nil) != tt.wantErr {
				t.Errorf("validateHashRanges() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
