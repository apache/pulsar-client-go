//
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
//

package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testProvider struct {
	str string

	hash uint32
}

var javaHashValues = []testProvider{
	{"", 0x0,},
	{"hello", 0x5e918d2},
	{"test", 0x364492},
}

var murmurHashValues = []testProvider{
	{"", 0x0},
	{"hello", 0x248bfa47},
	{"test", 0x3a6bd213},
}

func TestJavaHash(t *testing.T) {
	for _, p := range javaHashValues {
		t.Run(p.str, func(t *testing.T) {
			assert.Equal(t, p.hash, JavaStringHash(p.str))
		})
	}
}

func TestMurmurHash(t *testing.T) {
	for _, p := range murmurHashValues {
		t.Run(p.str, func(t *testing.T) {
			assert.Equal(t, p.hash, Murmur3_32Hash(p.str))
		})
	}
}
