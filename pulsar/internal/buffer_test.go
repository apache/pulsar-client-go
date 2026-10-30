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

package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuffer(t *testing.T) {
	b := NewBuffer(1024)
	assert.Equal(t, uint32(0), b.ReadableBytes())
	assert.Equal(t, uint32(1024), b.WritableBytes())
	assert.Equal(t, uint32(1024), b.Capacity())

	b.Write([]byte("hello"))
	assert.Equal(t, uint32(5), b.ReadableBytes())
	assert.Equal(t, uint32(1019), b.WritableBytes())
	assert.Equal(t, uint32(1024), b.Capacity())
}

// Read refuses a size larger than the readable remainder, and must keep doing so
// when readerIdx plus that size overflows uint32. Sizes reaching Read come from
// the wire, so the overflowing case is reachable rather than theoretical.
func TestBufferReadRefusesOversizedRead(t *testing.T) {
	for _, tc := range []struct {
		name      string
		readFirst uint32
		size      uint32
	}{
		{"size beyond the buffer", 0, 2048},
		{"size one past the readable remainder", 8, 1024 - 8 + 1},
		{"readerIdx plus size overflows uint32", 8, 4294967295},
		{"maximum size on an untouched buffer", 0, 4294967295},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBuffer(1024)
			b.Write(make([]byte, 1024))

			if tc.readFirst > 0 {
				require.NotNil(t, b.Read(tc.readFirst))
			}
			readerIdxBefore := b.ReaderIndex()

			assert.Nil(t, b.Read(tc.size), "oversized read should be refused")
			assert.Equal(t, readerIdxBefore, b.ReaderIndex(), "a refused read must not advance readerIdx")
		})
	}
}

// Skip does not bound readerIdx, so it can leave it past the end of the data.
// Read must refuse rather than panic in that state.
func TestBufferReadAfterSkipPastEnd(t *testing.T) {
	b := NewBuffer(1024)
	b.Write(make([]byte, 1024))

	b.Skip(2048)

	assert.Nil(t, b.Read(1))
}
