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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestSynchronizedBufferPool_Clone_returnsNewlyAllocatedBuffer(t *testing.T) {
	pool := newBufferPool(&sync.Pool{})

	buffer := NewBuffer(1024)
	buffer.Write([]byte{1, 2, 3})

	res := pool.Clone(buffer)
	assert.Equal(t, []byte{1, 2, 3}, res.ReadableSlice())
}

func TestSynchronizedBufferPool_Clone_returnsRecycledBuffer(t *testing.T) {
	pool := newBufferPool(&sync.Pool{})

	for range 100 {
		buffer := NewBuffer(1024)
		buffer.Write([]byte{1, 2, 3})
		pool.Put(buffer)
	}

	buffer := NewBuffer(1024)
	buffer.Write([]byte{1, 2, 3})

	res := pool.Clone(buffer)
	assert.Equal(t, []byte{1, 2, 3}, res.ReadableSlice())
}

// BenchmarkBufferPool_Clone demonstrates the cloning of a buffer without
// allocation if the pool is filled making the process very efficient.
func BenchmarkBufferPool_Clone(b *testing.B) {
	pool := GetDefaultBufferPool()
	buffer := NewBuffer(1024)
	buffer.Write(make([]byte, 1024))

	for range b.N {
		newBuffer := pool.Clone(buffer)
		pool.Put(newBuffer)
	}
}

// --- Helpers

type capturingPool struct {
	buffers []Buffer
}

func (p *capturingPool) Get() Buffer {
	if len(p.buffers) > 0 {
		value := p.buffers[0]
		p.buffers = p.buffers[1:]
		return value
	}
	return nil
}

func (p *capturingPool) Put(value Buffer) {
	p.buffers = append(p.buffers, value)
}

func (p *capturingPool) Clone(value Buffer) Buffer {
	return value
}
