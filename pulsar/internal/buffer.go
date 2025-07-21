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
	"encoding/binary"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

type BuffersPool interface {
	GetBuffer(initSize int) Buffer
	Put(buf Buffer)
}

// Buffer is a variable-sized buffer of bytes with Read and Write methods.
// The zero value for Buffer is an empty buffer ready to use.
type Buffer interface {
	ReadableBytes() uint32

	WritableBytes() uint32

	// Capacity returns the capacity of the buffer's underlying byte slice,
	// that is, the total space allocated for the buffer's data.
	Capacity() uint32

	IsWritable() bool

	Read(size uint32) []byte

	Skip(size uint32)

	Get(readerIndex uint32, size uint32) []byte

	ReadableSlice() []byte

	WritableSlice() []byte

	// WrittenBytes advance the writer index when data was written in a slice
	WrittenBytes(size uint32)

	// MoveToFront copy the available portion of data at the beginning of the buffer
	MoveToFront()

	ReadUint16() uint16
	ReadUint32() uint32

	WriteUint16(n uint16)
	WriteUint32(n uint32)

	WriterIndex() uint32
	ReaderIndex() uint32

	Write(s []byte)

	Put(writerIdx uint32, s []byte)
	PutUint32(n uint32, writerIdx uint32)

	Resize(newSize uint32)
	ResizeIfNeeded(spaceNeeded uint32)

	Retain()
	Release()
	RefCnt() int64

	// Clear will clear the current buffer data.
	Clear()
}

type bufferPoolImpl struct {
	sync.Pool
}

func NewBufferPool() BuffersPool {
	return &bufferPoolImpl{
		Pool: sync.Pool{},
	}
}

func (p *bufferPoolImpl) GetBuffer(initSize int) Buffer {
	sendingBuffersCount.Inc()
	b, ok := p.Get().(*buffer)
	if ok {
		b.Clear()
	} else {
		b = &buffer{
			data:      make([]byte, initSize),
			readerIdx: 0,
			writerIdx: 0,
		}
	}
	b.pool = p
	b.Retain()
	return b
}

func (p *bufferPoolImpl) Put(buf Buffer) {
	sendingBuffersCount.Dec()
	if b, ok := buf.(*buffer); ok {
		p.Pool.Put(b)
	}
}

type buffer struct {
	data []byte

	readerIdx uint32
	writerIdx uint32

	refCnt atomic.Int64
	pool   BuffersPool
}

// NewBuffer creates and initializes a new Buffer using buf as its initial contents.
func NewBuffer(size int) Buffer {
	return &buffer{
		data:      make([]byte, size),
		readerIdx: 0,
		writerIdx: 0,
	}
}

func NewBufferWrapper(buf []byte) Buffer {
	return &buffer{
		data:      buf,
		readerIdx: 0,
		writerIdx: uint32(len(buf)),
	}
}

func (b *buffer) ReadableBytes() uint32 {
	return b.writerIdx - b.readerIdx
}

func (b *buffer) WritableBytes() uint32 {
	return uint32(cap(b.data)) - b.writerIdx
}

func (b *buffer) Capacity() uint32 {
	return uint32(cap(b.data))
}

func (b *buffer) IsWritable() bool {
	return b.WritableBytes() > 0
}

func (b *buffer) Read(size uint32) []byte {
	// Check []byte slice size, avoid slice bounds out of range
	if b.readerIdx+size > uint32(len(b.data)) {
		log.Errorf("The input size [%d] > byte slice of data size [%d]", b.readerIdx+size, len(b.data))
		return nil
	}
	res := b.data[b.readerIdx : b.readerIdx+size]
	b.readerIdx += size
	return res
}

func (b *buffer) Skip(size uint32) {
	b.readerIdx += size
}

func (b *buffer) Get(readerIdx uint32, size uint32) []byte {
	return b.data[readerIdx : readerIdx+size]
}

func (b *buffer) ReadableSlice() []byte {
	return b.data[b.readerIdx:b.writerIdx]
}

func (b *buffer) WritableSlice() []byte {
	return b.data[b.writerIdx:]
}

func (b *buffer) WrittenBytes(size uint32) {
	b.writerIdx += size
}

func (b *buffer) WriterIndex() uint32 {
	return b.writerIdx
}

func (b *buffer) ReaderIndex() uint32 {
	return b.readerIdx
}

func (b *buffer) MoveToFront() {
	size := b.ReadableBytes()
	copy(b.data, b.Read(size))
	b.readerIdx = 0
	b.writerIdx = size
}

func (b *buffer) Resize(newSize uint32) {
	newData := make([]byte, newSize)
	size := b.ReadableBytes()
	copy(newData, b.Read(size))
	b.data = newData
	b.readerIdx = 0
	b.writerIdx = size
}

func (b *buffer) ResizeIfNeeded(spaceNeeded uint32) {
	if b.WritableBytes() < spaceNeeded {
		capacityNeeded := uint32(cap(b.data)) + spaceNeeded
		minCapacityIncrease := uint32(cap(b.data) * 3 / 2)
		if capacityNeeded < minCapacityIncrease {
			capacityNeeded = minCapacityIncrease
		}
		b.Resize(capacityNeeded)
	}
}

func (b *buffer) ReadUint32() uint32 {
	return binary.BigEndian.Uint32(b.Read(4))
}

func (b *buffer) ReadUint16() uint16 {
	return binary.BigEndian.Uint16(b.Read(2))
}

func (b *buffer) WriteUint32(n uint32) {
	b.ResizeIfNeeded(4)
	binary.BigEndian.PutUint32(b.WritableSlice(), n)
	b.writerIdx += 4
}

func (b *buffer) PutUint32(n uint32, idx uint32) {
	binary.BigEndian.PutUint32(b.data[idx:], n)
}

func (b *buffer) WriteUint16(n uint16) {
	b.ResizeIfNeeded(2)
	binary.BigEndian.PutUint16(b.WritableSlice(), n)
	b.writerIdx += 2
}

func (b *buffer) Write(s []byte) {
	b.ResizeIfNeeded(uint32(len(s)))
	copy(b.WritableSlice(), s)
	b.writerIdx += uint32(len(s))
}

func (b *buffer) Put(writerIdx uint32, s []byte) {
	copy(b.data[writerIdx:], s)
}

func (b *buffer) Retain() {
	b.refCnt.Add(1)
}

func (b *buffer) Release() {
	if b.refCnt.Add(-1) == 0 {
		if b.pool != nil {
			b.pool.Put(b)
		}
	}
}

func (b *buffer) RefCnt() int64 {
	return b.refCnt.Load()
}

func (b *buffer) Clear() {
	b.readerIdx = 0
	b.writerIdx = 0
	b.refCnt.Store(0)
}
