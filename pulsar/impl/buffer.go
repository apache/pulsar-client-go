package impl

import (
	"encoding/binary"
)

type Buffer interface {
	ReadableBytes() uint32

	WritableBytes() uint32

	Capacity() uint32

	IsWritable() bool

	Read(size uint32) []byte

	Get(readerIndex uint32, size uint32) []byte

	ReadableSlice() []byte

	WritableSlice() []byte

	// Advance the writer index when data was written in a slice
	WrittenBytes(size uint32)

	// Copy the available portion of data at the beginning of the buffer
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

	Clear()
}

type buffer struct {
	data []byte

	readerIdx uint32
	writerIdx uint32
}

func NewBuffer(size int) Buffer {
	return &buffer{
		data:      make([]byte, size),
		readerIdx: 0,
		writerIdx: 0,
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
	res := b.data[b.readerIdx : b.readerIdx+size]
	b.readerIdx += size
	return res
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

func (b *buffer) resizeIfNeeded(spaceNeeded int) {
	if b.WritableBytes() < uint32(spaceNeeded) {
		capacityNeeded := uint32(cap(b.data) + spaceNeeded)
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
	b.resizeIfNeeded(4)
	binary.BigEndian.PutUint32(b.WritableSlice(), n)
	b.writerIdx += 4
}

func (b *buffer) PutUint32(n uint32, idx uint32) {
	binary.BigEndian.PutUint32(b.data[idx:], n)
}

func (b *buffer) WriteUint16(n uint16) {
	b.resizeIfNeeded(2)
	binary.BigEndian.PutUint16(b.WritableSlice(), n)
	b.writerIdx += 2
}

func (b *buffer) Write(s []byte) {
	b.resizeIfNeeded(len(s))
	copy(b.WritableSlice(), s)
	b.writerIdx += uint32(len(s))
}

func (b *buffer) Put(writerIdx uint32, s []byte) {
	copy(b.data[writerIdx:], s)
}

func (b *buffer) Clear() {
	b.readerIdx = 0
	b.writerIdx = 0
}
