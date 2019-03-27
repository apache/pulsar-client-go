package impl

import (
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
