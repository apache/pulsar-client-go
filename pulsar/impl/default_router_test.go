package impl

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)


func TestDefaultRouter(t *testing.T) {

	var currentClock uint64

	router := NewDefaultRouter(func() uint64 {
		return currentClock
	}, JavaStringHash, 10*time.Nanosecond)

	// partition index should not change with time
	p1 := router("my-key", 100)
	p2 := router("my-key", 100)

	assert.Equal(t, p1, p2)

	currentClock = 100
	p3 := router("my-key", 100)

	assert.Equal(t, p1, p3)

	// With no key, we should give the same partition for a given time range
	pr1 := router("", 100)
	pr2 := router("", 100)
	assert.Equal(t, pr1, pr2)

	currentClock = 101
	pr3 := router("", 100)
	assert.Equal(t, pr1, pr3)

	currentClock = 102
	pr4 := router("", 100)
	assert.Equal(t, pr1, pr4)

	currentClock = 111
	pr5 := router("", 100)
	assert.NotEqual(t, pr1, pr5)

	currentClock = 112
	pr6 := router("", 100)
	assert.Equal(t, pr5, pr6)
}

func TestDefaultRouterNoPartitions(t *testing.T) {

	router := NewDefaultRouter(NewSystemClock(), JavaStringHash, 10*time.Nanosecond)

	// partition index should not change with time
	p1 := router("", 1)
	p2 := router("my-key", 1)
	p3 := router("my-key-2", 1)
	p4 := router("my-key-3", 1)

	assert.Equal(t, 0, p1)
	assert.Equal(t, 0, p2)
	assert.Equal(t, 0, p3)
	assert.Equal(t, 0, p4)
}

