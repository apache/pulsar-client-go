package impl

import (
	"math/rand"
	"time"
)

type defaultRouter struct {
	clock            Clock
	shiftIdx         uint32
	maxBatchingDelay time.Duration
	hashFunc         func(string) uint32
}

type Clock func() uint64

func NewSystemClock() Clock {
	return func() uint64 {
		return uint64(time.Now().UnixNano())
	}
}

func NewDefaultRouter(clock Clock, hashFunc func(string) uint32, maxBatchingDelay time.Duration) func(string, uint32) int {
	state := &defaultRouter{
		clock:            clock,
		shiftIdx:         rand.Uint32(),
		maxBatchingDelay: maxBatchingDelay,
		hashFunc:         hashFunc,
	}

	return func(key string, numPartitions uint32) int {
		if numPartitions == 1 {
			// When there are no partitions, don't even bother
			return 0
		}

		if key != "" {
			// When a key is specified, use the hash of that key
			return int(state.hashFunc(key) % numPartitions)
		}

		// If there's no key, we do round-robin across partition, sticking with a given
		// partition for a certain amount of time, to ensure we can have a decent amount
		// of batching of the messages.
		//
		//currentMs / maxBatchingDelayMs + startPtnIdx
		n := uint32(state.clock()/uint64(maxBatchingDelay.Nanoseconds())) + state.shiftIdx
		return int(n % numPartitions)
	}
}
