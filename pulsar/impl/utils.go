package impl

import (
	"sync/atomic"
	"time"
)

func TimestampMillis(t time.Time) uint64 {
	return uint64(t.UnixNano()) / uint64(time.Millisecond)
}

// Perform atomic read and update
func GetAndAdd(n *uint64, diff uint64) uint64 {
	for {
		v := *n
		if atomic.CompareAndSwapUint64(n, v, v+diff) {
			return v
		}
	}
}
