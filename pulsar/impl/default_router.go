package impl

import (
	"time"
)

type defaultRouter struct {
	partitionIdx     uint32
	maxBatchingDelay time.Duration
}

func NewDefaultRouter(maxBatchingDelay time.Duration) func(uint32) int {
	// TODO: XXX
	//state := &defaultRouter{
	//	partitionIdx:     rand.Uint32(),
	//	maxBatchingDelay: maxBatchingDelay,
	//}

	return func(numPartitions uint32) int {
		if numPartitions == 1 {
			return 0
		}

		return -1
	}
}
