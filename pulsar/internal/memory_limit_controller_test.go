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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLimit(t *testing.T) {

	mlc := NewMemoryLimitController(100)

	for i := 0; i < 101; i++ {
		assert.True(t, mlc.TryReserveMemory(1))
	}

	assert.False(t, mlc.TryReserveMemory(1))
	assert.Equal(t, int64(101), mlc.CurrentUsage())
	assert.InDelta(t, 1.01, mlc.CurrentUsagePercent(), 0.000001)

	mlc.ReleaseMemory(1)
	assert.Equal(t, int64(100), mlc.CurrentUsage())
	assert.InDelta(t, 1.0, mlc.CurrentUsagePercent(), 0.000001)

	assert.True(t, mlc.TryReserveMemory(1))
	assert.Equal(t, int64(101), mlc.CurrentUsage())

	mlc.ForceReserveMemory(99)
	assert.False(t, mlc.TryReserveMemory(1))
	assert.Equal(t, int64(200), mlc.CurrentUsage())
	assert.InDelta(t, 2.0, mlc.CurrentUsagePercent(), 0.000001)

	mlc.ReleaseMemory(50)
	assert.False(t, mlc.TryReserveMemory(1))
	assert.Equal(t, int64(150), mlc.CurrentUsage())
	assert.InDelta(t, 1.5, mlc.CurrentUsagePercent(), 0.000001)
}

func TestDisableLimit(t *testing.T) {
	mlc := NewMemoryLimitController(-1)
	assert.True(t, mlc.TryReserveMemory(1000000))
	assert.True(t, mlc.ReserveMemory(context.Background(), 1000000))
	mlc.ReleaseMemory(1000000)
	assert.Equal(t, int64(1000000), mlc.CurrentUsage())
}

func TestMultiGoroutineTryReserveMem(t *testing.T) {
	mlc := NewMemoryLimitController(10000)

	// Multi goroutine try reserve memory.
	wg := sync.WaitGroup{}

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			for i := 0; i < 1000; i++ {
				assert.True(t, mlc.TryReserveMemory(1))
			}
			wg.Done()
		}()
	}
	assert.True(t, mlc.TryReserveMemory(1))
	wg.Wait()
	assert.False(t, mlc.TryReserveMemory(1))
	assert.Equal(t, int64(10001), mlc.CurrentUsage())
	assert.InDelta(t, 1.0001, mlc.CurrentUsagePercent(), 0.000001)
}

func TestReserveWithContext(t *testing.T) {
	mlc := NewMemoryLimitController(100)
	assert.True(t, mlc.TryReserveMemory(101))
	gorNum := 10

	// Reserve ctx timeout
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(gorNum)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	for i := 0; i < gorNum; i++ {
		go func() {
			assert.False(t, mlc.ReserveMemory(ctx, 1))
			waitGroup.Done()
		}()
	}
	waitGroup.Wait()
	assert.Equal(t, int64(101), mlc.CurrentUsage())

	// Reserve ctx cancel
	waitGroup.Add(gorNum)
	cancelCtx, cancel := context.WithCancel(context.Background())
	for i := 0; i < gorNum; i++ {
		go func() {
			assert.False(t, mlc.ReserveMemory(cancelCtx, 1))
			waitGroup.Done()
		}()
	}
	cancel()
	waitGroup.Wait()
	assert.Equal(t, int64(101), mlc.CurrentUsage())
}

func TestBlocking(t *testing.T) {
	mlc := NewMemoryLimitController(100)
	assert.True(t, mlc.TryReserveMemory(101))
	assert.Equal(t, int64(101), mlc.CurrentUsage())
	assert.InDelta(t, 1.01, mlc.CurrentUsagePercent(), 0.000001)

	gorNum := 10
	chs := make([]chan int, gorNum)
	for i := 0; i < gorNum; i++ {
		chs[i] = make(chan int, 1)
		go reserveMemory(mlc, chs[i])
	}

	// The threads are blocked since the quota is full
	for i := 0; i < gorNum; i++ {
		assert.False(t, awaitCh(chs[i]))
	}
	assert.Equal(t, int64(101), mlc.CurrentUsage())

	mlc.ReleaseMemory(int64(gorNum))
	for i := 0; i < gorNum; i++ {
		assert.True(t, awaitCh(chs[i]))
	}
	assert.Equal(t, int64(101), mlc.CurrentUsage())
}

func TestStepRelease(t *testing.T) {
	mlc := NewMemoryLimitController(100)
	assert.True(t, mlc.TryReserveMemory(101))
	assert.Equal(t, int64(101), mlc.CurrentUsage())
	assert.InDelta(t, 1.01, mlc.CurrentUsagePercent(), 0.000001)

	gorNum := 10
	ch := make(chan int, 1)
	for i := 0; i < gorNum; i++ {
		go reserveMemory(mlc, ch)
	}

	// The threads are blocked since the quota is full
	assert.False(t, awaitCh(ch))
	assert.Equal(t, int64(101), mlc.CurrentUsage())

	for i := 0; i < gorNum; i++ {
		mlc.ReleaseMemory(1)
		assert.True(t, awaitCh(ch))
		assert.False(t, awaitCh(ch))
	}
	assert.Equal(t, int64(101), mlc.CurrentUsage())
}

func TestRegisterTrigger(t *testing.T) {
	mlc := NewMemoryLimitController(100)
	triggeredLowThreshold := false
	triggeredHighThreshold := false
	finishCh := make(chan struct{}, 2)

	mlc.RegisterTrigger(0.5, func() {
		triggeredLowThreshold = true
		finishCh <- struct{}{}
	})

	mlc.RegisterTrigger(0.95, func() {
		triggeredHighThreshold = true
		finishCh <- struct{}{}
	})

	mlc.TryReserveMemory(50)
	timer := time.NewTimer(time.Millisecond * 100)
	select {
	case <-finishCh:
		assert.True(t, triggeredLowThreshold)
		assert.False(t, triggeredHighThreshold)
	case <-timer.C:
		assert.Error(t, fmt.Errorf("trigger timeout"))
	}

	mlc.TryReserveMemory(45)
	timer.Reset(time.Millisecond * 100)
	select {
	case <-finishCh:
		assert.True(t, triggeredLowThreshold)
		assert.True(t, triggeredHighThreshold)
	case <-timer.C:
		assert.Error(t, fmt.Errorf("trigger timeout"))
	}

	triggeredHighThreshold = false
	mlc.ReleaseMemory(1)
	assert.False(t, triggeredHighThreshold)
	mlc.ForceReserveMemory(1)
	timer.Reset(time.Millisecond * 100)
	select {
	case <-finishCh:
		assert.True(t, triggeredHighThreshold)
	case <-timer.C:
		assert.Error(t, fmt.Errorf("trigger timeout"))
	}
}

func reserveMemory(mlc MemoryLimitController, ch chan int) {
	mlc.ReserveMemory(context.Background(), 1)
	ch <- 1
}

func awaitCh(ch chan int) bool {
	select {
	case <-ch:
		return true
	case <-time.After(100 * time.Millisecond):
		return false
	}
}
