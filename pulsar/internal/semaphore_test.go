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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSemaphore(t *testing.T) {
	s := NewSemaphore(3)

	const n = 10

	wg := sync.WaitGroup{}
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			assert.True(t, s.Acquire(context.Background()))
			time.Sleep(100 * time.Millisecond)
			s.Release()
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestSemaphore_TryAcquire(t *testing.T) {
	s := NewSemaphore(1)

	assert.True(t, s.Acquire(context.Background()))

	assert.False(t, s.TryAcquire())

	s.Release()

	assert.True(t, s.TryAcquire())
	assert.False(t, s.TryAcquire())
	s.Release()
}

func TestSemaphore_ContextExpire(t *testing.T) {
	s := NewSemaphore(1)

	assert.True(t, s.Acquire(context.Background()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, s.Acquire(ctx))

	assert.False(t, s.TryAcquire())
	s.Release()

	assert.True(t, s.TryAcquire())
}

// A batch acquire is admitted as long as the semaphore has not yet reached its
// limit, even when the batch overshoots it, mirroring the memory limit
// controller. Otherwise a message split into more chunks than the limit could
// never be sent.
func TestSemaphore_AcquireNOvershoot(t *testing.T) {
	s := NewSemaphore(2)

	assert.True(t, s.AcquireN(context.Background(), 5))

	// The limit has been reached, so nothing else is admitted.
	assert.False(t, s.TryAcquireN(1))
	assert.False(t, s.TryAcquire())

	// Releasing the permits one by one frees the semaphore only once usage
	// drops below the limit again.
	for i := 0; i < 3; i++ {
		s.Release()
		assert.False(t, s.TryAcquire())
	}
	s.Release()
	assert.True(t, s.TryAcquire())
}

func TestSemaphore_TryAcquireNBoundary(t *testing.T) {
	s := NewSemaphore(3)

	assert.True(t, s.TryAcquireN(2))
	// One permit below the limit: still admitted, even though 2 more overshoot.
	assert.True(t, s.TryAcquireN(2))
	// Usage is now 4, at or above the limit.
	assert.False(t, s.TryAcquireN(1))

	// n <= 0 never blocks and never consumes permits.
	assert.True(t, s.TryAcquireN(0))
	assert.True(t, s.AcquireN(context.Background(), 0))
}

// A waiter must be woken by Release, and Release must never block when nobody
// is waiting, even if the semaphore is over its limit.
func TestSemaphore_ReleaseDoesNotBlockWithoutWaiters(t *testing.T) {
	s := NewSemaphore(1)
	assert.True(t, s.AcquireN(context.Background(), 4))

	done := make(chan struct{})
	go func() {
		for i := 0; i < 4; i++ {
			s.Release()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Release blocked although no goroutine is waiting")
	}
	assert.True(t, s.TryAcquire())
}

func TestSemaphore_WaiterWokenInOrder(t *testing.T) {
	s := NewSemaphore(1)
	assert.True(t, s.Acquire(context.Background()))

	order := make(chan int, 2)
	for i := 1; i <= 2; i++ {
		i := i
		go func() {
			assert.True(t, s.AcquireN(context.Background(), 3))
			order <- i
		}()
		// Give the goroutine time to enqueue so the order is deterministic.
		time.Sleep(50 * time.Millisecond)
	}

	select {
	case <-order:
		t.Fatal("waiter was admitted while the semaphore was full")
	case <-time.After(100 * time.Millisecond):
	}

	s.Release()
	select {
	case got := <-order:
		assert.Equal(t, 1, got)
	case <-time.After(5 * time.Second):
		t.Fatal("first waiter was not woken")
	}

	// The second waiter needs usage to fall below the limit again.
	select {
	case <-order:
		t.Fatal("second waiter was admitted while the semaphore was full")
	case <-time.After(100 * time.Millisecond):
	}
	for i := 0; i < 3; i++ {
		s.Release()
	}
	select {
	case got := <-order:
		assert.Equal(t, 2, got)
	case <-time.After(5 * time.Second):
		t.Fatal("second waiter was not woken")
	}
}

func TestSemaphore_AcquireNContextExpireWhileWaiting(t *testing.T) {
	s := NewSemaphore(1)
	assert.True(t, s.Acquire(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.False(t, s.AcquireN(ctx, 2))

	// The cancelled waiter must not have consumed or leaked any permit.
	s.Release()
	assert.True(t, s.TryAcquire())
	s.Release()
}

// Cancelling waiters while permits are being released must neither leak
// permits nor leave goroutines blocked.
func TestSemaphore_AcquireNCancelRace(t *testing.T) {
	s := NewSemaphore(2)

	const workers = 50
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i%5+1)*time.Millisecond)
			defer cancel()
			if s.AcquireN(ctx, int32(i%4+1)) {
				time.Sleep(time.Millisecond)
				for j := 0; j < i%4+1; j++ {
					s.Release()
				}
			}
		}()
	}
	wg.Wait()

	// Every granted permit was released, so the semaphore is empty again.
	assert.True(t, s.TryAcquireN(1))
	assert.True(t, s.TryAcquireN(1))
	assert.False(t, s.TryAcquireN(1))
}
