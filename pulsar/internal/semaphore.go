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
	"container/list"
	"context"
	"sync"

	log "github.com/sirupsen/logrus"
)

// Semaphore limits the number of in-flight operations.
//
// maxPermits is a throttle rather than a hard cap, in the same way as the
// MemoryLimitController: a request is admitted as long as the permits already
// handed out have not reached maxPermits, even if admitting it pushes the
// total above maxPermits. This lets a single request that needs more permits
// than maxPermits (for example a chunked message with more chunks than
// MaxPendingMessages) proceed as one unit instead of waiting for permits that
// can never become available. Later requests wait until enough permits have
// been released to drop the total back under maxPermits.
type Semaphore interface {
	// Acquire a permit, if one is available and returns immediately,
	// reducing the number of available permits by one.
	Acquire(ctx context.Context) bool

	// AcquireN acquires n permits as a single atomic operation. The call is
	// admitted as soon as the number of permits already held is below
	// maxPermits, even if n permits take the total above maxPermits.
	// It returns false if ctx expires first, in which case no permit is held.
	AcquireN(ctx context.Context, n int32) bool

	// Try to acquire a permit. The method will return immediately
	// with a `true` if it was possible to acquire a permit and
	// `false` otherwise.
	TryAcquire() bool

	// TryAcquireN is the non-blocking variant of AcquireN: it acquires n
	// permits atomically and returns true, or returns false immediately and
	// holds no permit.
	TryAcquireN(n int32) bool

	// Release a permit, returning it to the semaphore.
	// Release a permit, increasing the number of available permits by
	// one.  If any threads are trying to acquire a permit, then one is
	// selected and given the permit that was just released.  That thread
	// is (re)enabled for thread scheduling purposes.
	// There is no requirement that a thread that releases a permit must
	// have acquired that permit by calling Acquire().
	// Correct usage of a semaphore is established by programming convention
	// in the application.
	Release()
}

type semaphoreWaiter struct {
	n     int32
	ready chan struct{}
}

type semaphore struct {
	maxPermits int32

	mu sync.Mutex
	// permits is the number of permits currently held. It may exceed
	// maxPermits when a request larger than the remaining headroom is admitted.
	permits int32
	// waiters holds blocked acquirers in FIFO order. It is only non-empty
	// while permits >= maxPermits.
	waiters list.List
}

func NewSemaphore(maxPermits int32) Semaphore {
	if maxPermits <= 0 {
		log.Fatal("Max permits for semaphore needs to be > 0")
	}

	return &semaphore{
		maxPermits: maxPermits,
	}
}

func (s *semaphore) Acquire(ctx context.Context) bool {
	return s.AcquireN(ctx, 1)
}

func (s *semaphore) AcquireN(ctx context.Context, n int32) bool {
	if n <= 0 {
		return true
	}

	s.mu.Lock()
	if s.permits < s.maxPermits && s.waiters.Len() == 0 {
		s.permits += n
		s.mu.Unlock()
		return true
	}

	w := &semaphoreWaiter{n: n, ready: make(chan struct{})}
	elem := s.waiters.PushBack(w)
	s.mu.Unlock()

	// Block until the permits are granted or the context expires
	select {
	case <-w.ready:
		return true
	case <-ctx.Done():
		s.mu.Lock()
		defer s.mu.Unlock()
		select {
		case <-w.ready:
			// The permits were granted while the context expired. Hand them
			// back so that the caller, which sees a failure, holds nothing.
			s.permits -= n
		default:
			s.waiters.Remove(elem)
		}
		// Removing the head waiter, or returning its permits, may unblock
		// the waiters queued behind it.
		s.grantLocked()
		return false
	}
}

func (s *semaphore) TryAcquire() bool {
	return s.TryAcquireN(1)
}

func (s *semaphore) TryAcquireN(n int32) bool {
	if n <= 0 {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.permits >= s.maxPermits {
		// All the permits are already exhausted
		return false
	}

	s.permits += n
	return true
}

func (s *semaphore) Release() {
	s.mu.Lock()
	s.permits--
	s.grantLocked()
	s.mu.Unlock()
}

// grantLocked hands permits to blocked acquirers, in arrival order, for as
// long as the permits held are below maxPermits. s.mu must be held.
func (s *semaphore) grantLocked() {
	for s.permits < s.maxPermits {
		front := s.waiters.Front()
		if front == nil {
			return
		}
		w := s.waiters.Remove(front).(*semaphoreWaiter)
		s.permits += w.n
		close(w.ready)
	}
}