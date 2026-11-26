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

type Semaphore interface {
	// Acquire a permit, if one is available and returns immediately,
	// reducing the number of available permits by one.
	Acquire(ctx context.Context) bool

	// Try to acquire a permit. The method will return immediately
	// with a `true` if it was possible to acquire a permit and
	// `false` otherwise.
	TryAcquire() bool

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

type semaphore struct {
	maxPermits int32

	// mu protects permits and waiters. Holding it only for short, non-blocking
	// critical sections guarantees that Release never blocks.
	mu sync.Mutex
	// permits is the number of permits currently held (or handed to a waiter).
	// When waiters is non-empty, permits is always maxPermits.
	permits int32
	// waiters is a FIFO queue of blocked Acquire calls, each holding a *waiter.
	waiters list.List
}

type waiter struct {
	// ready is closed by Release when it hands a permit to this waiter.
	ready chan struct{}
	// granted is set under semaphore.mu when a permit has been handed to this waiter.
	granted bool
}

func NewSemaphore(maxPermits int32) Semaphore {
	if maxPermits <= 0 {
		log.Fatal("Max permits for semaphore needs to be > 0")
	}

	return &semaphore{
		maxPermits: maxPermits,
		permits:    0,
	}
}

func (s *semaphore) Acquire(ctx context.Context) bool {
	s.mu.Lock()
	if s.permits < s.maxPermits {
		s.permits++
		s.mu.Unlock()
		return true
	}

	w := &waiter{ready: make(chan struct{})}
	elem := s.waiters.PushBack(w)
	s.mu.Unlock()

	// Block until a permit is handed over or the context expires
	select {
	case <-w.ready:
		return true
	case <-ctx.Done():
		s.mu.Lock()
		defer s.mu.Unlock()
		if w.granted {
			// Release already handed us a permit before we could leave the queue.
			// Keep it: the caller will see a successful acquire.
			return true
		}
		s.waiters.Remove(elem)
		return false
	}
}

func (s *semaphore) TryAcquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.permits >= s.maxPermits {
		// All the permits are already exhausted
		return false
	}
	s.permits++
	return true
}

func (s *semaphore) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if front := s.waiters.Front(); front != nil {
		// Hand the permit directly to the next waiter. permits stays at maxPermits.
		w := s.waiters.Remove(front).(*waiter)
		w.granted = true
		close(w.ready)
		return
	}
	s.permits--
}
