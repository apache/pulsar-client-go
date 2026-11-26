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
	// Release never blocks. The permit does not have to be released by the
	// goroutine that acquired it, but each Release must match an earlier
	// successful Acquire or TryAcquire. A Release when no permit is held is
	// a no-op.
	Release()
}

type semaphore struct {
	// tokens holds one element per permit currently held. Its capacity is the
	// maximum number of permits. Acquire puts a token in, Release takes one out.
	// Release never waits for another goroutine, and a blocked Acquire is
	// cancelled atomically by select, so no permit can be lost or leaked.
	// Goroutines blocked sending on a full channel are woken in FIFO order.
	tokens chan struct{}
}

func NewSemaphore(maxPermits int32) Semaphore {
	if maxPermits <= 0 {
		log.Fatal("Max permits for semaphore needs to be > 0")
	}

	return &semaphore{
		tokens: make(chan struct{}, maxPermits),
	}
}

func (s *semaphore) Acquire(ctx context.Context) bool {
	// Fast path: take a free permit without considering the context,
	// matching the previous behaviour.
	select {
	case s.tokens <- struct{}{}:
		return true
	default:
	}

	// Block until a permit is available or the context expires
	select {
	case s.tokens <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *semaphore) TryAcquire() bool {
	select {
	case s.tokens <- struct{}{}:
		return true
	default:
		// All the permits are already exhausted
		return false
	}
}

func (s *semaphore) Release() {
	select {
	case <-s.tokens:
	default:
		// No permit is held. Releasing it is a no-op, so an unmatched Release
		// cannot raise the capacity above maxPermits.
		log.Warn("Semaphore released without a matching acquire")
	}
}
