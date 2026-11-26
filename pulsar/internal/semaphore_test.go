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
	"github.com/stretchr/testify/require"
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

// TestSemaphore_ReleaseRacingCancelledAcquire reproduces the race where Release runs
// while a blocked Acquire is being cancelled. Release must never block, and the
// permit accounting must stay correct.
func TestSemaphore_ReleaseRacingCancelledAcquire(t *testing.T) {
	const iterations = 2000
	for i := 0; i < iterations; i++ {
		s := NewSemaphore(1)
		require.True(t, s.Acquire(context.Background()))

		ctx, cancel := context.WithCancel(context.Background())
		acquired := make(chan bool, 1)
		go func() {
			acquired <- s.Acquire(ctx)
		}()
		// Give the waiter a chance to park.
		time.Sleep(50 * time.Microsecond)

		start := make(chan struct{})
		released := make(chan struct{})
		go func() {
			<-start
			s.Release()
			close(released)
		}()
		close(start)
		cancel()

		select {
		case <-released:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: Release blocked while racing a cancelled Acquire", i)
		}

		var got bool
		select {
		case got = <-acquired:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: Acquire did not return after cancel", i)
		}

		// Exactly one permit is outstanding if the waiter reports success, none otherwise.
		if got {
			require.False(t, s.TryAcquire(), "iteration %d: permit granted to waiter but still available", i)
			s.Release()
		} else {
			require.True(t, s.TryAcquire(), "iteration %d: permit lost after cancelled Acquire", i)
			s.Release()
		}
		require.True(t, s.TryAcquire(), "iteration %d: semaphore not empty after cleanup", i)
		require.False(t, s.TryAcquire(), "iteration %d: semaphore over-admitted", i)
		s.Release()
		cancel()
	}
}
