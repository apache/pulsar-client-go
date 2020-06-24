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
			s.Acquire()
			time.Sleep(100 * time.Millisecond)
			s.Release()
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestSemaphore_TryAcquire(t *testing.T) {
	s := NewSemaphore(1)

	s.Acquire()

	assert.False(t, s.TryAcquire())

	s.Release()

	assert.True(t, s.TryAcquire())
	assert.False(t, s.TryAcquire())
	s.Release()
}
