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
)

func TestChCond(t *testing.T) {
	cond := newCond(&sync.Mutex{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		cond.L.Lock()
		cond.wait()
		cond.L.Unlock()
		wg.Done()
	}()
	time.Sleep(10 * time.Millisecond)
	cond.broadcast()
	wg.Wait()
}

func TestChCondWithContext(t *testing.T) {
	cond := newCond(&sync.Mutex{})
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		cond.L.Lock()
		cond.waitWithContext(ctx)
		cond.L.Unlock()
		wg.Done()
	}()
	cancel()
	wg.Wait()
}
