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

package tests

import (
	"sync/atomic"
	"testing"
	"time"
)

type blockingFailureInjectHook struct {
	calls     int32
	blocked   chan struct{}
	releaseCh chan struct{}
}

func newBlockingFailureInjectHook() *blockingFailureInjectHook {
	return &blockingFailureInjectHook{
		blocked:   make(chan struct{}),
		releaseCh: make(chan struct{}),
	}
}

func (h *blockingFailureInjectHook) BeforeAssignPartitionConsumers() {
	if atomic.AddInt32(&h.calls, 1) == 1 {
		return
	}
	close(h.blocked)
	<-h.releaseCh
}

func (h *blockingFailureInjectHook) waitUntilBlocked(t *testing.T) {
	t.Helper()

	select {
	case <-h.blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for failure injection hook")
	}
}

func (h *blockingFailureInjectHook) release() {
	close(h.releaseCh)
}
