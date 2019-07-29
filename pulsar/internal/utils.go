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
	"sync/atomic"
	"time"
)

// TimestampMillis return a time unix nano.
func TimestampMillis(t time.Time) uint64 {
	return uint64(t.UnixNano()) / uint64(time.Millisecond)
}

// GetAndAdd perform atomic read and update
func GetAndAdd(n *uint64, diff uint64) uint64 {
	for {
		v := *n
		if atomic.CompareAndSwapUint64(n, v, v+diff) {
			return v
		}
	}
}
