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
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Backoff struct {
	initGap, nextGap, maxGap time.Duration

	firstRetry       time.Time
	mandatoryStop    time.Duration
	mandatoryStopped bool
}

func NewBackoff(initGap, maxGap, mandatoryStop time.Duration) *Backoff {
	return &Backoff{
		initGap:          initGap,
		nextGap:          initGap,
		maxGap:           maxGap,
		firstRetry:       time.Time{},
		mandatoryStop:    mandatoryStop,
		mandatoryStopped: false,
	}
}

// for test mock convenience
type nowTimeProvider func() time.Time

var systemNow nowTimeProvider = func() time.Time {
	return time.Now()
}

func (b *Backoff) Next() time.Duration {
	curGap := b.nextGap
	if curGap < b.maxGap {
		b.nextGap = MinDuration(2*b.nextGap, b.maxGap)
	}

	if b.mandatoryStop > 0 && !b.mandatoryStopped {
		now := systemNow()
		tried := time.Duration(0)
		if curGap == b.initGap {
			b.firstRetry = now
		} else {
			tried = now.Sub(b.firstRetry)
		}
		if tried+curGap > b.mandatoryStop {
			// reached mandatory stop, reset current retry gap
			curGap = MaxDuration(b.mandatoryStop-tried, b.initGap)
			b.mandatoryStopped = true
		}
	}

	// randomly decrease the timeout up to 10% to avoid simultaneous retries
	if curGap > 10 {
		curGap -= time.Duration(rand.Int63n(int64(curGap) / 10))
	}
	return MaxDuration(curGap, b.initGap)
}
