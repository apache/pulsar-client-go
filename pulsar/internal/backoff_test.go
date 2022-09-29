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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoff_NextMinValue(t *testing.T) {
	backoff := &DefaultBackoff{}
	delay := backoff.Next()
	assert.GreaterOrEqual(t, int64(delay), int64(100*time.Millisecond))
	assert.LessOrEqual(t, int64(delay), int64(120*time.Millisecond))
}

func TestBackoff_NextExponentialBackoff(t *testing.T) {
	backoff := &DefaultBackoff{}
	previousDelay := backoff.Next()
	// the last value before capping to the max value is 51.2 s (.1, .2, .4, .8, 1.6, 3.2, 6.4, 12.8, 25.6, 51.2)
	for previousDelay < 51*time.Second {
		delay := backoff.Next()
		// the jitter introduces at most 20% difference so at least delay is 1.6=(1-0.2)*2 bigger
		assert.GreaterOrEqual(t, int64(delay), int64(1.6*float64(previousDelay)))
		// the jitter introduces at most 20% difference so delay is less than twice the previous value
		assert.LessOrEqual(t, int64(float64(delay)*.8), int64(2*float64(previousDelay)))
		previousDelay = delay
		assert.Equal(t, false, backoff.IsMaxBackoffReached())
	}
}

func TestBackoff_NextMaxValue(t *testing.T) {
	backoff := &DefaultBackoff{}
	var delay time.Duration
	for delay < maxBackoff {
		delay = backoff.Next()
	}

	cappedDelay := backoff.Next()
	assert.GreaterOrEqual(t, int64(cappedDelay), int64(maxBackoff))
	assert.Equal(t, true, backoff.IsMaxBackoffReached())
	// max value is 60 seconds + 20% jitter = 72 seconds
	assert.LessOrEqual(t, int64(cappedDelay), int64(72*time.Second))
}
