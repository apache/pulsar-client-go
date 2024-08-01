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
	"errors"
	"time"
)

type OpFn[T any] func() (T, error)

// Retry the given operation until the returned error is nil or the context is done.
func Retry[T any](ctx context.Context, op OpFn[T], nextDuration func(error) time.Duration) (T, error) {
	var (
		timer *time.Timer
		res   T
		err   error
	)

	cleanTimer := func() {
		if timer != nil {
			timer.Stop()
		}
	}
	defer cleanTimer()

	for {
		res, err = op()
		if err == nil {
			return res, nil
		}

		duration := nextDuration(err)
		if timer == nil {
			timer = time.NewTimer(duration)
		} else {
			timer.Reset(duration)
		}

		select {
		case <-ctx.Done():
			return res, errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
	}
}
