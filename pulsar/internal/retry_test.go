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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryWithCtxBackground(t *testing.T) {
	ctx := context.Background()
	i := 0
	res, err := Retry(ctx, func() (string, error) {
		if i == 2 {
			return "ok", nil
		}
		i++
		return "", errors.New("error")
	}, func(_ error) time.Duration {
		return 1 * time.Second
	})
	require.NoError(t, err)
	require.Equal(t, "ok", res)
}

func TestRetryWithCtxTimeout(t *testing.T) {
	ctx, cancelFn := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFn()
	retryErr := errors.New("error")
	res, err := Retry(ctx, func() (string, error) {
		return "", retryErr
	}, func(err error) time.Duration {
		require.Equal(t, retryErr, err)
		return 1 * time.Second
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, retryErr.Error())
	require.Equal(t, "", res)
}
