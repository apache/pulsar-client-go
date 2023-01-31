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

package pulsar

import (
	"testing"
	"time"

	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common/algorithm/algorithm"
	"github.com/stretchr/testify/require"
)

func TestCreateToken(t *testing.T) {
	tokenProvider := token{}

	alg := algorithm.HS256
	key, err := tokenProvider.CreateSecretKey(alg)
	require.NoError(t, err)

	subject := "test-role"
	myToken, err := tokenProvider.Create(alg, key, subject, 0)
	require.NoError(t, err)

	parsedSubject, exp, err := tokenProvider.Validate(alg, myToken, key)
	require.NoError(t, err)
	require.Equal(t, subject, parsedSubject)
	require.Equal(t, exp, int64(0))
}

func TestCreateTokenWithExp(t *testing.T) {
	tokenProvider := token{}

	alg := algorithm.HS256
	key, err := tokenProvider.CreateSecretKey(alg)
	require.NoError(t, err)

	subject := "test-role"
	exp := time.Now().Add(time.Hour).Unix()
	myToken, err := tokenProvider.Create(alg, key, subject, exp)
	require.NoError(t, err)

	parsedSubject, exp, err := tokenProvider.Validate(alg, myToken, key)
	require.NoError(t, err)
	require.Equal(t, subject, parsedSubject)
	require.Equal(t, exp, exp)
}
