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
	"net/url"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/assert"
)

func TestNewRPCClient_InvalidURL_ShouldNotPanic(t *testing.T) {
	// Test that NewRPCClient doesn't panic with invalid URL
	invalidURL, _ := url.Parse("invalid://scheme")

	// This should not panic and should return an error
	_, err := NewRPCClient(invalidURL, nil, 0, log.DefaultNopLogger(), nil, "", nil, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid URL scheme")
}

func TestLookupService_InvalidURL_ShouldNotPanic(t *testing.T) {
	// Create a minimal RPC client for testing
	validURL, _ := url.Parse("pulsar://localhost:6650")
	rpcClient, err := NewRPCClient(validURL, nil, 0, log.DefaultNopLogger(), nil, "", nil, nil, nil)
	assert.NoError(t, err)

	// Test that LookupService doesn't panic with invalid URL
	_, err = rpcClient.LookupService("invalid://url")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid URL scheme")
}

func TestLookupService_InvalidScheme_ShouldNotPanic(t *testing.T) {
	// Create a minimal RPC client for testing
	validURL, _ := url.Parse("pulsar://localhost:6650")
	rpcClient, err := NewRPCClient(validURL, nil, 0, log.DefaultNopLogger(), nil, "", nil, nil, nil)
	assert.NoError(t, err)

	// Test that LookupService doesn't panic with invalid scheme
	_, err = rpcClient.LookupService("ftp://localhost:21")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid URL scheme")
}
