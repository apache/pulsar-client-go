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
	"errors"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/backoff"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestNewRPCClient_InvalidURL_ShouldNotPanic(t *testing.T) {
	// Test that NewRPCClient doesn't panic with invalid URL
	invalidURL := "invalid://scheme"

	// This should not panic and should return an error
	_, err := NewRPCClient(invalidURL, nil, 0, log.DefaultNopLogger(), nil, "", nil, nil, nil, nil)
	assert.Error(t, err)
}

func TestLookupService_InvalidURL_ShouldNotPanic(t *testing.T) {
	// Create a minimal RPC client for testing
	validURL := "pulsar://localhost:6650"
	rpcClient, err := NewRPCClient(validURL, nil, 0, log.DefaultNopLogger(), nil, "", nil, nil, nil, nil)
	assert.NoError(t, err)

	// Test that LookupService doesn't panic with invalid URL
	_, err = rpcClient.LookupService("invalid://url")
	assert.Error(t, err)
}

func TestLookupService_InvalidScheme_ShouldNotPanic(t *testing.T) {
	// Create a minimal RPC client for testing
	validURL := "pulsar://localhost:6650"
	rpcClient, err := NewRPCClient(validURL, nil, 0, log.DefaultNopLogger(), nil, "", nil, nil, nil, nil)
	assert.NoError(t, err)

	// Test that LookupService doesn't panic with invalid scheme
	_, err = rpcClient.LookupService("ftp://localhost:21")
	assert.Error(t, err)
}

// refusedPool simulates a service URL where every connection is refused,
// so requestToHost exhausts its retries without a broker.
type refusedPool struct {
	calls int32
}

func (p *refusedPool) GetConnection(logicalAddr *url.URL, physicalAddr *url.URL,
	keySuffix int32) (Connection, error) {
	atomic.AddInt32(&p.calls, 1)
	return nil, errors.New("connection refused")
}

func (p *refusedPool) GetConnections() map[string]Connection { return nil }

func (p *refusedPool) GenerateRoundRobinIndex() int32 { return 0 }

func (p *refusedPool) Close() {}

// countingPolicy counts the delays handed out between the retries of a request.
type countingPolicy struct {
	nexts int32
	delay time.Duration
}

func (p *countingPolicy) Next() time.Duration {
	atomic.AddInt32(&p.nexts, 1)
	return p.delay
}

func (p *countingPolicy) IsMaxBackoffReached() bool { return false }

func (p *countingPolicy) Reset() {}

func newLookupRequest(c RPCClient) (uint64, pb.BaseCommand_Type, *pb.CommandLookupTopic) {
	return c.NewRequestID(), pb.BaseCommand_LOOKUP,
		&pb.CommandLookupTopic{Topic: proto.String("persistent://public/default/t")}
}

func TestRequestToAnyBroker_UsesClientBackoffPolicy(t *testing.T) {
	pool := &refusedPool{}
	policy := &countingPolicy{delay: time.Millisecond}
	factories := int32(0)
	rpcClient, err := NewRPCClient("pulsar://localhost:6650", pool, 300*time.Millisecond,
		log.DefaultNopLogger(), nil, "", nil, nil, nil,
		func() backoff.Policy {
			atomic.AddInt32(&factories, 1)
			return policy
		})
	assert.NoError(t, err)

	requestID, cmdType, message := newLookupRequest(rpcClient)
	_, err = rpcClient.RequestToAnyBroker(requestID, cmdType, message)
	assert.Error(t, err)

	// One policy per request, and its delays pace every retry.
	assert.EqualValues(t, 1, atomic.LoadInt32(&factories))
	assert.Greater(t, atomic.LoadInt32(&policy.nexts), int32(10))
	assert.Greater(t, atomic.LoadInt32(&pool.calls), int32(10))
}

func TestRequestToAnyBroker_DefaultBackoffStaysSlow(t *testing.T) {
	pool := &refusedPool{}
	rpcClient, err := NewRPCClient("pulsar://localhost:6650", pool, 300*time.Millisecond,
		log.DefaultNopLogger(), nil, "", nil, nil, nil, nil)
	assert.NoError(t, err)

	requestID, cmdType, message := newLookupRequest(rpcClient)
	_, err = rpcClient.RequestToAnyBroker(requestID, cmdType, message)
	assert.Error(t, err)

	// The default doubles from 100ms, so a 300ms operation timeout leaves
	// room for only a handful of attempts.
	assert.LessOrEqual(t, atomic.LoadInt32(&pool.calls), int32(8))
}
