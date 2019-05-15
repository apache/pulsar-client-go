//
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
//

package internal

import (
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"net/url"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"testing"
)

type mockedRpcClient struct {
	requestIdGenerator uint64
	t                  *testing.T

	expectedUrl      string
	expectedRequests []pb.CommandLookupTopic
	mockedResponses  []pb.CommandLookupTopicResponse
}

// Create a new unique request id
func (c *mockedRpcClient) NewRequestId() uint64 {
	c.requestIdGenerator += 1
	return c.requestIdGenerator
}

func (c *mockedRpcClient) NewProducerId() uint64 {
	return 1
}

func (c *mockedRpcClient) NewConsumerId() uint64 {
	return 1
}

func (c *mockedRpcClient) RequestToAnyBroker(requestId uint64, cmdType pb.BaseCommand_Type, message proto.Message) (*RpcResult, error) {
	assert.Equal(c.t, cmdType, pb.BaseCommand_LOOKUP)

	expectedRequest := &c.expectedRequests[0]
	c.expectedRequests = c.expectedRequests[1:]

	assert.Equal(c.t, expectedRequest, message)

	mockedResponse := &c.mockedResponses[0]
	c.mockedResponses = c.mockedResponses[1:]

	return &RpcResult{
		&pb.BaseCommand{
			LookupTopicResponse: mockedResponse,
		},
		nil,
	}, nil
}

func (c *mockedRpcClient) Request(logicalAddr *url.URL, physicalAddr *url.URL, requestId uint64,
	cmdType pb.BaseCommand_Type, message proto.Message) (*RpcResult, error) {
	assert.Equal(c.t, cmdType, pb.BaseCommand_LOOKUP)
	expectedRequest := &c.expectedRequests[0]
	c.expectedRequests = c.expectedRequests[1:]

	assert.Equal(c.t, expectedRequest, message)

	mockedResponse := &c.mockedResponses[0]
	c.mockedResponses = c.mockedResponses[1:]

	assert.Equal(c.t, c.expectedUrl, logicalAddr.String())
	assert.Equal(c.t, c.expectedUrl, physicalAddr.String())

	return &RpcResult{
		&pb.BaseCommand{
			LookupTopicResponse: mockedResponse,
		},
		nil,
	}, nil
}

func (c *mockedRpcClient) RequestOnCnx(cnx Connection, requestId uint64, cmdType pb.BaseCommand_Type, message proto.Message) (*RpcResult, error) {
	assert.Fail(c.t, "Shouldn't be called")
	return nil, nil
}

func responseType(r pb.CommandLookupTopicResponse_LookupType) *pb.CommandLookupTopicResponse_LookupType {
	return &r
}

func TestLookupSuccess(t *testing.T) {
	url, err := url.Parse("pulsar://example:6650")
	assert.NoError(t, err)

	ls := NewLookupService(&mockedRpcClient{
		t: t,

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:     proto.Uint64(1),
				Topic:         proto.String("my-topic"),
				Authoritative: proto.Bool(false),
			},
		},
		mockedResponses: []pb.CommandLookupTopicResponse{
			{
				RequestId:        proto.Uint64(1),
				Response:         responseType(pb.CommandLookupTopicResponse_Connect),
				Authoritative:    proto.Bool(true),
				BrokerServiceUrl: proto.String("pulsar://broker-1:6650"),
			},
		},
	}, url)

	lr, err := ls.Lookup("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, lr)

	assert.Equal(t, "pulsar://broker-1:6650", lr.LogicalAddr.String())
	assert.Equal(t, "pulsar://broker-1:6650", lr.PhysicalAddr.String())
}

func TestLookupWithProxy(t *testing.T) {
	url, err := url.Parse("pulsar://example:6650")
	assert.NoError(t, err)

	ls := NewLookupService(&mockedRpcClient{
		t: t,

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:     proto.Uint64(1),
				Topic:         proto.String("my-topic"),
				Authoritative: proto.Bool(false),
			},
		},
		mockedResponses: []pb.CommandLookupTopicResponse{
			{
				RequestId:              proto.Uint64(1),
				Response:               responseType(pb.CommandLookupTopicResponse_Connect),
				Authoritative:          proto.Bool(true),
				BrokerServiceUrl:       proto.String("pulsar://broker-1:6650"),
				ProxyThroughServiceUrl: proto.Bool(true),
			},
		},
	}, url)

	lr, err := ls.Lookup("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, lr)

	assert.Equal(t, "pulsar://broker-1:6650", lr.LogicalAddr.String())
	assert.Equal(t, "pulsar://example:6650", lr.PhysicalAddr.String())
}

func TestLookupWithRedirect(t *testing.T) {
	url, err := url.Parse("pulsar://example:6650")
	assert.NoError(t, err)

	ls := NewLookupService(&mockedRpcClient{
		t:           t,
		expectedUrl: "pulsar://broker-2:6650",

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:     proto.Uint64(1),
				Topic:         proto.String("my-topic"),
				Authoritative: proto.Bool(false),
			},
			{
				RequestId:     proto.Uint64(2),
				Topic:         proto.String("my-topic"),
				Authoritative: proto.Bool(true),
			},
		},
		mockedResponses: []pb.CommandLookupTopicResponse{
			{
				RequestId:        proto.Uint64(1),
				Response:         responseType(pb.CommandLookupTopicResponse_Redirect),
				Authoritative:    proto.Bool(true),
				BrokerServiceUrl: proto.String("pulsar://broker-2:6650"),
			},
			{
				RequestId:        proto.Uint64(2),
				Response:         responseType(pb.CommandLookupTopicResponse_Connect),
				Authoritative:    proto.Bool(true),
				BrokerServiceUrl: proto.String("pulsar://broker-1:6650"),
			},
		},
	}, url)

	lr, err := ls.Lookup("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, lr)

	assert.Equal(t, "pulsar://broker-1:6650", lr.LogicalAddr.String())
	assert.Equal(t, "pulsar://broker-1:6650", lr.PhysicalAddr.String())
}

func TestLookupWithInvalidUrlResponse(t *testing.T) {
	url, err := url.Parse("pulsar://example:6650")
	assert.NoError(t, err)

	ls := NewLookupService(&mockedRpcClient{
		t: t,

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:     proto.Uint64(1),
				Topic:         proto.String("my-topic"),
				Authoritative: proto.Bool(false),
			},
		},
		mockedResponses: []pb.CommandLookupTopicResponse{
			{
				RequestId:              proto.Uint64(1),
				Response:               responseType(pb.CommandLookupTopicResponse_Connect),
				Authoritative:          proto.Bool(true),
				BrokerServiceUrl:       proto.String("foo.html") /* invalid url */,
				ProxyThroughServiceUrl: proto.Bool(false),
			},
		},
	}, url)

	lr, err := ls.Lookup("my-topic")
	assert.Error(t, err)
	assert.Nil(t, lr)
}

func TestLookupWithLookupFailure(t *testing.T) {
	url, err := url.Parse("pulsar://example:6650")
	assert.NoError(t, err)

	ls := NewLookupService(&mockedRpcClient{
		t: t,

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:     proto.Uint64(1),
				Topic:         proto.String("my-topic"),
				Authoritative: proto.Bool(false),
			},
		},
		mockedResponses: []pb.CommandLookupTopicResponse{
			{
				RequestId:              proto.Uint64(1),
				Response:               responseType(pb.CommandLookupTopicResponse_Failed),
				Authoritative:          proto.Bool(true),
			},
		},
	}, url)

	lr, err := ls.Lookup("my-topic")
	assert.Error(t, err)
	assert.Nil(t, lr)
}
