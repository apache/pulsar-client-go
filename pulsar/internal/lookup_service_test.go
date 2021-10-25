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
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/url"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

type mockedLookupRPCClient struct {
	requestIDGenerator uint64
	t                  *testing.T

	expectedURL      string
	expectedRequests []pb.CommandLookupTopic
	mockedResponses  []pb.CommandLookupTopicResponse
}

// Create a new unique request id
func (c *mockedLookupRPCClient) NewRequestID() uint64 {
	c.requestIDGenerator++
	return c.requestIDGenerator
}

func (c *mockedLookupRPCClient) NewProducerID() uint64 {
	return 1
}

func (c *mockedLookupRPCClient) NewConsumerID() uint64 {
	return 1
}

func (c *mockedLookupRPCClient) RequestToAnyBroker(requestID uint64, cmdType pb.BaseCommand_Type,
	message proto.Message) (*RPCResult, error) {
	assert.Equal(c.t, cmdType, pb.BaseCommand_LOOKUP)

	expectedRequest := &c.expectedRequests[0]
	c.expectedRequests = c.expectedRequests[1:]

	assert.Equal(c.t, expectedRequest, message)

	mockedResponse := &c.mockedResponses[0]
	c.mockedResponses = c.mockedResponses[1:]

	return &RPCResult{
		&pb.BaseCommand{
			LookupTopicResponse: mockedResponse,
		},
		nil,
	}, nil
}

func (c *mockedLookupRPCClient) Request(logicalAddr *url.URL, physicalAddr *url.URL, requestID uint64,
	cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error) {
	assert.Equal(c.t, cmdType, pb.BaseCommand_LOOKUP)
	expectedRequest := &c.expectedRequests[0]
	c.expectedRequests = c.expectedRequests[1:]

	assert.Equal(c.t, expectedRequest, message)

	mockedResponse := &c.mockedResponses[0]
	c.mockedResponses = c.mockedResponses[1:]

	assert.Equal(c.t, c.expectedURL, logicalAddr.String())
	assert.Equal(c.t, c.expectedURL, physicalAddr.String())

	return &RPCResult{
		&pb.BaseCommand{
			LookupTopicResponse: mockedResponse,
		},
		nil,
	}, nil
}

func (c *mockedLookupRPCClient) RequestOnCnx(cnx Connection, requestID uint64, cmdType pb.BaseCommand_Type,
	message proto.Message) (*RPCResult, error) {
	assert.Fail(c.t, "Shouldn't be called")
	return nil, nil
}

func (c *mockedLookupRPCClient) RequestOnCnxNoWait(cnx Connection, cmdType pb.BaseCommand_Type,
	message proto.Message) error {
	assert.Fail(c.t, "Shouldn't be called")
	return nil
}

func responseType(r pb.CommandLookupTopicResponse_LookupType) *pb.CommandLookupTopicResponse_LookupType {
	return &r
}

func TestLookupSuccess(t *testing.T) {
	url, err := url.Parse("pulsar://example:6650")
	assert.NoError(t, err)
	serviceNameResolver := NewPulsarServiceNameResolver(url)

	ls := NewLookupService(&mockedLookupRPCClient{
		t: t,

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:              proto.Uint64(1),
				Topic:                  proto.String("my-topic"),
				Authoritative:          proto.Bool(false),
				AdvertisedListenerName: proto.String(""),
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
	}, url, serviceNameResolver, false, "", log.DefaultNopLogger(), NewMetricsProvider(4, map[string]string{}))

	lr, err := ls.Lookup("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, lr)

	assert.Equal(t, "pulsar://broker-1:6650", lr.LogicalAddr.String())
	assert.Equal(t, "pulsar://broker-1:6650", lr.PhysicalAddr.String())
}

func TestTlsLookupSuccess(t *testing.T) {
	url, err := url.Parse("pulsar+ssl://example:6651")
	assert.NoError(t, err)
	serviceNameResolver := NewPulsarServiceNameResolver(url)

	ls := NewLookupService(&mockedLookupRPCClient{
		t: t,

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:              proto.Uint64(1),
				Topic:                  proto.String("my-topic"),
				Authoritative:          proto.Bool(false),
				AdvertisedListenerName: proto.String(""),
			},
		},
		mockedResponses: []pb.CommandLookupTopicResponse{
			{
				RequestId:           proto.Uint64(1),
				Response:            responseType(pb.CommandLookupTopicResponse_Connect),
				Authoritative:       proto.Bool(true),
				BrokerServiceUrlTls: proto.String("pulsar+ssl://broker-1:6651"),
			},
		},
	}, url, serviceNameResolver, true, "", log.DefaultNopLogger(), NewMetricsProvider(4, map[string]string{}))

	lr, err := ls.Lookup("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, lr)

	assert.Equal(t, "pulsar+ssl://broker-1:6651", lr.LogicalAddr.String())
	assert.Equal(t, "pulsar+ssl://broker-1:6651", lr.PhysicalAddr.String())
}

func TestLookupWithProxy(t *testing.T) {
	url, err := url.Parse("pulsar://example:6650")
	assert.NoError(t, err)
	serviceNameResolver := NewPulsarServiceNameResolver(url)

	ls := NewLookupService(&mockedLookupRPCClient{
		t: t,

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:              proto.Uint64(1),
				Topic:                  proto.String("my-topic"),
				Authoritative:          proto.Bool(false),
				AdvertisedListenerName: proto.String(""),
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
	}, url, serviceNameResolver, false, "", log.DefaultNopLogger(), NewMetricsProvider(4, map[string]string{}))

	lr, err := ls.Lookup("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, lr)

	assert.Equal(t, "pulsar://broker-1:6650", lr.LogicalAddr.String())
	assert.Equal(t, "pulsar://example:6650", lr.PhysicalAddr.String())
}

func TestTlsLookupWithProxy(t *testing.T) {
	url, err := url.Parse("pulsar+ssl://example:6651")
	assert.NoError(t, err)

	ls := NewLookupService(&mockedLookupRPCClient{
		t: t,

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:              proto.Uint64(1),
				Topic:                  proto.String("my-topic"),
				Authoritative:          proto.Bool(false),
				AdvertisedListenerName: proto.String(""),
			},
		},
		mockedResponses: []pb.CommandLookupTopicResponse{
			{
				RequestId:              proto.Uint64(1),
				Response:               responseType(pb.CommandLookupTopicResponse_Connect),
				Authoritative:          proto.Bool(true),
				BrokerServiceUrlTls:    proto.String("pulsar+ssl://broker-1:6651"),
				ProxyThroughServiceUrl: proto.Bool(true),
			},
		},
	}, url, NewPulsarServiceNameResolver(url), true, "", log.DefaultNopLogger(),
		NewMetricsProvider(4, map[string]string{}))

	lr, err := ls.Lookup("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, lr)

	assert.Equal(t, "pulsar+ssl://broker-1:6651", lr.LogicalAddr.String())
	assert.Equal(t, "pulsar+ssl://example:6651", lr.PhysicalAddr.String())
}

func TestLookupWithRedirect(t *testing.T) {
	url, err := url.Parse("pulsar://example:6650")
	assert.NoError(t, err)

	ls := NewLookupService(&mockedLookupRPCClient{
		t:           t,
		expectedURL: "pulsar://broker-2:6650",

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:              proto.Uint64(1),
				Topic:                  proto.String("my-topic"),
				Authoritative:          proto.Bool(false),
				AdvertisedListenerName: proto.String(""),
			},
			{
				RequestId:              proto.Uint64(2),
				Topic:                  proto.String("my-topic"),
				Authoritative:          proto.Bool(true),
				AdvertisedListenerName: proto.String(""),
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
	}, url, NewPulsarServiceNameResolver(url), false, "", log.DefaultNopLogger(),
		NewMetricsProvider(4, map[string]string{}))

	lr, err := ls.Lookup("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, lr)

	assert.Equal(t, "pulsar://broker-1:6650", lr.LogicalAddr.String())
	assert.Equal(t, "pulsar://broker-1:6650", lr.PhysicalAddr.String())
}

func TestTlsLookupWithRedirect(t *testing.T) {
	url, err := url.Parse("pulsar+ssl://example:6651")
	assert.NoError(t, err)

	ls := NewLookupService(&mockedLookupRPCClient{
		t:           t,
		expectedURL: "pulsar+ssl://broker-2:6651",

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:              proto.Uint64(1),
				Topic:                  proto.String("my-topic"),
				Authoritative:          proto.Bool(false),
				AdvertisedListenerName: proto.String(""),
			},
			{
				RequestId:              proto.Uint64(2),
				Topic:                  proto.String("my-topic"),
				Authoritative:          proto.Bool(true),
				AdvertisedListenerName: proto.String(""),
			},
		},
		mockedResponses: []pb.CommandLookupTopicResponse{
			{
				RequestId:           proto.Uint64(1),
				Response:            responseType(pb.CommandLookupTopicResponse_Redirect),
				Authoritative:       proto.Bool(true),
				BrokerServiceUrlTls: proto.String("pulsar+ssl://broker-2:6651"),
			},
			{
				RequestId:           proto.Uint64(2),
				Response:            responseType(pb.CommandLookupTopicResponse_Connect),
				Authoritative:       proto.Bool(true),
				BrokerServiceUrlTls: proto.String("pulsar+ssl://broker-1:6651"),
			},
		},
	}, url, NewPulsarServiceNameResolver(url), true, "", log.DefaultNopLogger(),
		NewMetricsProvider(4, map[string]string{}))

	lr, err := ls.Lookup("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, lr)

	assert.Equal(t, "pulsar+ssl://broker-1:6651", lr.LogicalAddr.String())
	assert.Equal(t, "pulsar+ssl://broker-1:6651", lr.PhysicalAddr.String())
}

func TestLookupWithInvalidUrlResponse(t *testing.T) {
	url, err := url.Parse("pulsar://example:6650")
	assert.NoError(t, err)

	ls := NewLookupService(&mockedLookupRPCClient{
		t: t,

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:              proto.Uint64(1),
				Topic:                  proto.String("my-topic"),
				Authoritative:          proto.Bool(false),
				AdvertisedListenerName: proto.String(""),
			},
		},
		mockedResponses: []pb.CommandLookupTopicResponse{
			{
				RequestId:              proto.Uint64(1),
				Response:               responseType(pb.CommandLookupTopicResponse_Connect),
				Authoritative:          proto.Bool(true),
				BrokerServiceUrl:       proto.String("foo.html"), /* invalid url */
				ProxyThroughServiceUrl: proto.Bool(false),
			},
		},
	}, url, NewPulsarServiceNameResolver(url), false, "", log.DefaultNopLogger(),
		NewMetricsProvider(4, map[string]string{}))

	lr, err := ls.Lookup("my-topic")
	assert.Error(t, err)
	assert.Nil(t, lr)
}

func TestLookupWithLookupFailure(t *testing.T) {
	url, err := url.Parse("pulsar://example:6650")
	assert.NoError(t, err)

	ls := NewLookupService(&mockedLookupRPCClient{
		t: t,

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:              proto.Uint64(1),
				Topic:                  proto.String("my-topic"),
				Authoritative:          proto.Bool(false),
				AdvertisedListenerName: proto.String(""),
			},
		},
		mockedResponses: []pb.CommandLookupTopicResponse{
			{
				RequestId:     proto.Uint64(1),
				Response:      responseType(pb.CommandLookupTopicResponse_Failed),
				Authoritative: proto.Bool(true),
			},
		},
	}, url, NewPulsarServiceNameResolver(url), false, "", log.DefaultNopLogger(),
		NewMetricsProvider(4, map[string]string{}))

	lr, err := ls.Lookup("my-topic")
	assert.Error(t, err)
	assert.Nil(t, lr)
}

type mockedPartitionedTopicMetadataRPCClient struct {
	requestIDGenerator uint64
	t                  *testing.T

	expectedRequests []pb.CommandPartitionedTopicMetadata
	mockedResponses  []pb.CommandPartitionedTopicMetadataResponse
}

func (m mockedPartitionedTopicMetadataRPCClient) NewRequestID() uint64 {
	m.requestIDGenerator++
	return m.requestIDGenerator
}

func (m mockedPartitionedTopicMetadataRPCClient) NewProducerID() uint64 {
	return 1
}

func (m mockedPartitionedTopicMetadataRPCClient) NewConsumerID() uint64 {
	return 1
}

func (m mockedPartitionedTopicMetadataRPCClient) RequestToAnyBroker(requestID uint64, cmdType pb.BaseCommand_Type,
	message proto.Message) (*RPCResult, error) {
	assert.Equal(m.t, cmdType, pb.BaseCommand_PARTITIONED_METADATA)

	expectedRequest := &m.expectedRequests[0]
	m.expectedRequests = m.expectedRequests[1:]

	assert.Equal(m.t, *expectedRequest.RequestId, requestID)

	mockedResponse := &m.mockedResponses[0]
	m.mockedResponses = m.mockedResponses[1:]

	return &RPCResult{
		&pb.BaseCommand{
			PartitionMetadataResponse: mockedResponse,
		},
		nil,
	}, nil
}

func (m mockedPartitionedTopicMetadataRPCClient) Request(logicalAddr *url.URL, physicalAddr *url.URL, requestID uint64,
	cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error) {
	assert.Fail(m.t, "Shouldn't be called")
	return nil, nil
}

func (m mockedPartitionedTopicMetadataRPCClient) RequestOnCnxNoWait(cnx Connection, cmdType pb.BaseCommand_Type,
	message proto.Message) error {
	assert.Fail(m.t, "Shouldn't be called")
	return nil
}

func (m mockedPartitionedTopicMetadataRPCClient) RequestOnCnx(cnx Connection, requestID uint64,
	cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error) {
	assert.Fail(m.t, "Shouldn't be called")
	return nil, nil
}

func TestGetPartitionedTopicMetadataSuccess(t *testing.T) {
	url, err := url.Parse("pulsar://example:6650")
	assert.NoError(t, err)
	serviceNameResolver := NewPulsarServiceNameResolver(url)

	ls := NewLookupService(&mockedPartitionedTopicMetadataRPCClient{
		t: t,

		expectedRequests: []pb.CommandPartitionedTopicMetadata{
			{
				RequestId: proto.Uint64(1),
				Topic:     proto.String("my-topic"),
			},
		},
		mockedResponses: []pb.CommandPartitionedTopicMetadataResponse{
			{
				RequestId:  proto.Uint64(1),
				Partitions: proto.Uint32(1),
				Response:   pb.CommandPartitionedTopicMetadataResponse_Success.Enum(),
			},
		},
	}, url, serviceNameResolver, false, "", log.DefaultNopLogger(), NewMetricsProvider(4, map[string]string{}))

	metadata, err := ls.GetPartitionedTopicMetadata("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Equal(t, metadata.Partitions, 1)
}

func TestLookupSuccessWithMultipleHosts(t *testing.T) {
	url, err := url.Parse("pulsar://host1,host2,host3:6650")
	assert.NoError(t, err)
	serviceNameResolver := NewPulsarServiceNameResolver(url)

	ls := NewLookupService(&mockedLookupRPCClient{
		t: t,

		expectedRequests: []pb.CommandLookupTopic{
			{
				RequestId:              proto.Uint64(1),
				Topic:                  proto.String("my-topic"),
				AdvertisedListenerName: proto.String(""),
				Authoritative:          proto.Bool(false),
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
	}, url, serviceNameResolver, false, "", log.DefaultNopLogger(), NewMetricsProvider(4, map[string]string{}))

	lr, err := ls.Lookup("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, lr)

	assert.Equal(t, "pulsar://broker-1:6650", lr.LogicalAddr.String())
	assert.Equal(t, "pulsar://broker-1:6650", lr.PhysicalAddr.String())
}

type MockHTTPClient struct {
	ServiceNameResolver ServiceNameResolver
}

func (c *MockHTTPClient) Close() {}

func (c *MockHTTPClient) Get(endpoint string, obj interface{}, params map[string]string) error {
	if strings.Contains(endpoint, HTTPLookupServiceBasePathV1) || strings.Contains(endpoint,
		HTTPLookupServiceBasePathV2) {
		return mockHTTPGetLookupResult(obj)
	} else if strings.Contains(endpoint, "partitions") {
		return mockHTTPGetPartitionedTopicMetadataResult(obj)
	}
	return errors.New("not supported request")
}

func mockHTTPGetLookupResult(obj interface{}) error {
	jsonResponse := `{
   		"brokerUrl": "pulsar://broker-1:6650",
   		"brokerUrlTls": "",
		"httpUrl": "http://broker-1:8080",
		"httpUrlTls": ""
  	}`
	r := ioutil.NopCloser(bytes.NewReader([]byte(jsonResponse)))
	dec := json.NewDecoder(r)
	err := dec.Decode(obj)
	return err
}

func mockHTTPGetPartitionedTopicMetadataResult(obj interface{}) error {
	jsonResponse := `{
   		"partitions": 1
  	}`
	r := ioutil.NopCloser(bytes.NewReader([]byte(jsonResponse)))
	dec := json.NewDecoder(r)
	err := dec.Decode(obj)
	return err
}

func NewMockHTTPClient(serviceNameResolver ServiceNameResolver) HTTPClient {
	h := &MockHTTPClient{}
	h.ServiceNameResolver = serviceNameResolver
	return h
}

func TestHttpLookupSuccess(t *testing.T) {
	url, err := url.Parse("http://broker-1:8080")
	assert.NoError(t, err)
	serviceNameResolver := NewPulsarServiceNameResolver(url)
	httpClient := NewMockHTTPClient(serviceNameResolver)
	ls := NewHTTPLookupService(httpClient, url, serviceNameResolver, false,
		log.DefaultNopLogger(), NewMetricsProvider(4, map[string]string{}))

	lr, err := ls.Lookup("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, lr)

	assert.Equal(t, "pulsar://broker-1:6650", lr.LogicalAddr.String())
	assert.Equal(t, "pulsar://broker-1:6650", lr.PhysicalAddr.String())
}

func TestHttpGetPartitionedTopicMetadataSuccess(t *testing.T) {
	url, err := url.Parse("http://broker-1:8080")
	assert.NoError(t, err)
	serviceNameResolver := NewPulsarServiceNameResolver(url)
	httpClient := NewMockHTTPClient(serviceNameResolver)
	ls := NewHTTPLookupService(httpClient, url, serviceNameResolver, false,
		log.DefaultNopLogger(), NewMetricsProvider(4, map[string]string{}))

	tMetadata, err := ls.GetPartitionedTopicMetadata("my-topic")
	assert.NoError(t, err)
	assert.NotNil(t, tMetadata)

	assert.Equal(t, 1, tMetadata.Partitions)
}
