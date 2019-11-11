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
	"sync"
	"sync/atomic"

	"github.com/apache/pulsar-client-go/pkg/pb"
	"github.com/golang/protobuf/proto"
)

type RPCResult struct {
	Response *pb.BaseCommand
	Cnx      Connection
}

// RequestID for a request when there is no expected response
const RequestIDNoResponse = uint64(0)

type RPCClient interface {
	// Create a new unique request id
	NewRequestID() uint64

	NewProducerID() uint64

	NewConsumerID() uint64

	// Send a request and block until the result is available
	RequestToAnyBroker(requestID uint64, cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error)

	Request(logicalAddr *url.URL, physicalAddr *url.URL, requestID uint64,
		cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error)

	RequestOnCnxNoWait(cnx Connection, requestID uint64, cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error)

	RequestOnCnx(cnx Connection, requestID uint64, cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error)
}

type rpcClient struct {
	serviceURL          *url.URL
	pool                ConnectionPool
	requestIDGenerator  uint64
	producerIDGenerator uint64
	consumerIDGenerator uint64
}

func NewRPCClient(serviceURL *url.URL, pool ConnectionPool) RPCClient {
	return &rpcClient{
		serviceURL: serviceURL,
		pool:       pool,
	}
}

func (c *rpcClient) RequestToAnyBroker(requestID uint64, cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error) {
	return c.Request(c.serviceURL, c.serviceURL, requestID, cmdType, message)
}

func (c *rpcClient) Request(logicalAddr *url.URL, physicalAddr *url.URL, requestID uint64,
	cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error) {
	// TODO: Add retry logic in case of connection issues
	cnx, err := c.pool.GetConnection(logicalAddr, physicalAddr)
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	rpcResult := &RPCResult{
		Cnx: cnx,
	}

	var rpcErr error = nil

	// TODO: Handle errors with disconnections
	cnx.SendRequest(requestID, baseCommand(cmdType, message), func(response *pb.BaseCommand, err error) {
		rpcResult.Response = response
		rpcErr = err
		wg.Done()
	})

	wg.Wait()
	return rpcResult, rpcErr
}

func (c *rpcClient) RequestOnCnx(cnx Connection, requestID uint64, cmdType pb.BaseCommand_Type,
	message proto.Message) (*RPCResult, error) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	rpcResult := &RPCResult{
		Cnx: cnx,
	}

	var rpcErr error = nil
	cnx.SendRequest(requestID, baseCommand(cmdType, message), func(response *pb.BaseCommand, err error) {
		rpcResult.Response = response
		rpcErr = err
		wg.Done()
	})

	wg.Wait()
	return rpcResult, rpcErr
}

func (c *rpcClient) RequestOnCnxNoWait(cnx Connection, requestID uint64, cmdType pb.BaseCommand_Type,
	message proto.Message) (*RPCResult, error) {
	rpcResult := &RPCResult{
		Cnx: cnx,
	}

	cnx.SendRequest(requestID, baseCommand(cmdType, message), func(response *pb.BaseCommand, err error) {
		rpcResult.Response = response
	})

	return rpcResult, nil
}

func (c *rpcClient) NewRequestID() uint64 {
	return atomic.AddUint64(&c.requestIDGenerator, 1)
}

func (c *rpcClient) NewProducerID() uint64 {
	return atomic.AddUint64(&c.producerIDGenerator, 1)
}

func (c *rpcClient) NewConsumerID() uint64 {
	return atomic.AddUint64(&c.consumerIDGenerator, 1)
}
