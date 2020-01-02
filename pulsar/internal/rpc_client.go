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
	`fmt`
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal/pb"
	"github.com/golang/protobuf/proto"
)

type RPCResult struct {
	Response *pb.BaseCommand
	Cnx      Connection
}

type RPCClient interface {
	// Create a new unique request id
	NewRequestID() uint64

	NewProducerID() uint64

	NewConsumerID() uint64

	// Send a request and block until the result is available
	RequestToAnyBroker(requestID uint64, cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error)

	Request(logicalAddr *url.URL, physicalAddr *url.URL, requestID uint64,
		cmdType pb.BaseCommand_Type, message proto.Message, connectingThroughProxy bool) (*RPCResult, error)

	RequestOnCnxNoWait(cnx Connection, cmdType pb.BaseCommand_Type, message proto.Message)

	RequestOnCnx(cnx Connection, requestID uint64, cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error)
}

type rpcClient struct {
	serviceURL          *url.URL
	pool                ConnectionPool
	requestTimeout      time.Duration
	requestIDGenerator  uint64
	producerIDGenerator uint64
	consumerIDGenerator uint64
}

func NewRPCClient(serviceURL *url.URL, pool ConnectionPool, requestTimeout time.Duration) RPCClient {
	return &rpcClient{
		serviceURL:     serviceURL,
		pool:           pool,
		requestTimeout: requestTimeout,
	}
}

func (c *rpcClient) RequestToAnyBroker(requestID uint64, cmdType pb.BaseCommand_Type,
	message proto.Message) (*RPCResult, error) {
	return c.Request(c.serviceURL, c.serviceURL, requestID, cmdType, message, false)
}

func (c *rpcClient) Request(logicalAddr *url.URL, physicalAddr *url.URL, requestID uint64,
	cmdType pb.BaseCommand_Type, message proto.Message, connectingThroughProxy bool) (*RPCResult, error) {

	cnx, err := c.getConn(logicalAddr, physicalAddr, connectingThroughProxy)
	if err != nil {
		return nil, err
	}

	type Res struct {
		*RPCResult
		error
	}
	ch := make(chan Res)

	// TODO: in here, the error of callback always nil
	cnx.SendRequest(requestID, baseCommand(cmdType, message), func(response *pb.BaseCommand, err error) {
		ch <- Res{&RPCResult{
			Cnx:      cnx,
			Response: response,
		}, err}
		close(ch)
	})

	select {
	case res := <-ch:
		return res.RPCResult, res.error
	case <-time.After(c.requestTimeout):
		return nil, errors.New("request timed out")
	}
}

func (c *rpcClient) getConn(logicalAddr *url.URL, physicalAddr *url.URL, connectingThroughProxy bool) (Connection, error){
	cnx, err := c.pool.GetConnection(logicalAddr, physicalAddr, connectingThroughProxy)
	backoff := new(Backoff)
	var retryTime time.Duration
	if err != nil {
		for retryTime < c.requestTimeout {
			retryTime = backoff.Next()
			fmt.Printf("Reconnecting to broker in {%v} \n", retryTime)
			time.Sleep(retryTime)
			cnx, err = c.pool.GetConnection(logicalAddr, physicalAddr, connectingThroughProxy)
			if err == nil {
				fmt.Printf("retry connection success...\n")
				break
			}
		}
	}
	return cnx, nil
}

func (c *rpcClient) RequestOnCnx(cnx Connection, requestID uint64, cmdType pb.BaseCommand_Type,
	message proto.Message) (*RPCResult, error) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	rpcResult := &RPCResult{
		Cnx: cnx,
	}

	var rpcErr error
	cnx.SendRequest(requestID, baseCommand(cmdType, message), func(response *pb.BaseCommand, err error) {
		rpcResult.Response = response
		rpcErr = err
		wg.Done()
	})

	wg.Wait()
	return rpcResult, rpcErr
}

func (c *rpcClient) RequestOnCnxNoWait(cnx Connection, cmdType pb.BaseCommand_Type, message proto.Message) {
	cnx.SendRequestNoWait(baseCommand(cmdType, message))
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
