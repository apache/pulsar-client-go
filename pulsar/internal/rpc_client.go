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
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/log"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/gogo/protobuf/proto"
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
		cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error)

	RequestOnCnxNoWait(cnx Connection, cmdType pb.BaseCommand_Type, message proto.Message) error

	RequestOnCnx(cnx Connection, requestID uint64, cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error)
}

type rpcClient struct {
	serviceURL          *url.URL
	pool                ConnectionPool
	requestTimeout      time.Duration
	initBackoff         time.Duration
	maxBackoff          time.Duration
	requestIDGenerator  uint64
	producerIDGenerator uint64
	consumerIDGenerator uint64
	log                 log.Logger
	metrics             *Metrics
}

func NewRPCClient(serviceURL *url.URL, pool ConnectionPool,
	requestTimeout, initBackoff, maxBackoff time.Duration, logger log.Logger, metrics *Metrics) RPCClient {
	return &rpcClient{
		serviceURL:     serviceURL,
		pool:           pool,
		requestTimeout: requestTimeout,
		initBackoff:    initBackoff,
		maxBackoff:     maxBackoff,
		log:            logger.SubLogger(log.Fields{"serviceURL": serviceURL}),
		metrics:        metrics,
	}
}

func (c *rpcClient) RequestToAnyBroker(requestID uint64, cmdType pb.BaseCommand_Type,
	message proto.Message) (*RPCResult, error) {

	rpcResult, err := c.Request(c.serviceURL, c.serviceURL, requestID, cmdType, message)
	if _, ok := err.(net.Error); ok {
		// We can retry this kind of requests over a connection error because they're
		// not specific to a particular broker.
		backoff := NewBackoff(c.initBackoff, c.maxBackoff, 0)
		startTime := time.Now()
		var retryTime time.Duration

		for time.Since(startTime) < c.requestTimeout {
			retryTime = backoff.Next()
			c.log.Debugf("Retrying request in {%v} with timeout in {%v}", retryTime, c.requestTimeout)
			time.Sleep(retryTime)

			rpcResult, err = c.Request(c.serviceURL, c.serviceURL, requestID, cmdType, message)
			if _, ok := err.(net.Error); ok {
				continue
			} else {
				// We either succeeded or encountered a non connection error
				break
			}
		}
	}

	return rpcResult, err
}

func (c *rpcClient) Request(logicalAddr *url.URL, physicalAddr *url.URL, requestID uint64,
	cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error) {
	c.metrics.RPCRequestCount.Inc()
	cnx, err := c.pool.GetConnection(logicalAddr, physicalAddr)
	if err != nil {
		return nil, err
	}

	type Res struct {
		*RPCResult
		error
	}
	ch := make(chan Res, 10)

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

func (c *rpcClient) RequestOnCnx(cnx Connection, requestID uint64, cmdType pb.BaseCommand_Type,
	message proto.Message) (*RPCResult, error) {
	c.metrics.RPCRequestCount.Inc()
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

func (c *rpcClient) RequestOnCnxNoWait(cnx Connection, cmdType pb.BaseCommand_Type, message proto.Message) error {
	c.metrics.RPCRequestCount.Inc()
	return cnx.SendRequestNoWait(baseCommand(cmdType, message))
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
