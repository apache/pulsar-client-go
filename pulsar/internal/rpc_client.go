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
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/backoff"

	"github.com/apache/pulsar-client-go/pulsar/auth"
	"github.com/apache/pulsar-client-go/pulsar/log"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrRequestTimeOut happens when request not finished in given requestTimeout.
	ErrRequestTimeOut = errors.New("request timed out")
)

type result struct {
	*RPCResult
	error
}

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

	RequestToHost(serviceNameResolver *ServiceNameResolver, requestID uint64,
		cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error)

	Request(logicalAddr *url.URL, physicalAddr *url.URL, requestID uint64,
		cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error)

	RequestOnCnxNoWait(cnx Connection, cmdType pb.BaseCommand_Type, message proto.Message) error

	RequestOnCnx(cnx Connection, requestID uint64, cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error)

	LookupService(URL string) LookupService
}

type rpcClient struct {
	pool                    ConnectionPool
	requestTimeout          time.Duration
	requestIDGenerator      uint64
	producerIDGenerator     uint64
	consumerIDGenerator     uint64
	log                     log.Logger
	metrics                 *Metrics
	tlsConfig               *TLSOptions
	listenerName            string
	authProvider            auth.Provider
	lookupService           LookupService
	urlLookupServiceMapLock sync.RWMutex
	urlLookupServiceMap     map[string]LookupService
	lookupProperties        []*pb.KeyValue
}

func NewRPCClient(serviceURL *url.URL, pool ConnectionPool,
	requestTimeout time.Duration, logger log.Logger, metrics *Metrics,
	listenerName string, tlsConfig *TLSOptions, authProvider auth.Provider, lookupProperties []*pb.KeyValue) RPCClient {
	c := rpcClient{
		pool:                pool,
		requestTimeout:      requestTimeout,
		log:                 logger.SubLogger(log.Fields{"serviceURL": serviceURL}),
		metrics:             metrics,
		listenerName:        listenerName,
		tlsConfig:           tlsConfig,
		authProvider:        authProvider,
		urlLookupServiceMap: make(map[string]LookupService),
		lookupProperties:    lookupProperties,
	}
	lookupService, err := c.NewLookupService(serviceURL)
	if err != nil {
		panic(err)
	}
	c.lookupService = lookupService

	return &c
}

func (c *rpcClient) requestToHost(serviceNameResolver *ServiceNameResolver,
	requestID uint64, cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error) {
	var err error
	var host *url.URL
	bo := backoff.NewDefaultBackoffWithInitialBackOff(100 * time.Millisecond)
	// we can retry these requests because this kind of request is
	// not specific to any particular broker
	opFn := func() (*RPCResult, error) {
		host, err = (*serviceNameResolver).ResolveHost()
		if err != nil {
			c.log.WithError(err).Errorf("rpc client failed to resolve host")
			return nil, err
		}
		return c.Request(host, host, requestID, cmdType, message)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
	defer cancel()

	rpcResult, err := Retry(ctx, opFn, func(_ error) time.Duration {
		retryTime := bo.Next()
		c.log.Debugf("Retrying request in {%v} with timeout in {%v}", retryTime, c.requestTimeout)
		return retryTime
	})

	return rpcResult, err
}

func (c *rpcClient) RequestToAnyBroker(requestID uint64, cmdType pb.BaseCommand_Type,
	message proto.Message) (*RPCResult, error) {
	return c.requestToHost(c.lookupService.ServiceNameResolver(), requestID, cmdType, message)
}

func (c *rpcClient) RequestToHost(serviceNameResolver *ServiceNameResolver, requestID uint64,
	cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error) {
	return c.requestToHost(serviceNameResolver, requestID, cmdType, message)
}

func (c *rpcClient) Request(logicalAddr *url.URL, physicalAddr *url.URL, requestID uint64,
	cmdType pb.BaseCommand_Type, message proto.Message) (*RPCResult, error) {
	cnx, err := c.pool.GetConnection(logicalAddr, physicalAddr)
	if err != nil {
		return nil, err
	}

	return c.RequestOnCnx(cnx, requestID, cmdType, message)
}

func (c *rpcClient) RequestOnCnx(cnx Connection, requestID uint64, cmdType pb.BaseCommand_Type,
	message proto.Message) (*RPCResult, error) {
	c.metrics.RPCRequestCount.Inc()

	ch := make(chan result, 1)

	cnx.SendRequest(requestID, baseCommand(cmdType, message), func(response *pb.BaseCommand, err error) {
		ch <- result{&RPCResult{
			Cnx:      cnx,
			Response: response,
		}, err}
	})

	timeoutCh := time.After(c.requestTimeout)
	for {
		select {
		case res := <-ch:
			// Ignoring producer not ready response.
			// Continue to wait for the producer to create successfully
			if res.error == nil && res.Response != nil && *res.RPCResult.Response.Type == pb.BaseCommand_PRODUCER_SUCCESS {
				if !res.RPCResult.Response.ProducerSuccess.GetProducerReady() {
					timeoutCh = nil
					break
				}
			}
			return res.RPCResult, res.error
		case <-timeoutCh:
			return nil, ErrRequestTimeOut
		}
	}
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

func (c *rpcClient) LookupService(URL string) LookupService {
	if URL == "" {
		return c.lookupService
	}
	c.urlLookupServiceMapLock.Lock()
	defer c.urlLookupServiceMapLock.Unlock()
	lookupService, ok := c.urlLookupServiceMap[URL]
	if ok {
		return lookupService
	}

	serviceURL, err := url.Parse(URL)
	if err != nil {
		panic(err)
	}

	lookupService, err = c.NewLookupService(serviceURL)
	if err != nil {
		panic(err)
	}
	c.urlLookupServiceMap[URL] = lookupService
	return lookupService

}

func (c *rpcClient) NewLookupService(url *url.URL) (LookupService, error) {

	switch url.Scheme {
	case "pulsar", "pulsar+ssl":
		serviceNameResolver := NewPulsarServiceNameResolver(url)
		return NewLookupService(c, url, serviceNameResolver,
			c.tlsConfig != nil, c.listenerName, c.lookupProperties, c.log, c.metrics), nil
	case "http", "https":
		serviceNameResolver := NewPulsarServiceNameResolver(url)
		httpClient, err := NewHTTPClient(url, serviceNameResolver, c.tlsConfig,
			c.requestTimeout, c.log, c.metrics, c.authProvider)
		if err != nil {
			return nil, err
		}

		return NewHTTPLookupService(
			httpClient, url, serviceNameResolver, c.tlsConfig != nil, c.log, c.metrics), nil
	default:
		panic(fmt.Sprintf("Invalid URL scheme '%s'", url.Scheme))
	}
}

func (c *rpcClient) Close() {
	c.lookupService.Close()
	c.urlLookupServiceMapLock.Lock()
	defer c.urlLookupServiceMapLock.Unlock()
	for _, value := range c.urlLookupServiceMap {
		value.Close()
	}
}
