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
	"fmt"
	"net/url"

	"github.com/apache/pulsar-client-go/pulsar/internal/pb"
	"github.com/golang/protobuf/proto"

	log "github.com/sirupsen/logrus"
)

// LookupResult encapsulates a struct for lookup a request, containing two parts: LogicalAddr, PhysicalAddr.
// logicalAddress is the address to use as the broker tag
// physicalAddress is the real address where the TCP connection should be made
type LookupResult struct {
	LogicalAddr  *url.URL
	PhysicalAddr *url.URL
}

// LookupService is a interface of lookup service.
type LookupService interface {
	// Lookup perform a lookup for the given topic, confirm the location of the broker
	// where the topic is located, and return the LookupResult.
	Lookup(topic string) (*LookupResult, error)
}

type lookupService struct {
	host       HostResolve
	rpcClient  RPCClient
	tlsEnabled bool
}

// NewLookupService init a lookup service struct and return an object of LookupService.
func NewLookupService(rpcClient RPCClient, host HostResolve, tlsEnabled bool) LookupService {
	return &lookupService{
		host:       host,
		rpcClient:  rpcClient,
		tlsEnabled: tlsEnabled,
	}
}

func (ls *lookupService) getBrokerAddress(lr *pb.CommandLookupTopicResponse) (logicalAddress *url.URL,
	physicalAddress *url.URL, err error) {
	if ls.tlsEnabled {
		logicalAddress, err = url.ParseRequestURI(lr.GetBrokerServiceUrlTls())
	} else {
		logicalAddress, err = url.ParseRequestURI(lr.GetBrokerServiceUrl())
	}

	if err != nil {
		return nil, nil, err
	}

	var physicalAddr *url.URL
	if lr.GetProxyThroughServiceUrl() {
		physicalAddr, err = ls.host.GetHost()
		if err != nil {
			return nil, nil, err
		}
	} else {
		physicalAddr = logicalAddress
	}

	return logicalAddress, physicalAddr, nil
}

// Follow brokers redirect up to certain number of times
const lookupResultMaxRedirect = 20

func (ls *lookupService) Lookup(topic string) (*LookupResult, error) {
	id := ls.rpcClient.NewRequestID()
	res, err := ls.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_LOOKUP, &pb.CommandLookupTopic{
		RequestId:     &id,
		Topic:         &topic,
		Authoritative: proto.Bool(false),
	})
	if err != nil {
		return nil, err
	}
	log.Debugf("Got topic{%s} lookup response: %+v", topic, res)

	for i := 0; i < lookupResultMaxRedirect; i++ {
		lr := res.Response.LookupTopicResponse
		switch *lr.Response {

		case pb.CommandLookupTopicResponse_Redirect:
			//logicalAddress, physicalAddr, err := ls.getBrokerAddress(lr)
			if err != nil {
				return nil, err
			}

			log.Debugf("Follow topic{%s} redirect to broker. %v / %v - Use proxy: %v",
				topic, lr.BrokerServiceUrl, lr.BrokerServiceUrlTls, lr.ProxyThroughServiceUrl)

			id := ls.rpcClient.NewRequestID()
			res, err = ls.rpcClient.Request(id, pb.BaseCommand_LOOKUP, &pb.CommandLookupTopic{
				RequestId:     &id,
				Topic:         &topic,
				Authoritative: lr.Authoritative,
			})
			if err != nil {
				return nil, err
			}

			// Process the response at the top of the loop
			continue

		case pb.CommandLookupTopicResponse_Connect:
			log.Debugf("Successfully looked up topic{%s} on broker. %s / %s - Use proxy: %t",
				topic, lr.GetBrokerServiceUrl(), lr.GetBrokerServiceUrlTls(), lr.GetProxyThroughServiceUrl())

			logicalAddress, physicalAddress, err := ls.getBrokerAddress(lr)
			if err != nil {
				return nil, err
			}

			return &LookupResult{
				LogicalAddr:  logicalAddress,
				PhysicalAddr: physicalAddress,
			}, nil

		case pb.CommandLookupTopicResponse_Failed:
			errorMsg := ""
			if lr.Error != nil {
				errorMsg = lr.Error.String()
			}
			log.Warnf("Failed to lookup topic: %s, error msg: %s", topic, errorMsg)
			return nil, fmt.Errorf("failed to lookup topic: %s", errorMsg)
		}
	}

	return nil, errors.New("exceeded max number of redirection during topic lookup")
}
