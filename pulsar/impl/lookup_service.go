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

package impl

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"net/url"
	pb "pulsar-client-go/pulsar/pulsar_proto"
)

type LookupResult struct {
	LogicalAddr  *url.URL
	PhysicalAddr *url.URL
}

type LookupService interface {
	Lookup(topic string) (*LookupResult, error)
}

type lookupService struct {
	rpcClient  RpcClient
	serviceUrl *url.URL
}

func NewLookupService(rpcClient RpcClient, serviceUrl *url.URL) LookupService {
	return &lookupService{
		rpcClient:  rpcClient,
		serviceUrl: serviceUrl,
	}
}

func (ls *lookupService) getBrokerAddress(lr *pb.CommandLookupTopicResponse) (logicalAddress *url.URL, physicalAddress *url.URL, err error) {
	logicalAddress, err = url.ParseRequestURI(lr.GetBrokerServiceUrl())
	if err != nil {
		return nil, nil, err
	}

	var physicalAddr *url.URL
	if lr.GetProxyThroughServiceUrl() {
		physicalAddr = ls.serviceUrl
	} else {
		physicalAddr = logicalAddress
	}

	return logicalAddress, physicalAddr, nil
}

// Follow brokers redirect up to certain number of times
const lookupResultMaxRedirect = 20

func (ls *lookupService) Lookup(topic string) (*LookupResult, error) {
	id := ls.rpcClient.NewRequestId()
	res, err := ls.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_LOOKUP, &pb.CommandLookupTopic{
		RequestId:     &id,
		Topic:         &topic,
		Authoritative: proto.Bool(false),
	})

	for i := 0; i < lookupResultMaxRedirect; i++ {
		if err != nil {
			return nil, err
		}

		log.WithField("topic", topic).Debugf("Got topic lookup response: %s", res)
		lr := res.Response.LookupTopicResponse
		switch *lr.Response {

		case pb.CommandLookupTopicResponse_Redirect:
			logicalAddress, physicalAddr, err := ls.getBrokerAddress(lr)
			if err != nil {
				return nil, err
			}

			log.WithField("topic", topic).Debugf("Follow redirect to broker. %v / %v - Use proxy: %v",
				lr.BrokerServiceUrl, lr.BrokerServiceUrlTls, lr.ProxyThroughServiceUrl)

			id := ls.rpcClient.NewRequestId()
			res, err = ls.rpcClient.Request(logicalAddress, physicalAddr, id, pb.BaseCommand_LOOKUP, &pb.CommandLookupTopic{
				RequestId:     &id,
				Topic:         &topic,
				Authoritative: lr.Authoritative,
			})

			// Process the response at the top of the loop
			continue

		case pb.CommandLookupTopicResponse_Connect:
			log.WithField("topic", topic).Debugf("Successfully looked up topic on broker. %s / %s - Use proxy: %t",
				lr.GetBrokerServiceUrl(), lr.GetBrokerServiceUrlTls(), lr.GetProxyThroughServiceUrl())

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
			log.WithField("topic", topic).Warn("Failed to lookup topic", errorMsg)
			return nil, errors.New(fmt.Sprintf("failed to lookup topic: %s", errorMsg))
		}
	}

	return nil, errors.New("exceeded max number of redirection during topic lookup")
}
