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

	"github.com/gogo/protobuf/proto"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

// LookupResult encapsulates a struct for lookup a request, containing two parts: LogicalAddr, PhysicalAddr.
type LookupResult struct {
	LogicalAddr  *url.URL
	PhysicalAddr *url.URL
}

// LookupService is a interface of lookup service.
type LookupService interface {
	// Lookup perform a lookup for the given topic, confirm the location of the broker
	// where the topic is located, and return the LookupResult.
	Lookup(topic string) (*LookupResult, error)

	// GetPartitionedTopicMetadata perform a CommandPartitionedTopicMetadata request for
	// the given topic, returns the CommandPartitionedTopicMetadataResponse as the result.
	GetPartitionedTopicMetadata(topic string) (*pb.CommandPartitionedTopicMetadataResponse, error)
}

type lookupService struct {
	rpcClient           RPCClient
	serviceNameResolver ServiceNameResolver
	tlsEnabled          bool
	listenerName        string
	log                 log.Logger
	metrics             *Metrics
}

// NewLookupService init a lookup service struct and return an object of LookupService.
func NewLookupService(rpcClient RPCClient, serviceURL *url.URL, serviceNameResolver ServiceNameResolver,
	tlsEnabled bool, listenerName string, logger log.Logger, metrics *Metrics) LookupService {
	return &lookupService{
		rpcClient:           rpcClient,
		serviceNameResolver: serviceNameResolver,
		tlsEnabled:          tlsEnabled,
		log:                 logger.SubLogger(log.Fields{"serviceURL": serviceURL}),
		metrics:             metrics,
		listenerName:        listenerName,
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
		physicalAddr, err = ls.serviceNameResolver.ResolveHost()
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
	ls.metrics.LookupRequestsCount.Inc()
	id := ls.rpcClient.NewRequestID()
	res, err := ls.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_LOOKUP, &pb.CommandLookupTopic{
		RequestId:              &id,
		Topic:                  &topic,
		Authoritative:          proto.Bool(false),
		AdvertisedListenerName: proto.String(ls.listenerName),
	})
	if err != nil {
		return nil, err
	}
	ls.log.Debugf("Got topic{%s} lookup response: %+v", topic, res)

	for i := 0; i < lookupResultMaxRedirect; i++ {
		lr := res.Response.LookupTopicResponse
		switch *lr.Response {

		case pb.CommandLookupTopicResponse_Redirect:
			logicalAddress, physicalAddr, err := ls.getBrokerAddress(lr)
			if err != nil {
				return nil, err
			}

			ls.log.Debugf("Follow topic{%s} redirect to broker. %v / %v - Use proxy: %v",
				topic, lr.BrokerServiceUrl, lr.BrokerServiceUrlTls, lr.ProxyThroughServiceUrl)

			id := ls.rpcClient.NewRequestID()
			res, err = ls.rpcClient.Request(logicalAddress, physicalAddr, id, pb.BaseCommand_LOOKUP, &pb.CommandLookupTopic{
				RequestId:              &id,
				Topic:                  &topic,
				Authoritative:          lr.Authoritative,
				AdvertisedListenerName: proto.String(ls.listenerName),
			})
			if err != nil {
				return nil, err
			}

			// Process the response at the top of the loop
			continue

		case pb.CommandLookupTopicResponse_Connect:
			ls.log.Debugf("Successfully looked up topic{%s} on broker. %s / %s - Use proxy: %t",
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
			ls.log.WithFields(log.Fields{
				"topic":   topic,
				"error":   lr.GetError(),
				"message": lr.GetMessage(),
			}).Warn("Failed to lookup topic")
			return nil, errors.New(lr.GetError().String())
		}
	}

	return nil, errors.New("exceeded max number of redirection during topic lookup")
}

func (ls *lookupService) GetPartitionedTopicMetadata(topic string) (*pb.CommandPartitionedTopicMetadataResponse,
	error) {
	ls.metrics.PartitionedTopicMetadataRequestsCount.Inc()
	topicName, err := ParseTopicName(topic)
	if err != nil {
		return nil, err
	}

	id := ls.rpcClient.NewRequestID()
	res, err := ls.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_PARTITIONED_METADATA,
		&pb.CommandPartitionedTopicMetadata{
			RequestId: &id,
			Topic:     &topicName.Name,
		})
	if err != nil {
		return nil, err
	}
	ls.log.Debugf("Got topic{%s} partitioned metadata response: %+v", topic, res)

	return res.Response.PartitionMetadataResponse, nil
}
