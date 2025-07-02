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
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"google.golang.org/protobuf/proto"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

// LookupResult encapsulates a struct for lookup a request, containing two parts: LogicalAddr, PhysicalAddr.
type LookupResult struct {
	LogicalAddr  *url.URL
	PhysicalAddr *url.URL
}

// LookupSchema return lookup schema result
type LookupSchema struct {
	SchemaType SchemaType
	Data       []byte
	Properties map[string]string
}

// GetTopicsOfNamespaceMode for CommandGetTopicsOfNamespace_Mode
type GetTopicsOfNamespaceMode string

const (
	Persistent    GetTopicsOfNamespaceMode = "PERSISTENT"
	NonPersistent                          = "NON_PERSISTENT"
	All                                    = "ALL"
)

// PartitionedTopicMetadata encapsulates a struct for metadata of a partitioned topic
type PartitionedTopicMetadata struct {
	Partitions int `json:"partitions"` // Number of partitions for the topic
}

// LookupService is a interface of lookup service.
type LookupService interface {
	// Lookup perform a lookup for the given topic, confirm the location of the broker
	// where the topic is located, and return the LookupResult.
	Lookup(topic string) (*LookupResult, error)

	// GetPartitionedTopicMetadata perform a CommandPartitionedTopicMetadata request for
	// the given topic, returns the CommandPartitionedTopicMetadataResponse as the result.
	GetPartitionedTopicMetadata(topic string) (*PartitionedTopicMetadata, error)

	// GetTopicsOfNamespace returns all the topics name for a given namespace.
	GetTopicsOfNamespace(namespace string, mode GetTopicsOfNamespaceMode) ([]string, error)

	// GetSchema returns schema for a given version.
	GetSchema(topic string, schemaVersion []byte) (*LookupSchema, error)

	GetBrokerAddress(brokerServiceURL string, proxyThroughServiceURL bool) (*LookupResult, error)

	ServiceNameResolver() *ServiceNameResolver

	// Closable Allow Lookup Service's internal client to be able to closed
	Closable
}

type lookupService struct {
	rpcClient           RPCClient
	serviceNameResolver ServiceNameResolver
	tlsEnabled          bool
	listenerName        string
	lookupProperties    []*pb.KeyValue
	log                 log.Logger
	metrics             *Metrics
}

// NewLookupService init a lookup service struct and return an object of LookupService.
func NewLookupService(rpcClient RPCClient, serviceURL *url.URL, serviceNameResolver ServiceNameResolver,
	tlsEnabled bool, listenerName string,
	lookupProperties []*pb.KeyValue, logger log.Logger, metrics *Metrics) LookupService {
	return &lookupService{
		rpcClient:           rpcClient,
		serviceNameResolver: serviceNameResolver,
		tlsEnabled:          tlsEnabled,
		log:                 logger.SubLogger(log.Fields{"serviceURL": serviceURL}),
		lookupProperties:    lookupProperties,
		metrics:             metrics,
		listenerName:        listenerName,
	}
}

func (ls *lookupService) GetSchema(topic string, schemaVersion []byte) (*LookupSchema, error) {
	id := ls.rpcClient.NewRequestID()
	req := &pb.CommandGetSchema{
		RequestId:     proto.Uint64(id),
		Topic:         proto.String(topic),
		SchemaVersion: schemaVersion,
	}
	res, err := ls.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_GET_SCHEMA, req)
	if err != nil {
		return &LookupSchema{}, err
	}
	if res.Response.Error != nil {
		return &LookupSchema{}, errors.New(res.Response.GetError().String())
	}

	//	deserialize pbSchema and convert it to LookupSchema struct
	pbSchema := res.Response.GetSchemaResponse.Schema
	if pbSchema == nil {
		err = fmt.Errorf("schema not found for topic: [ %v ], schema version : [ %v ]", topic, schemaVersion)
		return &LookupSchema{}, err
	}
	return &LookupSchema{
		SchemaType: SchemaType(int(*pbSchema.Type)),
		Data:       pbSchema.SchemaData,
		Properties: ConvertToStringMap(pbSchema.Properties),
	}, nil
}

func (ls *lookupService) GetBrokerAddress(brokerServiceURL string, proxyThroughServiceURL bool) (*LookupResult, error) {
	logicalAddress, err := url.ParseRequestURI(brokerServiceURL)
	if err != nil {
		return nil, err
	}

	var physicalAddress *url.URL
	if proxyThroughServiceURL {
		physicalAddress, err = ls.serviceNameResolver.ResolveHost()
		if err != nil {
			return nil, err
		}
	} else {
		physicalAddress = logicalAddress
	}

	return &LookupResult{
		LogicalAddr:  logicalAddress,
		PhysicalAddr: physicalAddress,
	}, nil
}

// Follow brokers redirect up to certain number of times
const lookupResultMaxRedirect = 20

func (ls *lookupService) Lookup(topic string) (*LookupResult, error) {
	ls.metrics.LookupRequestsCount.Inc()
	id := ls.rpcClient.NewRequestID()

	res, err := ls.rpcClient.RequestToHost(&ls.serviceNameResolver, id, pb.BaseCommand_LOOKUP,
		&pb.CommandLookupTopic{
			RequestId:              &id,
			Topic:                  &topic,
			Authoritative:          proto.Bool(false),
			AdvertisedListenerName: proto.String(ls.listenerName),
			Properties:             ls.lookupProperties,
		})
	if err != nil {
		return nil, err
	}
	ls.log.Debugf("Got topic{%s} lookup response: %+v", topic, res)

	for i := 0; i < lookupResultMaxRedirect; i++ {
		lr := res.Response.LookupTopicResponse
		switch *lr.Response {

		case pb.CommandLookupTopicResponse_Redirect:
			brokerServiceURL := selectServiceURL(ls.tlsEnabled, lr.GetBrokerServiceUrl(), lr.GetBrokerServiceUrlTls())
			lookupResult, err := ls.GetBrokerAddress(brokerServiceURL, lr.GetProxyThroughServiceUrl())
			if err != nil {
				return nil, err
			}

			ls.log.Debugf("Follow topic{%s} redirect to broker. %v / %v - Use proxy: %v",
				topic, lr.BrokerServiceUrl, lr.BrokerServiceUrlTls, lr.ProxyThroughServiceUrl)

			id := ls.rpcClient.NewRequestID()
			res, err = ls.rpcClient.Request(lookupResult.LogicalAddr, lookupResult.PhysicalAddr, id, pb.BaseCommand_LOOKUP,
				&pb.CommandLookupTopic{
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

			brokerServiceURL := selectServiceURL(ls.tlsEnabled, lr.GetBrokerServiceUrl(), lr.GetBrokerServiceUrlTls())
			return ls.GetBrokerAddress(brokerServiceURL, lr.GetProxyThroughServiceUrl())
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

func (ls *lookupService) GetPartitionedTopicMetadata(topic string) (*PartitionedTopicMetadata,
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

	var partitionedTopicMetadata PartitionedTopicMetadata

	if res.Response.Error != nil {
		return nil, errors.New(res.Response.GetError().String())
	}

	if res.Response.PartitionMetadataResponse != nil {
		if res.Response.PartitionMetadataResponse.Error != nil {
			return nil, errors.New(res.Response.PartitionMetadataResponse.GetError().String())
		}

		partitionedTopicMetadata.Partitions = int(res.Response.PartitionMetadataResponse.GetPartitions())
	} else {
		return nil, fmt.Errorf("no partitioned metadata for topic{%s} in lookup response", topic)
	}

	return &partitionedTopicMetadata, nil
}

func (ls *lookupService) GetTopicsOfNamespace(namespace string, mode GetTopicsOfNamespaceMode) ([]string, error) {
	id := ls.rpcClient.NewRequestID()
	pbMode := pb.CommandGetTopicsOfNamespace_Mode(pb.CommandGetTopicsOfNamespace_Mode_value[string(mode)])
	req := &pb.CommandGetTopicsOfNamespace{
		RequestId: proto.Uint64(id),
		Namespace: proto.String(namespace),
		Mode:      &pbMode,
	}
	res, err := ls.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_GET_TOPICS_OF_NAMESPACE, req)
	if err != nil {
		return nil, err
	}
	if res.Response.Error != nil {
		return []string{}, errors.New(res.Response.GetError().String())
	}

	return res.Response.GetTopicsOfNamespaceResponse.GetTopics(), nil
}

func (ls *lookupService) Close() {}

func (ls *lookupService) ServiceNameResolver() *ServiceNameResolver {
	return &ls.serviceNameResolver
}

const HTTPLookupServiceBasePathV1 string = "/lookup/v2/destination/"
const HTTPLookupServiceBasePathV2 string = "/lookup/v2/topic/"
const HTTPAdminServiceV1Format string = "/admin/%s/partitions"
const HTTPAdminServiceV2Format string = "/admin/v2/%s/partitions"
const HTTPTopicUnderNamespaceV1 string = "/admin/namespaces/%s/destinations?mode=%s"
const HTTPTopicUnderNamespaceV2 string = "/admin/v2/namespaces/%s/topics?mode=%s"
const HTTPSchemaV2 string = "/admin/v2/schemas/%s/schema"
const HTTPSchemaWithVersionV2 string = "/admin/v2/schemas/%s/schema/%d"

type httpLookupData struct {
	BrokerURL    string `json:"brokerUrl"`
	BrokerURLTLS string `json:"brokerUrlTls"`
	HTTPURL      string `json:"httpUrl"`
	HTTPURLTLS   string `json:"httpUrlTls"`
}

type httpLookupService struct {
	httpClient          HTTPClient
	serviceNameResolver ServiceNameResolver
	tlsEnabled          bool
	log                 log.Logger
	metrics             *Metrics
}

type httpLookupSchema struct {
	HTTPSchemaType string            `json:"type"`
	Data           string            `json:"data"`
	Properties     map[string]string `json:"properties"`
}

func (h *httpLookupService) GetBrokerAddress(brokerServiceURL string, _ bool) (*LookupResult, error) {
	logicalAddress, err := url.ParseRequestURI(brokerServiceURL)
	if err != nil {
		return nil, err
	}
	return &LookupResult{
		LogicalAddr:  logicalAddress,
		PhysicalAddr: logicalAddress,
	}, err
}

func (h *httpLookupService) Lookup(topic string) (*LookupResult, error) {
	topicName, err := ParseTopicName(topic)
	if err != nil {
		return nil, err
	}

	basePath := HTTPLookupServiceBasePathV2
	if !IsV2TopicName(topicName) {
		basePath = HTTPLookupServiceBasePathV1
	}

	lookupData := &httpLookupData{}
	err = h.httpClient.Get(basePath+GetTopicRestPath(topicName), lookupData, nil)
	if err != nil {
		return nil, err
	}

	h.log.Debugf("Successfully looked up topic{%s} on http broker. %+v",
		topic, lookupData)

	brokerServiceURL := selectServiceURL(h.tlsEnabled, lookupData.BrokerURL, lookupData.BrokerURLTLS)
	return h.GetBrokerAddress(brokerServiceURL, false /* ignored */)
}

func (h *httpLookupService) GetPartitionedTopicMetadata(topic string) (*PartitionedTopicMetadata,
	error) {
	topicName, err := ParseTopicName(topic)
	if err != nil {
		return nil, err
	}

	format := HTTPAdminServiceV2Format
	if !IsV2TopicName(topicName) {
		format = HTTPAdminServiceV1Format
	}

	path := fmt.Sprintf(format, GetTopicRestPath(topicName))

	tMetadata := &PartitionedTopicMetadata{}

	err = h.httpClient.Get(path, tMetadata, map[string]string{"checkAllowAutoCreation": "true"})
	if err != nil {
		return nil, err
	}

	h.log.Debugf("Got topic{%s} partitioned metadata response: %+v", topic, tMetadata)

	return tMetadata, nil
}

func (h *httpLookupService) GetTopicsOfNamespace(namespace string, mode GetTopicsOfNamespaceMode) ([]string, error) {

	format := HTTPTopicUnderNamespaceV2
	if !IsV2Namespace(namespace) {
		format = HTTPTopicUnderNamespaceV1
	}

	path := fmt.Sprintf(format, namespace, string(mode))

	topics := []string{}

	err := h.httpClient.Get(path, &topics, nil)
	if err != nil {
		return nil, err
	}

	h.log.Debugf("Got namespace{%s} mode{%s} topics response: %+v", namespace, mode, topics)

	return topics, nil
}

func (h *httpLookupService) GetSchema(topic string, schemaVersion []byte) (*LookupSchema, error) {
	topicName, err := ParseTopicName(topic)
	if err != nil {
		return nil, err
	}
	topicRestPath := fmt.Sprintf("%s/%s", topicName.Namespace, topicName.Topic)
	var path string
	if schemaVersion != nil {
		path = fmt.Sprintf(HTTPSchemaWithVersionV2, topicRestPath, int64(binary.BigEndian.Uint64(schemaVersion)))
	} else {
		path = fmt.Sprintf(HTTPSchemaV2, topicRestPath)
	}
	lookupSchema := &httpLookupSchema{}
	if err := h.httpClient.Get(path, &lookupSchema, nil); err != nil {
		if strings.HasPrefix(err.Error(), "Code: 404") {
			err = fmt.Errorf("schema not found for topic: [ %v ], schema version : [ %v ]", topic, schemaVersion)
		}
		h.log.Errorf("schema [ %v ] request error, schema version : [ %v ]", topic, schemaVersion)
		return &LookupSchema{}, err
	}

	//	deserialize httpSchema and convert it to LookupSchema struct
	schemaType, exists := HTTPSchemaTypeMap[strings.ToUpper(lookupSchema.HTTPSchemaType)]
	if !exists {
		err = fmt.Errorf("unsupported schema type [%s] for topic: [ %v ], schema version : [ %v ]",
			lookupSchema.HTTPSchemaType,
			topic,
			schemaVersion,
		)
		return nil, err
	}
	return &LookupSchema{
		SchemaType: schemaType,
		Data:       []byte(lookupSchema.Data),
		Properties: lookupSchema.Properties,
	}, nil
}

func (h *httpLookupService) ServiceNameResolver() *ServiceNameResolver {
	return &h.serviceNameResolver
}

func (h *httpLookupService) Close() {
	h.httpClient.Close()
}

// NewHTTPLookupService init a http based lookup service struct and return an object of LookupService.
func NewHTTPLookupService(httpClient HTTPClient, serviceURL *url.URL, serviceNameResolver ServiceNameResolver,
	tlsEnabled bool, logger log.Logger, metrics *Metrics) LookupService {

	return &httpLookupService{
		httpClient:          httpClient,
		serviceNameResolver: serviceNameResolver,
		tlsEnabled:          tlsEnabled,
		log:                 logger.SubLogger(log.Fields{"serviceURL": serviceURL}),
		metrics:             metrics,
	}
}

func selectServiceURL(tlsEnabled bool, brokerServiceURL, brokerServiceURLTLS string) string {
	if tlsEnabled {
		return brokerServiceURLTLS
	}
	return brokerServiceURL
}
