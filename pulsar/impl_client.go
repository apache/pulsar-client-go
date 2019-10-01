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

package pulsar

import (
	"fmt"
	"net/url"
	"time"

	"github.com/apache/pulsar-client-go/pkg/auth"
	"github.com/apache/pulsar-client-go/pkg/pb"
	"github.com/apache/pulsar-client-go/pulsar/internal"

	log "github.com/sirupsen/logrus"
)

type client struct {
	options ClientOptions

	cnxPool       internal.ConnectionPool
	rpcClient     internal.RPCClient
	lookupService internal.LookupService
	auth          auth.Provider

	handlers            map[internal.Closable]bool
	producerIDGenerator uint64
	consumerIDGenerator uint64
}

func defaultClientOptions() *ClientOptions {
	return &ClientOptions{
		OperationTimeout: 30 * time.Second,
		Authentication:   auth.NewAuthDisabled(),
		TlsConfig:        nil,
	}
}

func newClient(URL string, opts ...ClientOption) (Client, error) {
	if URL == "" {
		return nil, newError(ResultInvalidConfiguration, "URL is required for client")
	}

	url, err := url.Parse(URL)
	if err != nil {
		log.WithError(err).Error("Failed to parse service URL")
		return nil, newError(ResultInvalidConfiguration, "Invalid service URL")
	}

	options := defaultClientOptions()
	for _, opt := range opts {
		opt(options)
	}
	options.URL = URL

	switch url.Scheme {
	case "pulsar":
		if options.TlsConfig != nil {
			return nil, newError(ResultInvalidConfiguration, fmt.Sprintf("tlsConfig should be nil for scheme:'%s'", url.Scheme))
		}
	case "pulsar+ssl":
		if options.TlsConfig == nil {
			return nil, newError(ResultInvalidConfiguration, fmt.Sprintf("no tlsConfig for scheme:'%s'", url.Scheme))
		}
	default:
		return nil, newError(ResultInvalidConfiguration, fmt.Sprintf("Invalid URL scheme '%s'", url.Scheme))
	}

	c := &client{
		cnxPool: internal.NewConnectionPool(options.TlsConfig, options.Authentication),
	}
	c.rpcClient = internal.NewRPCClient(url, c.cnxPool)
	c.lookupService = internal.NewLookupService(c.rpcClient, url)
	c.handlers = make(map[internal.Closable]bool)
	return c, nil
}

func (client *client) CreateProducer(topic string, options ...ProducerOption) (Producer, error) {
	producer, err := newProducer(client, topic, options...)
	if err == nil {
		client.handlers[producer] = true
	}
	return producer, err
}

func (client *client) Subscribe(name string, opts ...ConsumerOption) (Consumer, error) {
	consumer, err := newConsumer(client, name, opts...)
	if err != nil {
		return nil, err
	}
	client.handlers[consumer] = true
	return consumer, nil
}

func (client *client) CreateReader(options ReaderOptions) (Reader, error) {
	// TODO: Implement reader
	return nil, nil
}

func (client *client) TopicPartitions(topic string) ([]string, error) {
	topicName, err := internal.ParseTopicName(topic)
	if err != nil {
		return nil, err
	}

	id := client.rpcClient.NewRequestID()
	res, err := client.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_PARTITIONED_METADATA,
		&pb.CommandPartitionedTopicMetadata{
			RequestId: &id,
			Topic:     &topicName.Name,
		})
	if err != nil {
		return nil, err
	}

	r := res.Response.PartitionMetadataResponse
	if r.Error != nil {
		return nil, newError(ResultLookupError, r.GetError().String())
	}

	if r.GetPartitions() > 0 {
		partitions := make([]string, r.GetPartitions())
		for i := 0; i < int(r.GetPartitions()); i++ {
			partitions[i] = fmt.Sprintf("%s-partition-%d", topic, i)
		}
		return partitions, nil
	}
	// Non-partitioned topic
	return []string{topicName.Name}, nil
}

func (client *client) Close() error {
	for handler := range client.handlers {
		if err := handler.Close(); err != nil {
			return err
		}
	}

	return nil
}
