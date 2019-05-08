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

package pulsar

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/url"
	"pulsar-client-go/pulsar/internal"
	pb "pulsar-client-go/pulsar/internal/pulsar_proto"
)

type client struct {
	options ClientOptions

	cnxPool       internal.ConnectionPool
	rpcClient     internal.RpcClient
	lookupService internal.LookupService

	handlers            map[internal.Closable]bool
	producerIdGenerator uint64
	consumerIdGenerator uint64
}

func newClient(options ClientOptions) (Client, error) {
	if options.URL == "" {
		return nil, newError(ResultInvalidConfiguration, "URL is required for client")
	}

	url, err := url.Parse(options.URL)
	if err != nil {
		log.WithError(err).Error("Failed to parse service URL")
		return nil, newError(ResultInvalidConfiguration, "Invalid service URL")
	}

	var tlsConfig *internal.TLSOptions
	if url.Scheme == "pulsar" {
		tlsConfig = nil
	} else if url.Scheme == "pulsar+ssl" {
		tlsConfig = &internal.TLSOptions{
			AllowInsecureConnection: options.TLSAllowInsecureConnection,
			TrustCertsFilePath:      options.TLSTrustCertsFilePath,
			ValidateHostname:        options.TLSValidateHostname,
		}
		if err != nil {
			return nil, err
		}
	} else {
		return nil, newError(ResultInvalidConfiguration, fmt.Sprintf("Invalid URL scheme '%s'", url.Scheme))
	}

	c := &client{
		cnxPool: internal.NewConnectionPool(tlsConfig),
	}
	c.rpcClient = internal.NewRpcClient(url, c.cnxPool)
	c.lookupService = internal.NewLookupService(c.rpcClient, url)
	c.handlers = make(map[internal.Closable]bool)
	return c, nil
}

func (client *client) CreateProducer(options ProducerOptions) (Producer, error) {
	producer, err := newProducer(client, &options)
	if err == nil {
		client.handlers[producer] = true
	}
	return producer, err
}

func (client *client) Subscribe(options ConsumerOptions) (Consumer, error) {
	// TODO: Implement consumer
	return nil, nil
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

	id := client.rpcClient.NewRequestId()
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
	} else {
		// Non-partitioned topic
		return []string{topicName.Name}, nil
	}
}

func (client *client) Close() error {
	for handler := range client.handlers {
		if err := handler.Close(); err != nil {
			return err
		}
	}

	return nil
}

func getTlsConfig(options ClientOptions) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: options.TLSAllowInsecureConnection,
	}

	if options.TLSTrustCertsFilePath != "" {
		caCerts, err := ioutil.ReadFile(options.TLSTrustCertsFilePath)
		if err != nil {
			return nil, err
		}

		tlsConfig.RootCAs = x509.NewCertPool()
		ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(caCerts))
		if !ok {
			return nil, errors.New("failed to parse root CAs certificates")
		}
	}

	if options.TLSValidateHostname {
		tlsConfig.ServerName = options.URL
	}

	return tlsConfig, nil
}
