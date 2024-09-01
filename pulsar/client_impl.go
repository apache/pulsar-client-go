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
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/auth"
	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const (
	defaultOperationTimeout            = 30 * time.Second
	defaultKeepAliveInterval           = 30 * time.Second
	defaultMemoryLimitBytes            = 64 * 1024 * 1024
	defaultMemoryLimitTriggerThreshold = 0.95
	defaultConnMaxIdleTime             = 180 * time.Second
	minConnMaxIdleTime                 = 60 * time.Second
)

type client struct {
	cnxPool          internal.ConnectionPool
	rpcClient        internal.RPCClient
	handlers         internal.ClientHandlers
	lookupService    internal.LookupService
	metrics          *internal.Metrics
	tcClient         *transactionCoordinatorClient
	memLimit         internal.MemoryLimitController
	closeOnce        sync.Once
	operationTimeout time.Duration
	tlsEnabled       bool

	log log.Logger
}

func newClient(options ClientOptions) (Client, error) {
	var logger log.Logger
	if options.Logger != nil {
		logger = options.Logger
	} else {
		logger = log.NewLoggerWithLogrus(logrus.StandardLogger())
	}

	connectionMaxIdleTime := options.ConnectionMaxIdleTime
	if connectionMaxIdleTime == 0 {
		connectionMaxIdleTime = defaultConnMaxIdleTime
	} else if connectionMaxIdleTime > 0 && connectionMaxIdleTime < minConnMaxIdleTime {
		return nil, newError(InvalidConfiguration, fmt.Sprintf("Connection max idle time should be at least %f "+
			"seconds", minConnMaxIdleTime.Seconds()))
	} else {
		logger.Debugf("Disable auto release idle connections")
	}

	if options.URL == "" {
		return nil, newError(InvalidConfiguration, "URL is required for client")
	}

	url, err := url.Parse(options.URL)
	if err != nil {
		logger.WithError(err).Error("Failed to parse service URL")
		return nil, newError(InvalidConfiguration, "Invalid service URL")
	}

	var tlsConfig *internal.TLSOptions
	switch url.Scheme {
	case "pulsar", "http":
		tlsConfig = nil
	case "pulsar+ssl", "https":
		tlsConfig = &internal.TLSOptions{
			AllowInsecureConnection: options.TLSAllowInsecureConnection,
			KeyFile:                 options.TLSKeyFilePath,
			CertFile:                options.TLSCertificateFile,
			TrustCertsFilePath:      options.TLSTrustCertsFilePath,
			ValidateHostname:        options.TLSValidateHostname,
			ServerName:              url.Hostname(),
			CipherSuites:            options.TLSCipherSuites,
			MinVersion:              options.TLSMinVersion,
			MaxVersion:              options.TLSMaxVersion,
		}
	default:
		return nil, newError(InvalidConfiguration, fmt.Sprintf("Invalid URL scheme '%s'", url.Scheme))
	}

	var authProvider auth.Provider
	var ok bool

	if options.Authentication == nil {
		authProvider = auth.NewAuthDisabled()
	} else {
		authProvider, ok = options.Authentication.(auth.Provider)
		if !ok {
			return nil, newError(AuthenticationError, "invalid auth provider interface")
		}
	}
	err = authProvider.Init()
	if err != nil {
		return nil, err
	}

	// the default timeout respects Go's default timeout which is no timeout
	// Missing user specified timeout renders 0 values that matches
	// net.Dailer's default if time.Duration value is not initialized
	connectionTimeout := options.ConnectionTimeout

	operationTimeout := options.OperationTimeout
	if operationTimeout.Nanoseconds() == 0 {
		operationTimeout = defaultOperationTimeout
	}

	maxConnectionsPerHost := options.MaxConnectionsPerBroker
	if maxConnectionsPerHost <= 0 {
		maxConnectionsPerHost = 1
	}

	if options.MetricsCardinality == 0 {
		options.MetricsCardinality = MetricsCardinalityNamespace
	}

	if options.MetricsRegisterer == nil {
		options.MetricsRegisterer = prometheus.DefaultRegisterer
	}

	var metrics *internal.Metrics
	if options.CustomMetricsLabels != nil {
		metrics = internal.NewMetricsProvider(
			int(options.MetricsCardinality), options.CustomMetricsLabels, options.MetricsRegisterer)
	} else {
		metrics = internal.NewMetricsProvider(
			int(options.MetricsCardinality), map[string]string{}, options.MetricsRegisterer)
	}

	keepAliveInterval := options.KeepAliveInterval
	if keepAliveInterval.Nanoseconds() == 0 {
		keepAliveInterval = defaultKeepAliveInterval
	}

	memLimitBytes := options.MemoryLimitBytes
	if memLimitBytes == 0 {
		memLimitBytes = defaultMemoryLimitBytes
	}

	c := &client{
		cnxPool: internal.NewConnectionPool(tlsConfig, authProvider, connectionTimeout, keepAliveInterval,
			maxConnectionsPerHost, logger, metrics, connectionMaxIdleTime),
		log:              logger,
		metrics:          metrics,
		memLimit:         internal.NewMemoryLimitController(memLimitBytes, defaultMemoryLimitTriggerThreshold),
		operationTimeout: operationTimeout,
		tlsEnabled:       tlsConfig != nil,
	}

	c.rpcClient = internal.NewRPCClient(url, c.cnxPool, operationTimeout, logger, metrics,
		options.ListenerName, tlsConfig, authProvider)

	c.lookupService = c.rpcClient.LookupService("")

	c.handlers = internal.NewClientHandlers()

	if options.EnableTransaction {
		c.tcClient = newTransactionCoordinatorClientImpl(c)
		err = c.tcClient.start()
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (c *client) NewTransaction(timeout time.Duration) (Transaction, error) {
	id, err := c.tcClient.newTransaction(timeout)
	if err != nil {
		return nil, err
	}
	return newTransaction(*id, c.tcClient, timeout), nil
}

func (c *client) CreateProducer(options ProducerOptions) (Producer, error) {
	producer, err := newProducer(c, &options)
	if err == nil {
		c.handlers.Add(producer)
	}
	return producer, err
}

func (c *client) Subscribe(options ConsumerOptions) (Consumer, error) {
	consumer, err := newConsumer(c, options)
	if err != nil {
		return nil, err
	}
	c.handlers.Add(consumer)
	return consumer, nil
}

func (c *client) CreateReader(options ReaderOptions) (Reader, error) {
	reader, err := newReader(c, options)
	if err != nil {
		return nil, err
	}
	c.handlers.Add(reader)
	return reader, nil
}

func (c *client) CreateTableView(options TableViewOptions) (TableView, error) {
	tableView, err := newTableView(c, options)
	if err != nil {
		return nil, err
	}
	c.handlers.Add(tableView)
	return tableView, nil
}

func (c *client) TopicPartitions(topic string) ([]string, error) {
	topicName, err := internal.ParseTopicName(topic)
	if err != nil {
		return nil, err
	}

	r, err := c.lookupService.GetPartitionedTopicMetadata(topic)
	if err != nil {
		return nil, err
	}
	if r != nil {
		if r.Partitions > 0 {
			partitions := make([]string, r.Partitions)
			for i := 0; i < r.Partitions; i++ {
				partitions[i] = fmt.Sprintf("%s-partition-%d", topic, i)
			}
			return partitions, nil
		}
	}

	// Non-partitioned topic
	return []string{topicName.Name}, nil
}

func (c *client) Close() {
	c.closeOnce.Do(func() {
		c.handlers.Close()
		c.cnxPool.Close()
		c.lookupService.Close()
	})
}

func (c *client) selectServiceURL(brokerServiceURL, brokerServiceURLTLS string) string {
	if c.tlsEnabled {
		return brokerServiceURLTLS
	}
	return brokerServiceURL
}
