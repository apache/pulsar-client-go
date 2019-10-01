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
	"time"

	"github.com/apache/pulsar-client-go/pkg/auth"
	"github.com/apache/pulsar-client-go/pulsar/internal"
)

func NewClient(url string, opts ...ClientOption) (Client, error) {
	return newClient(url, opts...)
}

// Opaque interface that represents the authentication credentials
type Authentication interface{}

func NewAuthentication(name string, params string) (Authentication, error) {
	return auth.NewProvider(name, params)
}

// Create new Authentication provider with specified auth token
func NewAuthenticationToken(token string) Authentication {
	return auth.NewAuthenticationToken(token)
}

// Create new Authentication provider with specified auth token from a file
func NewAuthenticationTokenFromFile(tokenFilePath string) Authentication {
	return auth.NewAuthenticationTokenFromFile(tokenFilePath)
}

// Create new Authentication provider with specified TLS certificate and private key
func NewAuthenticationTLS(certificatePath string, privateKeyPath string) Authentication {
	return auth.NewAuthenticationTLS(certificatePath, privateKeyPath)
}

// Create new Athenz Authentication provider with configuration in JSON form
func NewAuthenticationAthenz(authParams string) Authentication {
	// TODO: return newAuthenticationAthenz(authParams)
	return nil
}

// Builder interface that is used to construct a Pulsar Client instance.
type ClientOptions struct {
	// Configure the service URL for the Pulsar service.
	// This parameter is required
	URL string

	ConnectionTimeout time.Duration

	// Set the operation timeout (default: 30 seconds)
	// Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
	// operation will be marked as failed
	OperationTimeout time.Duration

	// Configure the authentication provider. (default: no authentication)
	// Example: `Authentication: NewAuthenticationTLS("my-cert.pem", "my-key.pem")`
	Authentication auth.Provider

	TlsConfig *internal.TLSOptions
}

type ClientOption func(*ClientOptions)

func WithConnectionTimeout(timeout time.Duration) ClientOption {
	return func(options *ClientOptions) {
		options.ConnectionTimeout = timeout
	}
}

func WithOperationTimeout(timeout time.Duration) ClientOption {
	return func(options *ClientOptions) {
		options.OperationTimeout = timeout
	}
}

func WithAuthentication(authProvider auth.Provider) ClientOption {
	return func(options *ClientOptions) {
		options.Authentication = authProvider
	}
}

func WithTLS(trustCertsFilePath string, allowInsecureConnection bool, validateHostname bool) ClientOption {
	return func(options *ClientOptions) {
		options.TlsConfig = &internal.TLSOptions{
			AllowInsecureConnection: allowInsecureConnection,
			TrustCertsFilePath:      trustCertsFilePath,
			ValidateHostname:        validateHostname,
		}
	}
}

type Client interface {
	// Create the producer instance
	// This method will block until the producer is created successfully
	CreateProducer(topic string, opt ...ProducerOption) (Producer, error)

	// Create a `Consumer` by subscribing to a topic.
	//
	// If the subscription does not exist, a new subscription will be created and all messages published after the
	// creation will be retained until acknowledged, even if the consumer is not connected
	Subscribe(opts ...ConsumerOption) (Consumer, error)

	// Create a Reader instance.
	// This method will block until the reader is created successfully.
	CreateReader(ReaderOptions) (Reader, error)

	// Fetch the list of partitions for a given topic
	//
	// If the topic is partitioned, this will return a list of partition names.
	// If the topic is not partitioned, the returned list will contain the topic
	// name itself.
	//
	// This can be used to discover the partitions and create {@link Reader},
	// {@link Consumer} or {@link Producer} instances directly on a particular partition.
	TopicPartitions(topic string) ([]string, error)

	// Close the Client and free associated resources
	Close() error
}
