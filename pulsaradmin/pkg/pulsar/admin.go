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
	"net/http"
	"net/url"
	"path"

	"github.com/streamnative/pulsar-admin-go/pkg/auth"
	"github.com/streamnative/pulsar-admin-go/pkg/cli"
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common"
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/utils"
)

type TLSOptions struct {
	TrustCertsFilePath      string
	AllowInsecureConnection bool
}

// Client provides a client to the Pulsar Restful API
type Client interface {
	Clusters() Clusters
	Functions() Functions
	Tenants() Tenants
	Topics() Topics
	Subscriptions() Subscriptions
	Sources() Sources
	Sinks() Sinks
	Namespaces() Namespaces
	Schemas() Schema
	NsIsolationPolicy() NsIsolationPolicy
	Brokers() Brokers
	BrokerStats() BrokerStats
	ResourceQuotas() ResourceQuotas
	FunctionsWorker() FunctionsWorker
	Token() Token
	Packages() Packages
}

type pulsarClient struct {
	Client     *cli.Client
	APIVersion common.APIVersion
}

// New returns a new client
func New(config *common.Config) (Client, error) {
	authProvider, err := auth.GetAuthProvider(config)
	if err != nil {
		return nil, err
	}
	return NewPulsarClientWithAuthProvider(config, authProvider)
}

// NewWithAuthProvider creates a client with auth provider.
// Deprecated: Use NewPulsarClientWithAuthProvider instead.
func NewWithAuthProvider(config *common.Config, authProvider auth.Provider) Client {
	client, err := NewPulsarClientWithAuthProvider(config, authProvider)
	if err != nil {
		panic(err)
	}
	return client
}

// NewPulsarClientWithAuthProvider create a client with auth provider.
func NewPulsarClientWithAuthProvider(config *common.Config,
	authProvider auth.Provider) (Client, error) {
	var transport http.RoundTripper

	if authProvider != nil {
		transport = authProvider.Transport()
		if transport != nil {
			transport = authProvider
		}
	}

	if transport == nil {
		defaultTransport, err := auth.NewDefaultTransport(config)
		if err != nil {
			return nil, err
		}
		if authProvider != nil {
			authProvider.WithTransport(authProvider)
		} else {
			transport = defaultTransport
		}
	}

	webServiceURL := config.WebServiceURL
	if len(webServiceURL) == 0 {
		config.WebServiceURL = DefaultWebServiceURL
	}

	c := &pulsarClient{
		APIVersion: config.PulsarAPIVersion,
		Client: &cli.Client{
			ServiceURL:  config.WebServiceURL,
			VersionInfo: ReleaseVersion,
			HTTPClient: &http.Client{
				Timeout:   DefaultHTTPTimeOutDuration,
				Transport: transport,
			},
		},
	}

	return c, nil
}

func (c *pulsarClient) endpoint(componentPath string, parts ...string) string {
	escapedParts := make([]string, len(parts))
	for i, part := range parts {
		escapedParts[i] = url.PathEscape(part)
	}
	return path.Join(
		utils.MakeHTTPPath(c.APIVersion.String(), componentPath),
		path.Join(escapedParts...),
	)
}
