// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package admin

import (
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/auth"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

const (
	DefaultWebServiceURL       = "http://localhost:8080"
	DefaultHTTPTimeOutDuration = 5 * time.Minute
	ReleaseVersion             = "None"
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
	Packages() Packages
}

type pulsarClient struct {
	Client     *rest.Client
	APIVersion config.APIVersion
}

// New returns a new client
func New(config *config.Config) (Client, error) {
	authProvider, err := auth.GetAuthProvider(config)
	if err != nil {
		return nil, err
	}
	return NewPulsarClientWithAuthProvider(config, authProvider)
}

// NewWithAuthProvider creates a client with auth provider.
// Deprecated: Use NewPulsarClientWithAuthProvider instead.
func NewWithAuthProvider(config *config.Config, authProvider auth.Provider) Client {
	client, err := NewPulsarClientWithAuthProvider(config, authProvider)
	if err != nil {
		panic(err)
	}
	return client
}

// NewPulsarClientWithAuthProvider create a client with auth provider.
func NewPulsarClientWithAuthProvider(config *config.Config, authProvider auth.Provider) (Client, error) {
	if len(config.WebServiceURL) == 0 {
		config.WebServiceURL = DefaultWebServiceURL
	}

	return &pulsarClient{
		APIVersion: config.PulsarAPIVersion,
		Client: &rest.Client{
			ServiceURL:  config.WebServiceURL,
			VersionInfo: ReleaseVersion,
			HTTPClient: &http.Client{
				Timeout:   DefaultHTTPTimeOutDuration,
				Transport: authProvider,
			},
		},
	}, nil
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
