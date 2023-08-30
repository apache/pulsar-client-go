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

package pulsaradmin

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin/internal/rest"
)

const (
	DefaultWebServiceURL       = "http://localhost:8080"
	DefaultBKWebServiceURL     = "pulsar://localhost:6650"
	DefaultHTTPTimeOutDuration = 5 * time.Minute
	Product                    = "pulsar-admin-go"
	ReleaseVersion             = "None"
	adminBasePath              = `/admin`
)

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
	restClient *rest.Client
	apiProfile APIProfile
}

// NewClient returns a new client
func NewClient(config ClientConfig) (Client, error) {
	if config.WebServiceURL == "" {
		config.WebServiceURL = DefaultWebServiceURL
	}
	if config.BKWebServiceURL == "" {
		config.BKWebServiceURL = DefaultBKWebServiceURL
	}
	if config.APIProfile == nil {
		config.APIProfile = defaultAPIProfile()
	}

	baseTransport := config.CustomTransport
	if baseTransport == nil {
		defaultTransport, err := defaultTransport(config)
		if err != nil {
			return nil, fmt.Errorf("initializing default transport: %w", err)
		}
		baseTransport = defaultTransport
	}

	clientTransport := http.RoundTripper(baseTransport)

	if config.AuthProvider != nil {
		authTransport, err := config.AuthProvider(baseTransport)
		if err != nil {
			return nil, fmt.Errorf("auth provider: %w", err)
		}
		if authTransport != nil {
			clientTransport = authTransport
		}
	}

	return &pulsarClient{
		restClient: rest.NewClient(clientTransport, config.WebServiceURL, Product+`/`+ReleaseVersion),
		apiProfile: *config.APIProfile,
	}, nil
}

func (c *pulsarClient) endpoint(apiVersion APIVersion, componentPath string, parts ...string) string {
	escapedParts := make([]string, len(parts))
	for i, part := range parts {
		escapedParts[i] = url.PathEscape(part)
	}
	return path.Join(
		adminBasePath,
		apiVersion.String(),
		componentPath,
		path.Join(escapedParts...),
	)
}
