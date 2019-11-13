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
	"path"
	"strings"

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
}

type pulsarClient struct {
	Client     *cli.Client
	APIVersion common.APIVersion
}

// New returns a new client
func New(config *Config) (Client, error) {
	if len(config.WebServiceURL) == 0 {
		config.WebServiceURL = DefaultWebServiceURL
	}

	c := &pulsarClient{
		APIVersion: config.APIVersion,
		Client: &cli.Client{
			ServiceURL:  config.WebServiceURL,
			VersionInfo: ReleaseVersion,
			HTTPClient: &http.Client{
				Timeout: DefaultHTTPTimeOutDuration,
			},
		},
	}

	err := c.initAuth(config)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *pulsarClient) initAuth(config *Config) error {
	if strings.HasPrefix(config.WebServiceURL, "https") {
		err := c.initTLS(config)
		if err != nil {
			return err
		}
	}

	if config.TokenFile != "" || config.Token != "" {
		c.initToken(config)
	}

	return nil
}

func (c *pulsarClient) initTLS(config *Config) error {
	tlsAuth := auth.NewAuthenticationTLS(config.TLSCertFile, config.TLSKeyFile, config.TLSAllowInsecureConnection)
	tlsConf, err := tlsAuth.GetTLSConfig(config.TLSCertFile, config.TLSAllowInsecureConnection)
	if err != nil {
		return err
	}

	c.Client.HTTPClient.Transport = &http.Transport{
		MaxIdleConnsPerHost: 10,
		TLSClientConfig:     tlsConf,
	}

	return nil
}

func (c *pulsarClient) initToken(config *Config) {
	if config.Token != "" {
		c.Client.AuthProvider = auth.NewAuthenticationToken(config.Token)
	}

	if config.TokenFile != "" {
		c.Client.AuthProvider = auth.NewAuthenticationTokenFromFile(config.TokenFile)
	}
}

func (c *pulsarClient) endpoint(componentPath string, parts ...string) string {
	return path.Join(utils.MakeHTTPPath(c.APIVersion.String(), componentPath), path.Join(parts...))
}
