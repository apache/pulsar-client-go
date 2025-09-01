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

package admin

import (
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

// Proxy is admin interface for proxy management
type Proxy interface {
	// GetProxies returns the list of active proxies in the cluster
	GetProxies() ([]string, error)

	// GetProxyStats returns the stats of all active proxies in the cluster
	GetProxyStats() ([]utils.ProxyStats, error)

	// GetConnectionsStats returns connection stats for all active proxies
	GetConnectionsStats() ([]utils.ConnectionStats, error)

	// GetTopicsStats returns topic stats for all active proxies
	GetTopicsStats() ([]utils.ProxyTopicStats, error)
}

type proxy struct {
	pulsar   *pulsarClient
	basePath string
}

// Proxy is used to access the proxy endpoints
func (c *pulsarClient) Proxy() Proxy {
	return &proxy{
		pulsar:   c,
		basePath: "/proxy-stats",
	}
}

func (p *proxy) GetProxies() ([]string, error) {
	var proxies []string
	endpoint := p.pulsar.endpoint(p.basePath, "proxies")
	err := p.pulsar.Client.Get(endpoint, &proxies)
	return proxies, err
}

func (p *proxy) GetProxyStats() ([]utils.ProxyStats, error) {
	var stats []utils.ProxyStats
	endpoint := p.pulsar.endpoint(p.basePath, "stats")
	err := p.pulsar.Client.Get(endpoint, &stats)
	return stats, err
}

func (p *proxy) GetConnectionsStats() ([]utils.ConnectionStats, error) {
	var stats []utils.ConnectionStats
	endpoint := p.pulsar.endpoint(p.basePath, "connections")
	err := p.pulsar.Client.Get(endpoint, &stats)
	return stats, err
}

func (p *proxy) GetTopicsStats() ([]utils.ProxyTopicStats, error) {
	var stats []utils.ProxyTopicStats
	endpoint := p.pulsar.endpoint(p.basePath, "topics")
	err := p.pulsar.Client.Get(endpoint, &stats)
	return stats, err
}