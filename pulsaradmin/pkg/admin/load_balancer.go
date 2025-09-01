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

// LoadBalancer is admin interface for load balancer management
type LoadBalancer interface {
	// GetLoadBalancerBrokerRanking returns the broker ranking
	GetLoadBalancerBrokerRanking() (map[string]interface{}, error)

	// GetBundleUnloadingMetrics returns bundle unloading metrics
	GetBundleUnloadingMetrics() (*utils.BundleUnloadingMetrics, error)

	// GetLeaderBroker returns the leader broker for load balancing
	GetLeaderBroker() (*utils.BrokerInfo, error)

	// UpdateLoadManagerLeader updates the load manager leader
	UpdateLoadManagerLeader() error
}

type loadBalancer struct {
	pulsar   *pulsarClient
	basePath string
}

// LoadBalancer is used to access the load balancer endpoints
func (c *pulsarClient) LoadBalancer() LoadBalancer {
	return &loadBalancer{
		pulsar:   c,
		basePath: "/load-manager",
	}
}

func (lb *loadBalancer) GetLoadBalancerBrokerRanking() (map[string]interface{}, error) {
	var ranking map[string]interface{}
	endpoint := lb.pulsar.endpoint(lb.basePath, "brokerRanking")
	err := lb.pulsar.Client.Get(endpoint, &ranking)
	return ranking, err
}

func (lb *loadBalancer) GetBundleUnloadingMetrics() (*utils.BundleUnloadingMetrics, error) {
	var metrics utils.BundleUnloadingMetrics
	endpoint := lb.pulsar.endpoint(lb.basePath, "bundle-unloading")
	err := lb.pulsar.Client.Get(endpoint, &metrics)
	return &metrics, err
}

func (lb *loadBalancer) GetLeaderBroker() (*utils.BrokerInfo, error) {
	var broker utils.BrokerInfo
	endpoint := lb.pulsar.endpoint(lb.basePath, "leader")
	err := lb.pulsar.Client.Get(endpoint, &broker)
	return &broker, err
}

func (lb *loadBalancer) UpdateLoadManagerLeader() error {
	endpoint := lb.pulsar.endpoint(lb.basePath, "leader")
	return lb.pulsar.Client.Post(endpoint, nil)
}