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

type NsIsolationPolicy interface {
	// Create a namespace isolation policy for a cluster
	CreateNamespaceIsolationPolicy(cluster, policyName string, namespaceIsolationData NamespaceIsolationData) error

	// Delete a namespace isolation policy for a cluster
	DeleteNamespaceIsolationPolicy(cluster, policyName string) error

	// Get a single namespace isolation policy for a cluster
	GetNamespaceIsolationPolicy(cluster, policyName string) (*NamespaceIsolationData, error)

	// Get the namespace isolation policies of a cluster
	GetNamespaceIsolationPolicies(cluster string) (map[string]NamespaceIsolationData, error)

	// Returns list of active brokers with namespace-isolation policies attached to it.
	GetBrokersWithNamespaceIsolationPolicy(cluster string) ([]BrokerNamespaceIsolationData, error)

	// Returns active broker with namespace-isolation policies attached to it.
	GetBrokerWithNamespaceIsolationPolicy(cluster, broker string) (*BrokerNamespaceIsolationData, error)
}

type nsIsolationPolicy struct {
	pulsar     *pulsarClient
	basePath   string
	apiVersion APIVersion
}

func (c *pulsarClient) NsIsolationPolicy() NsIsolationPolicy {
	return &nsIsolationPolicy{
		pulsar:     c,
		basePath:   "/clusters",
		apiVersion: c.apiProfile.NsIsolationPolicy,
	}
}

func (n *nsIsolationPolicy) CreateNamespaceIsolationPolicy(cluster, policyName string,
	namespaceIsolationData NamespaceIsolationData,
) error {
	return n.setNamespaceIsolationPolicy(cluster, policyName, namespaceIsolationData)
}

func (n *nsIsolationPolicy) setNamespaceIsolationPolicy(cluster, policyName string,
	namespaceIsolationData NamespaceIsolationData,
) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, cluster, "namespaceIsolationPolicies", policyName)
	return n.pulsar.restClient.Post(endpoint, &namespaceIsolationData)
}

func (n *nsIsolationPolicy) DeleteNamespaceIsolationPolicy(cluster, policyName string) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, cluster, "namespaceIsolationPolicies", policyName)
	return n.pulsar.restClient.Delete(endpoint)
}

func (n *nsIsolationPolicy) GetNamespaceIsolationPolicy(cluster, policyName string) (
	*NamespaceIsolationData, error,
) {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, cluster, "namespaceIsolationPolicies", policyName)
	var nsIsolationData NamespaceIsolationData
	err := n.pulsar.restClient.Get(endpoint, &nsIsolationData)
	if err != nil {
		return nil, err
	}
	return &nsIsolationData, nil
}

func (n *nsIsolationPolicy) GetNamespaceIsolationPolicies(cluster string) (
	map[string]NamespaceIsolationData, error,
) {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, cluster, "namespaceIsolationPolicies")
	var tmpMap map[string]NamespaceIsolationData
	err := n.pulsar.restClient.Get(endpoint, &tmpMap)
	if err != nil {
		return nil, err
	}
	return tmpMap, nil
}

func (n *nsIsolationPolicy) GetBrokersWithNamespaceIsolationPolicy(cluster string) (
	[]BrokerNamespaceIsolationData, error,
) {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, cluster, "namespaceIsolationPolicies", "brokers")
	var res []BrokerNamespaceIsolationData
	err := n.pulsar.restClient.Get(endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (n *nsIsolationPolicy) GetBrokerWithNamespaceIsolationPolicy(cluster,
	broker string,
) (*BrokerNamespaceIsolationData, error) {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, cluster, "namespaceIsolationPolicies", "brokers", broker)
	var brokerNamespaceIsolationData BrokerNamespaceIsolationData
	err := n.pulsar.restClient.Get(endpoint, &brokerNamespaceIsolationData)
	if err != nil {
		return nil, err
	}
	return &brokerNamespaceIsolationData, nil
}
