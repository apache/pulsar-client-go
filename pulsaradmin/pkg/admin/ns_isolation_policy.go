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
	"context"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

type NsIsolationPolicy interface {
	// CreateNamespaceIsolationPolicy creates a namespace isolation policy for a cluster
	CreateNamespaceIsolationPolicy(cluster, policyName string, namespaceIsolationData utils.NamespaceIsolationData) error

	// CreateNamespaceIsolationPolicyWithContext creates a namespace isolation policy for a cluster
	CreateNamespaceIsolationPolicyWithContext(
		ctx context.Context,
		cluster,
		policyName string,
		namespaceIsolationData utils.NamespaceIsolationData,
	) error

	// DeleteNamespaceIsolationPolicy deletes a namespace isolation policy for a cluster
	DeleteNamespaceIsolationPolicy(cluster, policyName string) error

	// DeleteNamespaceIsolationPolicyWithContext deletes a namespace isolation policy for a cluster
	DeleteNamespaceIsolationPolicyWithContext(ctx context.Context, cluster, policyName string) error

	// GetNamespaceIsolationPolicy returns a single namespace isolation policy for a cluster
	GetNamespaceIsolationPolicy(cluster, policyName string) (*utils.NamespaceIsolationData, error)

	// GetNamespaceIsolationPolicyWithContext returns a single namespace isolation policy for a cluster
	GetNamespaceIsolationPolicyWithContext(
		ctx context.Context,
		cluster,
		policyName string,
	) (*utils.NamespaceIsolationData, error)

	// GetNamespaceIsolationPolicies returns the namespace isolation policies of a cluster
	GetNamespaceIsolationPolicies(cluster string) (map[string]utils.NamespaceIsolationData, error)

	// GetNamespaceIsolationPoliciesWithContext returns the namespace isolation policies of a cluster
	GetNamespaceIsolationPoliciesWithContext(
		ctx context.Context,
		cluster string,
	) (map[string]utils.NamespaceIsolationData, error)

	// GetBrokersWithNamespaceIsolationPolicy returns list of active brokers
	// with namespace-isolation policies attached to it.
	GetBrokersWithNamespaceIsolationPolicy(cluster string) ([]utils.BrokerNamespaceIsolationData, error)

	// GetBrokersWithNamespaceIsolationPolicyWithContext returns list of active brokers
	// with namespace-isolation policies attached to it.
	GetBrokersWithNamespaceIsolationPolicyWithContext(
		ctx context.Context,
		cluster string,
	) ([]utils.BrokerNamespaceIsolationData, error)

	// GetBrokerWithNamespaceIsolationPolicy returns active broker with namespace-isolation policies attached to it.
	GetBrokerWithNamespaceIsolationPolicy(cluster, broker string) (*utils.BrokerNamespaceIsolationData, error)

	// GetBrokerWithNamespaceIsolationPolicyWithContext returns active broker
	// with namespace-isolation policies attached to it.
	GetBrokerWithNamespaceIsolationPolicyWithContext(
		ctx context.Context,
		cluster,
		broker string,
	) (*utils.BrokerNamespaceIsolationData, error)
}

type nsIsolationPolicy struct {
	pulsar   *pulsarClient
	basePath string
}

func (c *pulsarClient) NsIsolationPolicy() NsIsolationPolicy {
	return &nsIsolationPolicy{
		pulsar:   c,
		basePath: "/clusters",
	}
}

func (n *nsIsolationPolicy) CreateNamespaceIsolationPolicy(cluster, policyName string,
	namespaceIsolationData utils.NamespaceIsolationData) error {
	return n.CreateNamespaceIsolationPolicyWithContext(context.Background(), cluster, policyName, namespaceIsolationData)
}

func (n *nsIsolationPolicy) CreateNamespaceIsolationPolicyWithContext(ctx context.Context, cluster, policyName string,
	namespaceIsolationData utils.NamespaceIsolationData) error {
	return n.setNamespaceIsolationPolicy(ctx, cluster, policyName, namespaceIsolationData)
}

func (n *nsIsolationPolicy) setNamespaceIsolationPolicy(ctx context.Context, cluster, policyName string,
	namespaceIsolationData utils.NamespaceIsolationData) error {
	endpoint := n.pulsar.endpoint(n.basePath, cluster, "namespaceIsolationPolicies", policyName)
	return n.pulsar.Client.PostWithContext(ctx, endpoint, &namespaceIsolationData)
}

func (n *nsIsolationPolicy) DeleteNamespaceIsolationPolicy(cluster, policyName string) error {
	return n.DeleteNamespaceIsolationPolicyWithContext(context.Background(), cluster, policyName)
}

func (n *nsIsolationPolicy) DeleteNamespaceIsolationPolicyWithContext(
	ctx context.Context,
	cluster,
	policyName string,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, cluster, "namespaceIsolationPolicies", policyName)
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (n *nsIsolationPolicy) GetNamespaceIsolationPolicy(cluster, policyName string) (
	*utils.NamespaceIsolationData, error) {
	return n.GetNamespaceIsolationPolicyWithContext(context.Background(), cluster, policyName)
}

func (n *nsIsolationPolicy) GetNamespaceIsolationPolicyWithContext(ctx context.Context, cluster, policyName string) (
	*utils.NamespaceIsolationData, error) {
	endpoint := n.pulsar.endpoint(n.basePath, cluster, "namespaceIsolationPolicies", policyName)
	var nsIsolationData utils.NamespaceIsolationData
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &nsIsolationData)
	if err != nil {
		return nil, err
	}
	return &nsIsolationData, nil
}

func (n *nsIsolationPolicy) GetNamespaceIsolationPolicies(cluster string) (
	map[string]utils.NamespaceIsolationData, error) {
	return n.GetNamespaceIsolationPoliciesWithContext(context.Background(), cluster)
}

func (n *nsIsolationPolicy) GetNamespaceIsolationPoliciesWithContext(ctx context.Context, cluster string) (
	map[string]utils.NamespaceIsolationData, error) {
	endpoint := n.pulsar.endpoint(n.basePath, cluster, "namespaceIsolationPolicies")
	var tmpMap map[string]utils.NamespaceIsolationData
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &tmpMap)
	if err != nil {
		return nil, err
	}
	return tmpMap, nil
}

func (n *nsIsolationPolicy) GetBrokersWithNamespaceIsolationPolicy(cluster string) (
	[]utils.BrokerNamespaceIsolationData, error) {
	return n.GetBrokersWithNamespaceIsolationPolicyWithContext(context.Background(), cluster)
}

func (n *nsIsolationPolicy) GetBrokersWithNamespaceIsolationPolicyWithContext(ctx context.Context, cluster string) (
	[]utils.BrokerNamespaceIsolationData, error) {
	endpoint := n.pulsar.endpoint(n.basePath, cluster, "namespaceIsolationPolicies", "brokers")
	var res []utils.BrokerNamespaceIsolationData
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (n *nsIsolationPolicy) GetBrokerWithNamespaceIsolationPolicy(cluster,
	broker string) (*utils.BrokerNamespaceIsolationData, error) {
	return n.GetBrokerWithNamespaceIsolationPolicyWithContext(context.Background(), cluster, broker)
}

func (n *nsIsolationPolicy) GetBrokerWithNamespaceIsolationPolicyWithContext(ctx context.Context, cluster,
	broker string) (*utils.BrokerNamespaceIsolationData, error) {
	endpoint := n.pulsar.endpoint(n.basePath, cluster, "namespaceIsolationPolicies", "brokers", broker)
	var brokerNamespaceIsolationData utils.BrokerNamespaceIsolationData
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &brokerNamespaceIsolationData)
	if err != nil {
		return nil, err
	}
	return &brokerNamespaceIsolationData, nil
}
