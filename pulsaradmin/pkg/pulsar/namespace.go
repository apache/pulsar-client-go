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
	"strconv"
)

type Namespaces interface {
	// Get the list of all the namespaces for a certain tenant
	GetNamespaces(tenant string) ([]string, error)

	// Get the list of all the topics under a certain namespace
	GetTopics(namespace string) ([]string, error)

	// Get the dump all the policies specified for a namespace
	GetPolicies(namespace string) (*Policies, error)

	// Creates a new empty namespace with no policies attached
	CreateNamespace(namespace string) error

	// Creates a new empty namespace with no policies attached
	CreateNsWithNumBundles(namespace string, numBundles int) error

	// Creates a new namespace with the specified policies
	CreateNsWithPolices(namespace string, polices Policies) error

	// Creates a new empty namespace with no policies attached
	CreateNsWithBundlesData(namespace string, bundleData *BundlesData) error

	// Delete an existing namespace
	DeleteNamespace(namespace string) error

	// Delete an existing bundle in a namespace
	DeleteNamespaceBundle(namespace string, bundleRange string) error

	// Set the messages Time to Live for all the topics within a namespace
	SetNamespaceMessageTTL(namespace string, ttlInSeconds int) error

	// Get the message TTL for a namespace
	GetNamespaceMessageTTL(namespace string) (int, error)

	// Get the retention configuration for a namespace
	GetRetention(namespace string) (*RetentionPolicies, error)

	// Set the retention configuration for all the topics on a namespace
	SetRetention(namespace string, policy RetentionPolicies) error

	// Get backlog quota map on a namespace
	GetBacklogQuotaMap(namespace string) (map[BacklogQuotaType]BacklogQuota, error)

	// Set a backlog quota for all the topics on a namespace
	SetBacklogQuota(namespace string, backlogQuota BacklogQuota) error

	// Remove a backlog quota policy from a namespace
	RemoveBacklogQuota(namespace string) error

	// Get the replication clusters for a namespace
	GetNamespaceReplicationClusters(namespace string) ([]string, error)

	// Set the replication clusters for a namespace
	SetNamespaceReplicationClusters(namespace string, clusterIds []string) error

	// Set anti-affinity group name for a namespace
	SetNamespaceAntiAffinityGroup(namespace string, namespaceAntiAffinityGroup string) error

	// Get all namespaces that grouped with given anti-affinity group
	GetAntiAffinityNamespaces(tenant, cluster, namespaceAntiAffinityGroup string) ([]string, error)

	// Get anti-affinity group name for a namespace
	GetNamespaceAntiAffinityGroup(namespace string) (string, error)

	// Delete anti-affinity group name for a namespace
	DeleteNamespaceAntiAffinityGroup(namespace string) error

	// Set the deduplication status for all topics within a namespace
	// When deduplication is enabled, the broker will prevent to store the same message multiple times
	SetDeduplicationStatus(namespace string, enableDeduplication bool) error

	// Set the persistence configuration for all the topics on a namespace
	SetPersistence(namespace string, persistence PersistencePolicies) error

	// Get the persistence configuration for a namespace
	GetPersistence(namespace string) (*PersistencePolicies, error)

	// Set bookie affinity group for a namespace to isolate namespace write to bookies that are
	// part of given affinity group
	SetBookieAffinityGroup(namespace string, bookieAffinityGroup BookieAffinityGroupData) error

	// Delete bookie affinity group configured for a namespace
	DeleteBookieAffinityGroup(namespace string) error

	// Get bookie affinity group configured for a namespace
	GetBookieAffinityGroup(namespace string) (*BookieAffinityGroupData, error)

	// Unload a namespace from the current serving broker
	Unload(namespace string) error

	// Unload namespace bundle
	UnloadNamespaceBundle(namespace, bundle string) error

	// Split namespace bundle
	SplitNamespaceBundle(namespace, bundle string, unloadSplitBundles bool) error
}

type namespaces struct {
	client   *client
	basePath string
}

func (c *client) Namespaces() Namespaces {
	return &namespaces{
		client:   c,
		basePath: "/namespaces",
	}
}

func (n *namespaces) GetNamespaces(tenant string) ([]string, error) {
	var namespaces []string
	endpoint := n.client.endpoint(n.basePath, tenant)
	err := n.client.get(endpoint, &namespaces)
	return namespaces, err
}

func (n *namespaces) GetTopics(namespace string) ([]string, error) {
	var topics []string
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String(), "topics")
	err = n.client.get(endpoint, &topics)
	return topics, err
}

func (n *namespaces) GetPolicies(namespace string) (*Policies, error) {
	var police Policies
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String())
	err = n.client.get(endpoint, &police)
	return &police, err
}

func (n *namespaces) CreateNsWithNumBundles(namespace string, numBundles int) error {
	return n.CreateNsWithBundlesData(namespace, NewBundlesDataWithNumBundles(numBundles))
}

func (n *namespaces) CreateNsWithPolices(namespace string, policies Policies) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String())
	return n.client.put(endpoint, &policies, nil)
}

func (n *namespaces) CreateNsWithBundlesData(namespace string, bundleData *BundlesData) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String())
	polices := new(Policies)
	polices.Bundles = bundleData

	return n.client.put(endpoint, &polices, nil)
}

func (n *namespaces) CreateNamespace(namespace string) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String())
	return n.client.put(endpoint, nil, nil)
}

func (n *namespaces) DeleteNamespace(namespace string) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String())
	return n.client.delete(endpoint, nil)
}

func (n *namespaces) DeleteNamespaceBundle(namespace string, bundleRange string) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String(), bundleRange)
	return n.client.delete(endpoint, nil)
}

func (n *namespaces) GetNamespaceMessageTTL(namespace string) (int, error) {
	var ttl int
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return 0, err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "messageTTL")
	err = n.client.get(endpoint, &ttl)
	return ttl, err
}

func (n *namespaces) SetNamespaceMessageTTL(namespace string, ttlInSeconds int) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}

	endpoint := n.client.endpoint(n.basePath, nsName.String(), "messageTTL")
	return n.client.post(endpoint, &ttlInSeconds, nil)
}

func (n *namespaces) SetRetention(namespace string, policy RetentionPolicies) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "retention")
	return n.client.post(endpoint, &policy, nil)
}

func (n *namespaces) GetRetention(namespace string) (*RetentionPolicies, error) {
	var policy RetentionPolicies
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "retention")
	err = n.client.get(endpoint, &policy)
	return &policy, err
}

func (n *namespaces) GetBacklogQuotaMap(namespace string) (map[BacklogQuotaType]BacklogQuota, error) {
	var backlogQuotaMap map[BacklogQuotaType]BacklogQuota
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "backlogQuotaMap")
	err = n.client.get(endpoint, &backlogQuotaMap)
	return backlogQuotaMap, err
}

func (n *namespaces) SetBacklogQuota(namespace string, backlogQuota BacklogQuota) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "backlogQuota")
	return n.client.post(endpoint, &backlogQuota, nil)
}

func (n *namespaces) RemoveBacklogQuota(namespace string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "backlogQuota")
	params := map[string]string{
		"backlogQuotaType": string(DestinationStorage),
	}
	return n.client.deleteWithQueryParams(endpoint, nil, params)
}

func (n *namespaces) GetNamespaceReplicationClusters(namespace string) ([]string, error) {
	var data []string
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "replication")
	err = n.client.get(endpoint, &data)
	return data, err
}

func (n *namespaces) SetNamespaceReplicationClusters(namespace string, clusterIds []string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "replication")
	return n.client.post(endpoint, &clusterIds, nil)
}

func (n *namespaces) SetNamespaceAntiAffinityGroup(namespace string, namespaceAntiAffinityGroup string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "antiAffinity")
	return n.client.post(endpoint, namespaceAntiAffinityGroup, nil)
}

func (n *namespaces) GetAntiAffinityNamespaces(tenant, cluster, namespaceAntiAffinityGroup string) ([]string, error) {
	var data []string
	endpoint := n.client.endpoint(n.basePath, cluster, "antiAffinity", namespaceAntiAffinityGroup)
	params := map[string]string{
		"property": tenant,
	}
	_, err := n.client.getWithQueryParams(endpoint, &data, params, false)
	return data, err
}

func (n *namespaces) GetNamespaceAntiAffinityGroup(namespace string) (string, error) {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return "", err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "antiAffinity")
	data, err := n.client.getWithQueryParams(endpoint, nil, nil, false)
	return string(data), err
}

func (n *namespaces) DeleteNamespaceAntiAffinityGroup(namespace string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "antiAffinity")
	return n.client.delete(endpoint, nil)
}

func (n *namespaces) SetDeduplicationStatus(namespace string, enableDeduplication bool) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "deduplication")
	return n.client.post(endpoint, enableDeduplication, nil)
}

func (n *namespaces) SetPersistence(namespace string, persistence PersistencePolicies) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "persistence")
	return n.client.post(endpoint, &persistence, nil)
}

func (n *namespaces) SetBookieAffinityGroup(namespace string, bookieAffinityGroup BookieAffinityGroupData) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "persistence", "bookieAffinity")
	return n.client.post(endpoint, &bookieAffinityGroup, nil)
}

func (n *namespaces) DeleteBookieAffinityGroup(namespace string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "persistence", "bookieAffinity")
	return n.client.delete(endpoint, nil)
}

func (n *namespaces) GetBookieAffinityGroup(namespace string) (*BookieAffinityGroupData, error) {
	var data BookieAffinityGroupData
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "persistence", "bookieAffinity")
	err = n.client.get(endpoint, &data)
	return &data, err
}

func (n *namespaces) GetPersistence(namespace string) (*PersistencePolicies, error) {
	var persistence PersistencePolicies
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "persistence")
	err = n.client.get(endpoint, &persistence)
	return &persistence, err
}

func (n *namespaces) Unload(namespace string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), "unload")
	return n.client.put(endpoint, "", nil)
}

func (n *namespaces) UnloadNamespaceBundle(namespace, bundle string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), bundle, "unload")
	return n.client.put(endpoint, "", nil)
}

func (n *namespaces) SplitNamespaceBundle(namespace, bundle string, unloadSplitBundles bool) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, nsName.String(), bundle, "split")
	params := map[string]string{
		"unload": strconv.FormatBool(unloadSplitBundles),
	}
	return n.client.putWithQueryParams(endpoint, "", nil, params)
}
