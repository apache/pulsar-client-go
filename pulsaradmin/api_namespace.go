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
	"net/url"
	"strconv"
	"strings"
)

// Namespaces is admin interface for namespaces management
type Namespaces interface {
	// GetNamespaces returns the list of all the namespaces for a certain tenant
	GetNamespaces(tenant string) ([]string, error)

	// GetTopics returns the list of all the topics under a certain namespace
	GetTopics(namespace string) ([]string, error)

	// GetPolicies returns the dump all the policies specified for a namespace
	GetPolicies(namespace string) (*Policies, error)

	// CreateNamespace creates a new empty namespace with no policies attached
	CreateNamespace(namespace string) error

	// CreateNsWithNumBundles creates a new empty namespace with no policies attached
	CreateNsWithNumBundles(namespace string, numBundles int) error

	// CreateNsWithPolices creates a new namespace with the specified policies
	CreateNsWithPolices(namespace string, polices Policies) error

	// CreateNsWithBundlesData creates a new empty namespace with no policies attached
	CreateNsWithBundlesData(namespace string, bundleData *BundlesData) error

	// DeleteNamespace deletes an existing namespace
	DeleteNamespace(namespace string) error

	// DeleteNamespaceBundle deletes an existing bundle in a namespace
	DeleteNamespaceBundle(namespace string, bundleRange string) error

	// SetNamespaceMessageTTL sets the messages Time to Live for all the topics within a namespace
	SetNamespaceMessageTTL(namespace string, ttlInSeconds int) error

	// GetNamespaceMessageTTL returns the message TTL for a namespace
	GetNamespaceMessageTTL(namespace string) (int, error)

	// GetRetention returns the retention configuration for a namespace
	GetRetention(namespace string) (*RetentionPolicies, error)

	// SetRetention sets the retention configuration for all the topics on a namespace
	SetRetention(namespace string, policy RetentionPolicies) error

	// GetBacklogQuotaMap returns backlog quota map on a namespace
	GetBacklogQuotaMap(namespace string) (map[BacklogQuotaType]BacklogQuota, error)

	// SetBacklogQuota sets a backlog quota for all the topics on a namespace
	SetBacklogQuota(namespace string, backlogQuota BacklogQuota, backlogQuotaType BacklogQuotaType) error

	// RemoveBacklogQuota removes a backlog quota policy from a namespace
	RemoveBacklogQuota(namespace string) error

	// SetTopicAutoCreation sets topic auto-creation config for a namespace, overriding broker settings
	SetTopicAutoCreation(namespace NameSpaceName, config TopicAutoCreationConfig) error

	// RemoveTopicAutoCreation removes topic auto-creation config for a namespace, defaulting to broker settings
	RemoveTopicAutoCreation(namespace NameSpaceName) error

	// SetSchemaValidationEnforced sets schema validation enforced for namespace
	SetSchemaValidationEnforced(namespace NameSpaceName, schemaValidationEnforced bool) error

	// GetSchemaValidationEnforced returns schema validation enforced for namespace
	GetSchemaValidationEnforced(namespace NameSpaceName) (bool, error)

	// SetSchemaAutoUpdateCompatibilityStrategy sets the strategy used to check the a new schema provided
	// by a producer is compatible with the current schema before it is installed
	SetSchemaAutoUpdateCompatibilityStrategy(namespace NameSpaceName,
		strategy SchemaCompatibilityStrategy) error

	// GetSchemaAutoUpdateCompatibilityStrategy returns the strategy used to check the a new schema provided
	// by a producer is compatible with the current schema before it is installed
	GetSchemaAutoUpdateCompatibilityStrategy(namespace NameSpaceName) (SchemaCompatibilityStrategy, error)

	// ClearOffloadDeleteLag clears the offload deletion lag for a namespace.
	ClearOffloadDeleteLag(namespace NameSpaceName) error

	// SetOffloadDeleteLag sets the offload deletion lag for a namespace
	SetOffloadDeleteLag(namespace NameSpaceName, timeMs int64) error

	// GetOffloadDeleteLag returns the offload deletion lag for a namespace, in milliseconds
	GetOffloadDeleteLag(namespace NameSpaceName) (int64, error)

	// SetOffloadThreshold sets the offloadThreshold for a namespace
	SetOffloadThreshold(namespace NameSpaceName, threshold int64) error

	// GetOffloadThreshold returns the offloadThreshold for a namespace
	GetOffloadThreshold(namespace NameSpaceName) (int64, error)

	// SetCompactionThreshold sets the compactionThreshold for a namespace
	SetCompactionThreshold(namespace NameSpaceName, threshold int64) error

	// GetCompactionThreshold returns the compactionThreshold for a namespace
	GetCompactionThreshold(namespace NameSpaceName) (int64, error)

	// SetMaxConsumersPerSubscription sets maxConsumersPerSubscription for a namespace.
	SetMaxConsumersPerSubscription(namespace NameSpaceName, max int) error

	// GetMaxConsumersPerSubscription returns the maxConsumersPerSubscription for a namespace.
	GetMaxConsumersPerSubscription(namespace NameSpaceName) (int, error)

	// SetMaxConsumersPerTopic sets maxConsumersPerTopic for a namespace.
	SetMaxConsumersPerTopic(namespace NameSpaceName, max int) error

	// GetMaxConsumersPerTopic returns the maxProducersPerTopic for a namespace.
	GetMaxConsumersPerTopic(namespace NameSpaceName) (int, error)

	// SetMaxProducersPerTopic sets maxProducersPerTopic for a namespace.
	SetMaxProducersPerTopic(namespace NameSpaceName, max int) error

	// GetMaxProducersPerTopic returns the maxProducersPerTopic for a namespace.
	GetMaxProducersPerTopic(namespace NameSpaceName) (int, error)

	// GetNamespaceReplicationClusters returns the replication clusters for a namespace
	GetNamespaceReplicationClusters(namespace string) ([]string, error)

	// SetNamespaceReplicationClusters returns the replication clusters for a namespace
	SetNamespaceReplicationClusters(namespace string, clusterIds []string) error

	// SetNamespaceAntiAffinityGroup sets anti-affinity group name for a namespace
	SetNamespaceAntiAffinityGroup(namespace string, namespaceAntiAffinityGroup string) error

	// GetAntiAffinityNamespaces returns all namespaces that grouped with given anti-affinity group
	GetAntiAffinityNamespaces(tenant, cluster, namespaceAntiAffinityGroup string) ([]string, error)

	// GetNamespaceAntiAffinityGroup returns anti-affinity group name for a namespace
	GetNamespaceAntiAffinityGroup(namespace string) (string, error)

	// DeleteNamespaceAntiAffinityGroup deletes anti-affinity group name for a namespace
	DeleteNamespaceAntiAffinityGroup(namespace string) error

	// SetDeduplicationStatus sets the deduplication status for all topics within a namespace
	// When deduplication is enabled, the broker will prevent to store the same Message multiple times
	SetDeduplicationStatus(namespace string, enableDeduplication bool) error

	// SetPersistence sets the persistence configuration for all the topics on a namespace
	SetPersistence(namespace string, persistence PersistencePolicies) error

	// GetPersistence returns the persistence configuration for a namespace
	GetPersistence(namespace string) (*PersistencePolicies, error)

	// SetBookieAffinityGroup sets bookie affinity group for a namespace to isolate namespace write to bookies that are
	// part of given affinity group
	SetBookieAffinityGroup(namespace string, bookieAffinityGroup BookieAffinityGroupData) error

	// DeleteBookieAffinityGroup deletes bookie affinity group configured for a namespace
	DeleteBookieAffinityGroup(namespace string) error

	// GetBookieAffinityGroup returns bookie affinity group configured for a namespace
	GetBookieAffinityGroup(namespace string) (*BookieAffinityGroupData, error)

	// Unload a namespace from the current serving broker
	Unload(namespace string) error

	// UnloadNamespaceBundle unloads namespace bundle
	UnloadNamespaceBundle(namespace, bundle string) error

	// SplitNamespaceBundle splits namespace bundle
	SplitNamespaceBundle(namespace, bundle string, unloadSplitBundles bool) error

	// GetNamespacePermissions returns permissions on a namespace
	GetNamespacePermissions(namespace NameSpaceName) (map[string][]AuthAction, error)

	// GrantNamespacePermission grants permission on a namespace.
	GrantNamespacePermission(namespace NameSpaceName, role string, action []AuthAction) error

	// RevokeNamespacePermission revokes permissions on a namespace.
	RevokeNamespacePermission(namespace NameSpaceName, role string) error

	// GrantSubPermission grants permission to role to access subscription's admin-api
	GrantSubPermission(namespace NameSpaceName, sName string, roles []string) error

	// RevokeSubPermission revoke permissions on a subscription's admin-api access
	RevokeSubPermission(namespace NameSpaceName, sName, role string) error

	// SetSubscriptionAuthMode sets the given subscription auth mode on all topics on a namespace
	SetSubscriptionAuthMode(namespace NameSpaceName, mode SubscriptionAuthMode) error

	// SetEncryptionRequiredStatus sets the encryption required status for all topics within a namespace
	SetEncryptionRequiredStatus(namespace NameSpaceName, encrypt bool) error

	// UnsubscribeNamespace unsubscribe the given subscription on all topics on a namespace
	UnsubscribeNamespace(namespace NameSpaceName, sName string) error

	// UnsubscribeNamespaceBundle unsubscribe the given subscription on all topics on a namespace bundle
	UnsubscribeNamespaceBundle(namespace NameSpaceName, bundle, sName string) error

	// ClearNamespaceBundleBacklogForSubscription clears backlog for a given subscription on all
	// topics on a namespace bundle
	ClearNamespaceBundleBacklogForSubscription(namespace NameSpaceName, bundle, sName string) error

	// ClearNamespaceBundleBacklog clears backlog for all topics on a namespace bundle
	ClearNamespaceBundleBacklog(namespace NameSpaceName, bundle string) error

	// ClearNamespaceBacklogForSubscription clears backlog for a given subscription on all topics on a namespace
	ClearNamespaceBacklogForSubscription(namespace NameSpaceName, sName string) error

	// ClearNamespaceBacklog clears backlog for all topics on a namespace
	ClearNamespaceBacklog(namespace NameSpaceName) error

	// SetReplicatorDispatchRate sets replicator-Message-dispatch-rate (Replicators under this namespace
	// can dispatch this many messages per second)
	SetReplicatorDispatchRate(namespace NameSpaceName, rate DispatchRate) error

	// Get replicator-Message-dispatch-rate (Replicators under this namespace
	// can dispatch this many messages per second)
	GetReplicatorDispatchRate(namespace NameSpaceName) (DispatchRate, error)

	// SetSubscriptionDispatchRate sets subscription-Message-dispatch-rate (subscriptions under this namespace
	// can dispatch this many messages per second)
	SetSubscriptionDispatchRate(namespace NameSpaceName, rate DispatchRate) error

	// GetSubscriptionDispatchRate returns subscription-Message-dispatch-rate (subscriptions under this namespace
	// can dispatch this many messages per second)
	GetSubscriptionDispatchRate(namespace NameSpaceName) (DispatchRate, error)

	// SetSubscribeRate sets namespace-subscribe-rate (topics under this namespace will limit by subscribeRate)
	SetSubscribeRate(namespace NameSpaceName, rate SubscribeRate) error

	// GetSubscribeRate returns namespace-subscribe-rate (topics under this namespace allow subscribe
	// times per consumer in a period)
	GetSubscribeRate(namespace NameSpaceName) (SubscribeRate, error)

	// SetDispatchRate sets Message-dispatch-rate (topics under this namespace can dispatch
	// this many messages per second)
	SetDispatchRate(namespace NameSpaceName, rate DispatchRate) error

	// GetDispatchRate returns Message-dispatch-rate (topics under this namespace can dispatch
	// this many messages per second)
	GetDispatchRate(namespace NameSpaceName) (DispatchRate, error)

	// SetPublishRate sets the maximum rate or number of messages that producers can publish to topics in this namespace
	SetPublishRate(namespace NameSpaceName, pubRate PublishRate) error

	// GetPublishRate gets the maximum rate or number of messages that producer can publish to topics in the namespace
	GetPublishRate(namespace NameSpaceName) (PublishRate, error)

	// SetIsAllowAutoUpdateSchema sets whether to allow auto update schema on a namespace
	SetIsAllowAutoUpdateSchema(namespace NameSpaceName, isAllowAutoUpdateSchema bool) error

	// GetIsAllowAutoUpdateSchema gets whether to allow auto update schema on a namespace
	GetIsAllowAutoUpdateSchema(namespace NameSpaceName) (bool, error)

	// GetInactiveTopicPolicies gets the inactive topic policies on a namespace
	GetInactiveTopicPolicies(namespace NameSpaceName) (InactiveTopicPolicies, error)

	// RemoveInactiveTopicPolicies removes inactive topic policies from a namespace
	RemoveInactiveTopicPolicies(namespace NameSpaceName) error

	// SetInactiveTopicPolicies sets the inactive topic policies on a namespace
	SetInactiveTopicPolicies(namespace NameSpaceName, data InactiveTopicPolicies) error
}

type namespaces struct {
	pulsar     *pulsarClient
	basePath   string
	apiVersion APIVersion
}

// Namespaces is used to access the namespaces endpoints
func (c *pulsarClient) Namespaces() Namespaces {
	return &namespaces{
		pulsar:     c,
		basePath:   "/namespaces",
		apiVersion: c.apiProfile.Namespaces,
	}
}

func (n *namespaces) GetNamespaces(tenant string) ([]string, error) {
	var namespaces []string
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, tenant)
	err := n.pulsar.restClient.Get(endpoint, &namespaces)
	return namespaces, err
}

func (n *namespaces) GetTopics(namespace string) ([]string, error) {
	var topics []string
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, ns.String(), "topics")
	err = n.pulsar.restClient.Get(endpoint, &topics)
	return topics, err
}

func (n *namespaces) GetPolicies(namespace string) (*Policies, error) {
	var police Policies
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, ns.String())
	err = n.pulsar.restClient.Get(endpoint, &police)
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
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, ns.String())
	return n.pulsar.restClient.Put(endpoint, &policies)
}

func (n *namespaces) CreateNsWithBundlesData(namespace string, bundleData *BundlesData) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, ns.String())
	polices := new(Policies)
	polices.Bundles = bundleData

	return n.pulsar.restClient.Put(endpoint, &polices)
}

func (n *namespaces) CreateNamespace(namespace string) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, ns.String())
	return n.pulsar.restClient.Put(endpoint, nil)
}

func (n *namespaces) DeleteNamespace(namespace string) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, ns.String())
	return n.pulsar.restClient.Delete(endpoint)
}

func (n *namespaces) DeleteNamespaceBundle(namespace string, bundleRange string) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, ns.String(), bundleRange)
	return n.pulsar.restClient.Delete(endpoint)
}

func (n *namespaces) GetNamespaceMessageTTL(namespace string) (int, error) {
	var ttl int
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return 0, err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "messageTTL")
	err = n.pulsar.restClient.Get(endpoint, &ttl)
	return ttl, err
}

func (n *namespaces) SetNamespaceMessageTTL(namespace string, ttlInSeconds int) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}

	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "messageTTL")
	return n.pulsar.restClient.Post(endpoint, &ttlInSeconds)
}

func (n *namespaces) SetRetention(namespace string, policy RetentionPolicies) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "retention")
	return n.pulsar.restClient.Post(endpoint, &policy)
}

func (n *namespaces) GetRetention(namespace string) (*RetentionPolicies, error) {
	var policy RetentionPolicies
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "retention")
	err = n.pulsar.restClient.Get(endpoint, &policy)
	return &policy, err
}

func (n *namespaces) GetBacklogQuotaMap(namespace string) (map[BacklogQuotaType]BacklogQuota, error) {
	var backlogQuotaMap map[BacklogQuotaType]BacklogQuota
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "backlogQuotaMap")
	err = n.pulsar.restClient.Get(endpoint, &backlogQuotaMap)
	return backlogQuotaMap, err
}

func (n *namespaces) SetBacklogQuota(namespace string, backlogQuota BacklogQuota,
	backlogQuotaType BacklogQuotaType,
) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "backlogQuota")
	params := make(map[string]string)
	params["backlogQuotaType"] = string(backlogQuotaType)
	return n.pulsar.restClient.PostWithQueryParams(endpoint, &backlogQuota, params)
}

func (n *namespaces) RemoveBacklogQuota(namespace string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "backlogQuota")
	params := map[string]string{
		"backlogQuotaType": string(DestinationStorage),
	}
	return n.pulsar.restClient.DeleteWithQueryParams(endpoint, params)
}

func (n *namespaces) SetTopicAutoCreation(namespace NameSpaceName, config TopicAutoCreationConfig) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "autoTopicCreation")
	return n.pulsar.restClient.Post(endpoint, &config)
}

func (n *namespaces) RemoveTopicAutoCreation(namespace NameSpaceName) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "autoTopicCreation")
	return n.pulsar.restClient.Delete(endpoint)
}

func (n *namespaces) SetSchemaValidationEnforced(namespace NameSpaceName, schemaValidationEnforced bool) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "schemaValidationEnforced")
	return n.pulsar.restClient.Post(endpoint, schemaValidationEnforced)
}

func (n *namespaces) GetSchemaValidationEnforced(namespace NameSpaceName) (bool, error) {
	var result bool
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "schemaValidationEnforced")
	err := n.pulsar.restClient.Get(endpoint, &result)
	return result, err
}

func (n *namespaces) SetSchemaAutoUpdateCompatibilityStrategy(namespace NameSpaceName,
	strategy SchemaCompatibilityStrategy,
) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "schemaAutoUpdateCompatibilityStrategy")
	return n.pulsar.restClient.Put(endpoint, strategy.String())
}

func (n *namespaces) GetSchemaAutoUpdateCompatibilityStrategy(namespace NameSpaceName) (
	SchemaCompatibilityStrategy, error,
) {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "schemaAutoUpdateCompatibilityStrategy")
	b, err := n.pulsar.restClient.GetWithQueryParams(endpoint, nil, nil, false)
	if err != nil {
		return "", err
	}
	s, err := ParseSchemaAutoUpdateCompatibilityStrategy(strings.ReplaceAll(string(b), "\"", ""))
	if err != nil {
		return "", err
	}
	return s, nil
}

func (n *namespaces) ClearOffloadDeleteLag(namespace NameSpaceName) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "offloadDeletionLagMs")
	return n.pulsar.restClient.Delete(endpoint)
}

func (n *namespaces) SetOffloadDeleteLag(namespace NameSpaceName, timeMs int64) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "offloadDeletionLagMs")
	return n.pulsar.restClient.Put(endpoint, timeMs)
}

func (n *namespaces) GetOffloadDeleteLag(namespace NameSpaceName) (int64, error) {
	var result int64
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "offloadDeletionLagMs")
	err := n.pulsar.restClient.Get(endpoint, &result)
	return result, err
}

func (n *namespaces) SetMaxConsumersPerSubscription(namespace NameSpaceName, max int) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "maxConsumersPerSubscription")
	return n.pulsar.restClient.Post(endpoint, max)
}

func (n *namespaces) GetMaxConsumersPerSubscription(namespace NameSpaceName) (int, error) {
	var result int
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "maxConsumersPerSubscription")
	err := n.pulsar.restClient.Get(endpoint, &result)
	return result, err
}

func (n *namespaces) SetOffloadThreshold(namespace NameSpaceName, threshold int64) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "offloadThreshold")
	return n.pulsar.restClient.Put(endpoint, threshold)
}

func (n *namespaces) GetOffloadThreshold(namespace NameSpaceName) (int64, error) {
	var result int64
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "offloadThreshold")
	err := n.pulsar.restClient.Get(endpoint, &result)
	return result, err
}

func (n *namespaces) SetMaxConsumersPerTopic(namespace NameSpaceName, max int) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "maxConsumersPerTopic")
	return n.pulsar.restClient.Post(endpoint, max)
}

func (n *namespaces) GetMaxConsumersPerTopic(namespace NameSpaceName) (int, error) {
	var result int
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "maxConsumersPerTopic")
	err := n.pulsar.restClient.Get(endpoint, &result)
	return result, err
}

func (n *namespaces) SetCompactionThreshold(namespace NameSpaceName, threshold int64) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "compactionThreshold")
	return n.pulsar.restClient.Put(endpoint, threshold)
}

func (n *namespaces) GetCompactionThreshold(namespace NameSpaceName) (int64, error) {
	var result int64
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "compactionThreshold")
	err := n.pulsar.restClient.Get(endpoint, &result)
	return result, err
}

func (n *namespaces) SetMaxProducersPerTopic(namespace NameSpaceName, max int) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "maxProducersPerTopic")
	return n.pulsar.restClient.Post(endpoint, max)
}

func (n *namespaces) GetMaxProducersPerTopic(namespace NameSpaceName) (int, error) {
	var result int
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "maxProducersPerTopic")
	err := n.pulsar.restClient.Get(endpoint, &result)
	return result, err
}

func (n *namespaces) GetNamespaceReplicationClusters(namespace string) ([]string, error) {
	var data []string
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "replication")
	err = n.pulsar.restClient.Get(endpoint, &data)
	return data, err
}

func (n *namespaces) SetNamespaceReplicationClusters(namespace string, clusterIds []string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "replication")
	return n.pulsar.restClient.Post(endpoint, &clusterIds)
}

func (n *namespaces) SetNamespaceAntiAffinityGroup(namespace string, namespaceAntiAffinityGroup string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "antiAffinity")
	return n.pulsar.restClient.Post(endpoint, namespaceAntiAffinityGroup)
}

func (n *namespaces) GetAntiAffinityNamespaces(tenant, cluster, namespaceAntiAffinityGroup string) ([]string, error) {
	var data []string
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, cluster, "antiAffinity", namespaceAntiAffinityGroup)
	params := map[string]string{
		"property": tenant,
	}
	_, err := n.pulsar.restClient.GetWithQueryParams(endpoint, &data, params, false)
	return data, err
}

func (n *namespaces) GetNamespaceAntiAffinityGroup(namespace string) (string, error) {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return "", err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "antiAffinity")
	data, err := n.pulsar.restClient.GetWithQueryParams(endpoint, nil, nil, false)
	return string(data), err
}

func (n *namespaces) DeleteNamespaceAntiAffinityGroup(namespace string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "antiAffinity")
	return n.pulsar.restClient.Delete(endpoint)
}

func (n *namespaces) SetDeduplicationStatus(namespace string, enableDeduplication bool) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "deduplication")
	return n.pulsar.restClient.Post(endpoint, enableDeduplication)
}

func (n *namespaces) SetPersistence(namespace string, persistence PersistencePolicies) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "persistence")
	return n.pulsar.restClient.Post(endpoint, &persistence)
}

func (n *namespaces) SetBookieAffinityGroup(namespace string, bookieAffinityGroup BookieAffinityGroupData) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "persistence", "bookieAffinity")
	return n.pulsar.restClient.Post(endpoint, &bookieAffinityGroup)
}

func (n *namespaces) DeleteBookieAffinityGroup(namespace string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "persistence", "bookieAffinity")
	return n.pulsar.restClient.Delete(endpoint)
}

func (n *namespaces) GetBookieAffinityGroup(namespace string) (*BookieAffinityGroupData, error) {
	var data BookieAffinityGroupData
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "persistence", "bookieAffinity")
	err = n.pulsar.restClient.Get(endpoint, &data)
	return &data, err
}

func (n *namespaces) GetPersistence(namespace string) (*PersistencePolicies, error) {
	var persistence PersistencePolicies
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "persistence")
	err = n.pulsar.restClient.Get(endpoint, &persistence)
	return &persistence, err
}

func (n *namespaces) Unload(namespace string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), "unload")
	return n.pulsar.restClient.Put(endpoint, nil)
}

func (n *namespaces) UnloadNamespaceBundle(namespace, bundle string) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), bundle, "unload")
	return n.pulsar.restClient.Put(endpoint, nil)
}

func (n *namespaces) SplitNamespaceBundle(namespace, bundle string, unloadSplitBundles bool) error {
	nsName, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, nsName.String(), bundle, "split")
	params := map[string]string{
		"unload": strconv.FormatBool(unloadSplitBundles),
	}
	return n.pulsar.restClient.PutWithQueryParams(endpoint, nil, nil, params)
}

func (n *namespaces) GetNamespacePermissions(namespace NameSpaceName) (map[string][]AuthAction, error) {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "permissions")
	var permissions map[string][]AuthAction
	err := n.pulsar.restClient.Get(endpoint, &permissions)
	return permissions, err
}

func (n *namespaces) GrantNamespacePermission(namespace NameSpaceName, role string,
	action []AuthAction,
) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "permissions", role)
	s := make([]string, 0)
	for _, v := range action {
		s = append(s, v.String())
	}
	return n.pulsar.restClient.Post(endpoint, s)
}

func (n *namespaces) RevokeNamespacePermission(namespace NameSpaceName, role string) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "permissions", role)
	return n.pulsar.restClient.Delete(endpoint)
}

func (n *namespaces) GrantSubPermission(namespace NameSpaceName, sName string, roles []string) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "permissions",
		"subscription", sName)
	return n.pulsar.restClient.Post(endpoint, roles)
}

func (n *namespaces) RevokeSubPermission(namespace NameSpaceName, sName, role string) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "permissions",
		"subscription", sName, role)
	return n.pulsar.restClient.Delete(endpoint)
}

func (n *namespaces) SetSubscriptionAuthMode(namespace NameSpaceName, mode SubscriptionAuthMode) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "subscriptionAuthMode")
	return n.pulsar.restClient.Post(endpoint, mode.String())
}

func (n *namespaces) SetEncryptionRequiredStatus(namespace NameSpaceName, encrypt bool) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "encryptionRequired")
	return n.pulsar.restClient.Post(endpoint, strconv.FormatBool(encrypt))
}

func (n *namespaces) UnsubscribeNamespace(namespace NameSpaceName, sName string) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "unsubscribe", url.QueryEscape(sName))
	return n.pulsar.restClient.Post(endpoint, nil)
}

func (n *namespaces) UnsubscribeNamespaceBundle(namespace NameSpaceName, bundle, sName string) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), bundle, "unsubscribe",
		url.QueryEscape(sName))
	return n.pulsar.restClient.Post(endpoint, nil)
}

func (n *namespaces) ClearNamespaceBundleBacklogForSubscription(namespace NameSpaceName,
	bundle, sName string,
) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), bundle, "clearBacklog",
		url.QueryEscape(sName))
	return n.pulsar.restClient.Post(endpoint, nil)
}

func (n *namespaces) ClearNamespaceBundleBacklog(namespace NameSpaceName, bundle string) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), bundle, "clearBacklog")
	return n.pulsar.restClient.Post(endpoint, nil)
}

func (n *namespaces) ClearNamespaceBacklogForSubscription(namespace NameSpaceName, sName string) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "clearBacklog", url.QueryEscape(sName))
	return n.pulsar.restClient.Post(endpoint, nil)
}

func (n *namespaces) ClearNamespaceBacklog(namespace NameSpaceName) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "clearBacklog")
	return n.pulsar.restClient.Post(endpoint, nil)
}

func (n *namespaces) SetReplicatorDispatchRate(namespace NameSpaceName, rate DispatchRate) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "replicatorDispatchRate")
	return n.pulsar.restClient.Post(endpoint, rate)
}

func (n *namespaces) GetReplicatorDispatchRate(namespace NameSpaceName) (DispatchRate, error) {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "replicatorDispatchRate")
	var rate DispatchRate
	err := n.pulsar.restClient.Get(endpoint, &rate)
	return rate, err
}

func (n *namespaces) SetSubscriptionDispatchRate(namespace NameSpaceName, rate DispatchRate) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "subscriptionDispatchRate")
	return n.pulsar.restClient.Post(endpoint, rate)
}

func (n *namespaces) GetSubscriptionDispatchRate(namespace NameSpaceName) (DispatchRate, error) {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "subscriptionDispatchRate")
	var rate DispatchRate
	err := n.pulsar.restClient.Get(endpoint, &rate)
	return rate, err
}

func (n *namespaces) SetSubscribeRate(namespace NameSpaceName, rate SubscribeRate) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "subscribeRate")
	return n.pulsar.restClient.Post(endpoint, rate)
}

func (n *namespaces) GetSubscribeRate(namespace NameSpaceName) (SubscribeRate, error) {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "subscribeRate")
	var rate SubscribeRate
	err := n.pulsar.restClient.Get(endpoint, &rate)
	return rate, err
}

func (n *namespaces) SetDispatchRate(namespace NameSpaceName, rate DispatchRate) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "dispatchRate")
	return n.pulsar.restClient.Post(endpoint, rate)
}

func (n *namespaces) GetDispatchRate(namespace NameSpaceName) (DispatchRate, error) {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "dispatchRate")
	var rate DispatchRate
	err := n.pulsar.restClient.Get(endpoint, &rate)
	return rate, err
}

func (n *namespaces) SetPublishRate(namespace NameSpaceName, pubRate PublishRate) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "publishRate")
	return n.pulsar.restClient.Post(endpoint, pubRate)
}

func (n *namespaces) GetPublishRate(namespace NameSpaceName) (PublishRate, error) {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "publishRate")
	var pubRate PublishRate
	err := n.pulsar.restClient.Get(endpoint, &pubRate)
	return pubRate, err
}

func (n *namespaces) SetIsAllowAutoUpdateSchema(namespace NameSpaceName, isAllowAutoUpdateSchema bool) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "isAllowAutoUpdateSchema")
	return n.pulsar.restClient.Post(endpoint, &isAllowAutoUpdateSchema)
}

func (n *namespaces) GetIsAllowAutoUpdateSchema(namespace NameSpaceName) (bool, error) {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "isAllowAutoUpdateSchema")
	var result bool
	err := n.pulsar.restClient.Get(endpoint, &result)
	return result, err
}

func (n *namespaces) GetInactiveTopicPolicies(namespace NameSpaceName) (InactiveTopicPolicies, error) {
	var out InactiveTopicPolicies
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "inactiveTopicPolicies")
	err := n.pulsar.restClient.Get(endpoint, &out)
	return out, err
}

func (n *namespaces) RemoveInactiveTopicPolicies(namespace NameSpaceName) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "inactiveTopicPolicies")
	return n.pulsar.restClient.Delete(endpoint)
}

func (n *namespaces) SetInactiveTopicPolicies(namespace NameSpaceName, data InactiveTopicPolicies) error {
	endpoint := n.pulsar.endpoint(n.apiVersion, n.basePath, namespace.String(), "inactiveTopicPolicies")
	return n.pulsar.restClient.Post(endpoint, data)
}
