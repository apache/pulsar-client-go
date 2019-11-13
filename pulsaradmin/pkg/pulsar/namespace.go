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
	"net/url"
	"strconv"
	"strings"

	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common"
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/utils"
)

// Namespaces is admin interface for namespaces management
type Namespaces interface {
	// GetNamespaces returns the list of all the namespaces for a certain tenant
	GetNamespaces(tenant string) ([]string, error)

	// GetTopics returns the list of all the topics under a certain namespace
	GetTopics(namespace string) ([]string, error)

	// GetPolicies returns the dump all the policies specified for a namespace
	GetPolicies(namespace string) (*utils.Policies, error)

	// CreateNamespace creates a new empty namespace with no policies attached
	CreateNamespace(namespace string) error

	// CreateNsWithNumBundles creates a new empty namespace with no policies attached
	CreateNsWithNumBundles(namespace string, numBundles int) error

	// CreateNsWithPolices creates a new namespace with the specified policies
	CreateNsWithPolices(namespace string, polices utils.Policies) error

	// CreateNsWithBundlesData creates a new empty namespace with no policies attached
	CreateNsWithBundlesData(namespace string, bundleData *utils.BundlesData) error

	// DeleteNamespace deletes an existing namespace
	DeleteNamespace(namespace string) error

	// DeleteNamespaceBundle deletes an existing bundle in a namespace
	DeleteNamespaceBundle(namespace string, bundleRange string) error

	// SetNamespaceMessageTTL sets the messages Time to Live for all the topics within a namespace
	SetNamespaceMessageTTL(namespace string, ttlInSeconds int) error

	// GetNamespaceMessageTTL returns the message TTL for a namespace
	GetNamespaceMessageTTL(namespace string) (int, error)

	// GetRetention returns the retention configuration for a namespace
	GetRetention(namespace string) (*utils.RetentionPolicies, error)

	// SetRetention sets the retention configuration for all the topics on a namespace
	SetRetention(namespace string, policy utils.RetentionPolicies) error

	// GetBacklogQuotaMap returns backlog quota map on a namespace
	GetBacklogQuotaMap(namespace string) (map[utils.BacklogQuotaType]utils.BacklogQuota, error)

	// SetBacklogQuota sets a backlog quota for all the topics on a namespace
	SetBacklogQuota(namespace string, backlogQuota utils.BacklogQuota) error

	// RemoveBacklogQuota removes a backlog quota policy from a namespace
	RemoveBacklogQuota(namespace string) error

	// SetSchemaValidationEnforced sets schema validation enforced for namespace
	SetSchemaValidationEnforced(namespace utils.NameSpaceName, schemaValidationEnforced bool) error

	// GetSchemaValidationEnforced returns schema validation enforced for namespace
	GetSchemaValidationEnforced(namespace utils.NameSpaceName) (bool, error)

	// SetSchemaAutoUpdateCompatibilityStrategy sets the strategy used to check the a new schema provided
	// by a producer is compatible with the current schema before it is installed
	SetSchemaAutoUpdateCompatibilityStrategy(namespace utils.NameSpaceName,
		strategy utils.SchemaCompatibilityStrategy) error

	// GetSchemaAutoUpdateCompatibilityStrategy returns the strategy used to check the a new schema provided
	// by a producer is compatible with the current schema before it is installed
	GetSchemaAutoUpdateCompatibilityStrategy(namespace utils.NameSpaceName) (utils.SchemaCompatibilityStrategy, error)

	// ClearOffloadDeleteLag clears the offload deletion lag for a namespace.
	ClearOffloadDeleteLag(namespace utils.NameSpaceName) error

	// SetOffloadDeleteLag sets the offload deletion lag for a namespace
	SetOffloadDeleteLag(namespace utils.NameSpaceName, timeMs int64) error

	// GetOffloadDeleteLag returns the offload deletion lag for a namespace, in milliseconds
	GetOffloadDeleteLag(namespace utils.NameSpaceName) (int64, error)

	// SetOffloadThreshold sets the offloadThreshold for a namespace
	SetOffloadThreshold(namespace utils.NameSpaceName, threshold int64) error

	// GetOffloadThreshold returns the offloadThreshold for a namespace
	GetOffloadThreshold(namespace utils.NameSpaceName) (int64, error)

	// SetCompactionThreshold sets the compactionThreshold for a namespace
	SetCompactionThreshold(namespace utils.NameSpaceName, threshold int64) error

	// GetCompactionThreshold returns the compactionThreshold for a namespace
	GetCompactionThreshold(namespace utils.NameSpaceName) (int64, error)

	// SetMaxConsumersPerSubscription sets maxConsumersPerSubscription for a namespace.
	SetMaxConsumersPerSubscription(namespace utils.NameSpaceName, max int) error

	// GetMaxConsumersPerSubscription returns the maxConsumersPerSubscription for a namespace.
	GetMaxConsumersPerSubscription(namespace utils.NameSpaceName) (int, error)

	// SetMaxConsumersPerTopic sets maxConsumersPerTopic for a namespace.
	SetMaxConsumersPerTopic(namespace utils.NameSpaceName, max int) error

	// GetMaxConsumersPerTopic returns the maxProducersPerTopic for a namespace.
	GetMaxConsumersPerTopic(namespace utils.NameSpaceName) (int, error)

	// SetMaxProducersPerTopic sets maxProducersPerTopic for a namespace.
	SetMaxProducersPerTopic(namespace utils.NameSpaceName, max int) error

	// GetMaxProducersPerTopic returns the maxProducersPerTopic for a namespace.
	GetMaxProducersPerTopic(namespace utils.NameSpaceName) (int, error)

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
	SetPersistence(namespace string, persistence utils.PersistencePolicies) error

	// GetPersistence returns the persistence configuration for a namespace
	GetPersistence(namespace string) (*utils.PersistencePolicies, error)

	// SetBookieAffinityGroup sets bookie affinity group for a namespace to isolate namespace write to bookies that are
	// part of given affinity group
	SetBookieAffinityGroup(namespace string, bookieAffinityGroup utils.BookieAffinityGroupData) error

	// DeleteBookieAffinityGroup deletes bookie affinity group configured for a namespace
	DeleteBookieAffinityGroup(namespace string) error

	// GetBookieAffinityGroup returns bookie affinity group configured for a namespace
	GetBookieAffinityGroup(namespace string) (*utils.BookieAffinityGroupData, error)

	// Unload a namespace from the current serving broker
	Unload(namespace string) error

	// UnloadNamespaceBundle unloads namespace bundle
	UnloadNamespaceBundle(namespace, bundle string) error

	// SplitNamespaceBundle splits namespace bundle
	SplitNamespaceBundle(namespace, bundle string, unloadSplitBundles bool) error

	// GetNamespacePermissions returns permissions on a namespace
	GetNamespacePermissions(namespace utils.NameSpaceName) (map[string][]common.AuthAction, error)

	// GrantNamespacePermission grants permission on a namespace.
	GrantNamespacePermission(namespace utils.NameSpaceName, role string, action []common.AuthAction) error

	// RevokeNamespacePermission revokes permissions on a namespace.
	RevokeNamespacePermission(namespace utils.NameSpaceName, role string) error

	// GrantSubPermission grants permission to role to access subscription's admin-api
	GrantSubPermission(namespace utils.NameSpaceName, sName string, roles []string) error

	// RevokeSubPermission revoke permissions on a subscription's admin-api access
	RevokeSubPermission(namespace utils.NameSpaceName, sName, role string) error

	// SetSubscriptionAuthMode sets the given subscription auth mode on all topics on a namespace
	SetSubscriptionAuthMode(namespace utils.NameSpaceName, mode utils.SubscriptionAuthMode) error

	// SetEncryptionRequiredStatus sets the encryption required status for all topics within a namespace
	SetEncryptionRequiredStatus(namespace utils.NameSpaceName, encrypt bool) error

	// UnsubscribeNamespace unsubscribe the given subscription on all topics on a namespace
	UnsubscribeNamespace(namespace utils.NameSpaceName, sName string) error

	// UnsubscribeNamespaceBundle unsubscribe the given subscription on all topics on a namespace bundle
	UnsubscribeNamespaceBundle(namespace utils.NameSpaceName, bundle, sName string) error

	// ClearNamespaceBundleBacklogForSubscription clears backlog for a given subscription on all
	// topics on a namespace bundle
	ClearNamespaceBundleBacklogForSubscription(namespace utils.NameSpaceName, bundle, sName string) error

	// ClearNamespaceBundleBacklog clears backlog for all topics on a namespace bundle
	ClearNamespaceBundleBacklog(namespace utils.NameSpaceName, bundle string) error

	// ClearNamespaceBacklogForSubscription clears backlog for a given subscription on all topics on a namespace
	ClearNamespaceBacklogForSubscription(namespace utils.NameSpaceName, sName string) error

	// ClearNamespaceBacklog clears backlog for all topics on a namespace
	ClearNamespaceBacklog(namespace utils.NameSpaceName) error

	// SetReplicatorDispatchRate sets replicator-Message-dispatch-rate (Replicators under this namespace
	// can dispatch this many messages per second)
	SetReplicatorDispatchRate(namespace utils.NameSpaceName, rate utils.DispatchRate) error

	// Get replicator-Message-dispatch-rate (Replicators under this namespace
	// can dispatch this many messages per second)
	GetReplicatorDispatchRate(namespace utils.NameSpaceName) (utils.DispatchRate, error)

	// SetSubscriptionDispatchRate sets subscription-Message-dispatch-rate (subscriptions under this namespace
	// can dispatch this many messages per second)
	SetSubscriptionDispatchRate(namespace utils.NameSpaceName, rate utils.DispatchRate) error

	// GetSubscriptionDispatchRate returns subscription-Message-dispatch-rate (subscriptions under this namespace
	// can dispatch this many messages per second)
	GetSubscriptionDispatchRate(namespace utils.NameSpaceName) (utils.DispatchRate, error)

	// SetSubscribeRate sets namespace-subscribe-rate (topics under this namespace will limit by subscribeRate)
	SetSubscribeRate(namespace utils.NameSpaceName, rate utils.SubscribeRate) error

	// GetSubscribeRate returns namespace-subscribe-rate (topics under this namespace allow subscribe
	// times per consumer in a period)
	GetSubscribeRate(namespace utils.NameSpaceName) (utils.SubscribeRate, error)

	// SetDispatchRate sets Message-dispatch-rate (topics under this namespace can dispatch
	// this many messages per second)
	SetDispatchRate(namespace utils.NameSpaceName, rate utils.DispatchRate) error

	// GetDispatchRate returns Message-dispatch-rate (topics under this namespace can dispatch
	// this many messages per second)
	GetDispatchRate(namespace utils.NameSpaceName) (utils.DispatchRate, error)
}

type namespaces struct {
	pulsar   *pulsarClient
	basePath string
}

// Namespaces is used to access the namespaces endpoints
func (c *pulsarClient) Namespaces() Namespaces {
	return &namespaces{
		pulsar:   c,
		basePath: "/namespaces",
	}
}

func (n *namespaces) GetNamespaces(tenant string) ([]string, error) {
	var namespaces []string
	endpoint := n.pulsar.endpoint(n.basePath, tenant)
	err := n.pulsar.Client.Get(endpoint, &namespaces)
	return namespaces, err
}

func (n *namespaces) GetTopics(namespace string) ([]string, error) {
	var topics []string
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String(), "topics")
	err = n.pulsar.Client.Get(endpoint, &topics)
	return topics, err
}

func (n *namespaces) GetPolicies(namespace string) (*utils.Policies, error) {
	var police utils.Policies
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String())
	err = n.pulsar.Client.Get(endpoint, &police)
	return &police, err
}

func (n *namespaces) CreateNsWithNumBundles(namespace string, numBundles int) error {
	return n.CreateNsWithBundlesData(namespace, utils.NewBundlesDataWithNumBundles(numBundles))
}

func (n *namespaces) CreateNsWithPolices(namespace string, policies utils.Policies) error {
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String())
	return n.pulsar.Client.Put(endpoint, &policies)
}

func (n *namespaces) CreateNsWithBundlesData(namespace string, bundleData *utils.BundlesData) error {
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String())
	polices := new(utils.Policies)
	polices.Bundles = bundleData

	return n.pulsar.Client.Put(endpoint, &polices)
}

func (n *namespaces) CreateNamespace(namespace string) error {
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String())
	return n.pulsar.Client.Put(endpoint, nil)
}

func (n *namespaces) DeleteNamespace(namespace string) error {
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String())
	return n.pulsar.Client.Delete(endpoint)
}

func (n *namespaces) DeleteNamespaceBundle(namespace string, bundleRange string) error {
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String(), bundleRange)
	return n.pulsar.Client.Delete(endpoint)
}

func (n *namespaces) GetNamespaceMessageTTL(namespace string) (int, error) {
	var ttl int
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return 0, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "messageTTL")
	err = n.pulsar.Client.Get(endpoint, &ttl)
	return ttl, err
}

func (n *namespaces) SetNamespaceMessageTTL(namespace string, ttlInSeconds int) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}

	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "messageTTL")
	return n.pulsar.Client.Post(endpoint, &ttlInSeconds)
}

func (n *namespaces) SetRetention(namespace string, policy utils.RetentionPolicies) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "retention")
	return n.pulsar.Client.Post(endpoint, &policy)
}

func (n *namespaces) GetRetention(namespace string) (*utils.RetentionPolicies, error) {
	var policy utils.RetentionPolicies
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "retention")
	err = n.pulsar.Client.Get(endpoint, &policy)
	return &policy, err
}

func (n *namespaces) GetBacklogQuotaMap(namespace string) (map[utils.BacklogQuotaType]utils.BacklogQuota, error) {
	var backlogQuotaMap map[utils.BacklogQuotaType]utils.BacklogQuota
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "backlogQuotaMap")
	err = n.pulsar.Client.Get(endpoint, &backlogQuotaMap)
	return backlogQuotaMap, err
}

func (n *namespaces) SetBacklogQuota(namespace string, backlogQuota utils.BacklogQuota) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "backlogQuota")
	return n.pulsar.Client.Post(endpoint, &backlogQuota)
}

func (n *namespaces) RemoveBacklogQuota(namespace string) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "backlogQuota")
	params := map[string]string{
		"backlogQuotaType": string(utils.DestinationStorage),
	}
	return n.pulsar.Client.DeleteWithQueryParams(endpoint, params)
}

func (n *namespaces) SetSchemaValidationEnforced(namespace utils.NameSpaceName, schemaValidationEnforced bool) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "schemaValidationEnforced")
	return n.pulsar.Client.Post(endpoint, schemaValidationEnforced)
}

func (n *namespaces) GetSchemaValidationEnforced(namespace utils.NameSpaceName) (bool, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "schemaValidationEnforced")
	r, err := n.pulsar.Client.GetWithQueryParams(endpoint, nil, nil, false)
	if err != nil {
		return false, err
	}
	return strconv.ParseBool(string(r))
}

func (n *namespaces) SetSchemaAutoUpdateCompatibilityStrategy(namespace utils.NameSpaceName,
	strategy utils.SchemaCompatibilityStrategy) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "schemaAutoUpdateCompatibilityStrategy")
	return n.pulsar.Client.Put(endpoint, strategy.String())
}

func (n *namespaces) GetSchemaAutoUpdateCompatibilityStrategy(namespace utils.NameSpaceName) (
	utils.SchemaCompatibilityStrategy, error) {

	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "schemaAutoUpdateCompatibilityStrategy")
	b, err := n.pulsar.Client.GetWithQueryParams(endpoint, nil, nil, false)
	if err != nil {
		return "", err
	}
	s, err := utils.ParseSchemaAutoUpdateCompatibilityStrategy(strings.ReplaceAll(string(b), "\"", ""))
	if err != nil {
		return "", err
	}
	return s, nil
}

func (n *namespaces) ClearOffloadDeleteLag(namespace utils.NameSpaceName) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "offloadDeletionLagMs")
	return n.pulsar.Client.Delete(endpoint)
}

func (n *namespaces) SetOffloadDeleteLag(namespace utils.NameSpaceName, timeMs int64) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "offloadDeletionLagMs")
	return n.pulsar.Client.Put(endpoint, timeMs)
}

func (n *namespaces) GetOffloadDeleteLag(namespace utils.NameSpaceName) (int64, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "offloadDeletionLagMs")
	b, err := n.pulsar.Client.GetWithQueryParams(endpoint, nil, nil, false)
	if err != nil {
		return -1, err
	}
	return strconv.ParseInt(string(b), 10, 64)
}

func (n *namespaces) SetMaxConsumersPerSubscription(namespace utils.NameSpaceName, max int) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxConsumersPerSubscription")
	return n.pulsar.Client.Post(endpoint, max)
}

func (n *namespaces) GetMaxConsumersPerSubscription(namespace utils.NameSpaceName) (int, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxConsumersPerSubscription")
	b, err := n.pulsar.Client.GetWithQueryParams(endpoint, nil, nil, false)
	if err != nil {
		return -1, err
	}
	return strconv.Atoi(string(b))
}

func (n *namespaces) SetOffloadThreshold(namespace utils.NameSpaceName, threshold int64) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "offloadThreshold")
	return n.pulsar.Client.Put(endpoint, threshold)
}

func (n *namespaces) GetOffloadThreshold(namespace utils.NameSpaceName) (int64, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "offloadThreshold")
	b, err := n.pulsar.Client.GetWithQueryParams(endpoint, nil, nil, false)
	if err != nil {
		return -1, err
	}
	return strconv.ParseInt(string(b), 10, 64)
}

func (n *namespaces) SetMaxConsumersPerTopic(namespace utils.NameSpaceName, max int) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxConsumersPerTopic")
	return n.pulsar.Client.Post(endpoint, max)
}

func (n *namespaces) GetMaxConsumersPerTopic(namespace utils.NameSpaceName) (int, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxConsumersPerTopic")
	b, err := n.pulsar.Client.GetWithQueryParams(endpoint, nil, nil, false)
	if err != nil {
		return -1, err
	}
	return strconv.Atoi(string(b))
}

func (n *namespaces) SetCompactionThreshold(namespace utils.NameSpaceName, threshold int64) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "compactionThreshold")
	return n.pulsar.Client.Put(endpoint, threshold)
}

func (n *namespaces) GetCompactionThreshold(namespace utils.NameSpaceName) (int64, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "compactionThreshold")
	b, err := n.pulsar.Client.GetWithQueryParams(endpoint, nil, nil, false)
	if err != nil {
		return -1, err
	}
	return strconv.ParseInt(string(b), 10, 64)
}

func (n *namespaces) SetMaxProducersPerTopic(namespace utils.NameSpaceName, max int) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxProducersPerTopic")
	return n.pulsar.Client.Post(endpoint, max)
}

func (n *namespaces) GetMaxProducersPerTopic(namespace utils.NameSpaceName) (int, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxProducersPerTopic")
	b, err := n.pulsar.Client.GetWithQueryParams(endpoint, nil, nil, false)
	if err != nil {
		return -1, err
	}
	return strconv.Atoi(string(b))
}

func (n *namespaces) GetNamespaceReplicationClusters(namespace string) ([]string, error) {
	var data []string
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "replication")
	err = n.pulsar.Client.Get(endpoint, &data)
	return data, err
}

func (n *namespaces) SetNamespaceReplicationClusters(namespace string, clusterIds []string) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "replication")
	return n.pulsar.Client.Post(endpoint, &clusterIds)
}

func (n *namespaces) SetNamespaceAntiAffinityGroup(namespace string, namespaceAntiAffinityGroup string) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "antiAffinity")
	return n.pulsar.Client.Post(endpoint, namespaceAntiAffinityGroup)
}

func (n *namespaces) GetAntiAffinityNamespaces(tenant, cluster, namespaceAntiAffinityGroup string) ([]string, error) {
	var data []string
	endpoint := n.pulsar.endpoint(n.basePath, cluster, "antiAffinity", namespaceAntiAffinityGroup)
	params := map[string]string{
		"property": tenant,
	}
	_, err := n.pulsar.Client.GetWithQueryParams(endpoint, &data, params, false)
	return data, err
}

func (n *namespaces) GetNamespaceAntiAffinityGroup(namespace string) (string, error) {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return "", err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "antiAffinity")
	data, err := n.pulsar.Client.GetWithQueryParams(endpoint, nil, nil, false)
	return string(data), err
}

func (n *namespaces) DeleteNamespaceAntiAffinityGroup(namespace string) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "antiAffinity")
	return n.pulsar.Client.Delete(endpoint)
}

func (n *namespaces) SetDeduplicationStatus(namespace string, enableDeduplication bool) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "deduplication")
	return n.pulsar.Client.Post(endpoint, enableDeduplication)
}

func (n *namespaces) SetPersistence(namespace string, persistence utils.PersistencePolicies) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "persistence")
	return n.pulsar.Client.Post(endpoint, &persistence)
}

func (n *namespaces) SetBookieAffinityGroup(namespace string, bookieAffinityGroup utils.BookieAffinityGroupData) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "persistence", "bookieAffinity")
	return n.pulsar.Client.Post(endpoint, &bookieAffinityGroup)
}

func (n *namespaces) DeleteBookieAffinityGroup(namespace string) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "persistence", "bookieAffinity")
	return n.pulsar.Client.Delete(endpoint)
}

func (n *namespaces) GetBookieAffinityGroup(namespace string) (*utils.BookieAffinityGroupData, error) {
	var data utils.BookieAffinityGroupData
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "persistence", "bookieAffinity")
	err = n.pulsar.Client.Get(endpoint, &data)
	return &data, err
}

func (n *namespaces) GetPersistence(namespace string) (*utils.PersistencePolicies, error) {
	var persistence utils.PersistencePolicies
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "persistence")
	err = n.pulsar.Client.Get(endpoint, &persistence)
	return &persistence, err
}

func (n *namespaces) Unload(namespace string) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "unload")
	return n.pulsar.Client.Put(endpoint, "")
}

func (n *namespaces) UnloadNamespaceBundle(namespace, bundle string) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), bundle, "unload")
	return n.pulsar.Client.Put(endpoint, "")
}

func (n *namespaces) SplitNamespaceBundle(namespace, bundle string, unloadSplitBundles bool) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), bundle, "split")
	params := map[string]string{
		"unload": strconv.FormatBool(unloadSplitBundles),
	}
	return n.pulsar.Client.PutWithQueryParams(endpoint, "", nil, params)
}

func (n *namespaces) GetNamespacePermissions(namespace utils.NameSpaceName) (map[string][]common.AuthAction, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "permissions")
	var permissions map[string][]common.AuthAction
	err := n.pulsar.Client.Get(endpoint, &permissions)
	return permissions, err
}

func (n *namespaces) GrantNamespacePermission(namespace utils.NameSpaceName, role string,
	action []common.AuthAction) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "permissions", role)
	s := make([]string, 0)
	for _, v := range action {
		s = append(s, v.String())
	}
	return n.pulsar.Client.Post(endpoint, s)
}

func (n *namespaces) RevokeNamespacePermission(namespace utils.NameSpaceName, role string) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "permissions", role)
	return n.pulsar.Client.Delete(endpoint)
}

func (n *namespaces) GrantSubPermission(namespace utils.NameSpaceName, sName string, roles []string) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "permissions",
		"subscription", sName)
	return n.pulsar.Client.Post(endpoint, roles)
}

func (n *namespaces) RevokeSubPermission(namespace utils.NameSpaceName, sName, role string) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "permissions",
		"subscription", sName, role)
	return n.pulsar.Client.Delete(endpoint)
}

func (n *namespaces) SetSubscriptionAuthMode(namespace utils.NameSpaceName, mode utils.SubscriptionAuthMode) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscriptionAuthMode")
	return n.pulsar.Client.Post(endpoint, mode.String())
}

func (n *namespaces) SetEncryptionRequiredStatus(namespace utils.NameSpaceName, encrypt bool) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "encryptionRequired")
	return n.pulsar.Client.Post(endpoint, strconv.FormatBool(encrypt))
}

func (n *namespaces) UnsubscribeNamespace(namespace utils.NameSpaceName, sName string) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "unsubscribe", url.QueryEscape(sName))
	return n.pulsar.Client.Post(endpoint, "")
}

func (n *namespaces) UnsubscribeNamespaceBundle(namespace utils.NameSpaceName, bundle, sName string) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), bundle, "unsubscribe", url.QueryEscape(sName))
	return n.pulsar.Client.Post(endpoint, "")
}

func (n *namespaces) ClearNamespaceBundleBacklogForSubscription(namespace utils.NameSpaceName,
	bundle, sName string) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), bundle, "clearBacklog", url.QueryEscape(sName))
	return n.pulsar.Client.Post(endpoint, "")
}

func (n *namespaces) ClearNamespaceBundleBacklog(namespace utils.NameSpaceName, bundle string) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), bundle, "clearBacklog")
	return n.pulsar.Client.Post(endpoint, "")
}

func (n *namespaces) ClearNamespaceBacklogForSubscription(namespace utils.NameSpaceName, sName string) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "clearBacklog", url.QueryEscape(sName))
	return n.pulsar.Client.Post(endpoint, "")
}

func (n *namespaces) ClearNamespaceBacklog(namespace utils.NameSpaceName) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "clearBacklog")
	return n.pulsar.Client.Post(endpoint, "")
}

func (n *namespaces) SetReplicatorDispatchRate(namespace utils.NameSpaceName, rate utils.DispatchRate) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "replicatorDispatchRate")
	return n.pulsar.Client.Post(endpoint, rate)
}

func (n *namespaces) GetReplicatorDispatchRate(namespace utils.NameSpaceName) (utils.DispatchRate, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "replicatorDispatchRate")
	var rate utils.DispatchRate
	err := n.pulsar.Client.Get(endpoint, &rate)
	return rate, err
}

func (n *namespaces) SetSubscriptionDispatchRate(namespace utils.NameSpaceName, rate utils.DispatchRate) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscriptionDispatchRate")
	return n.pulsar.Client.Post(endpoint, rate)
}

func (n *namespaces) GetSubscriptionDispatchRate(namespace utils.NameSpaceName) (utils.DispatchRate, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscriptionDispatchRate")
	var rate utils.DispatchRate
	err := n.pulsar.Client.Get(endpoint, &rate)
	return rate, err
}

func (n *namespaces) SetSubscribeRate(namespace utils.NameSpaceName, rate utils.SubscribeRate) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscribeRate")
	return n.pulsar.Client.Post(endpoint, rate)
}

func (n *namespaces) GetSubscribeRate(namespace utils.NameSpaceName) (utils.SubscribeRate, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscribeRate")
	var rate utils.SubscribeRate
	err := n.pulsar.Client.Get(endpoint, &rate)
	return rate, err
}

func (n *namespaces) SetDispatchRate(namespace utils.NameSpaceName, rate utils.DispatchRate) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "dispatchRate")
	return n.pulsar.Client.Post(endpoint, rate)
}

func (n *namespaces) GetDispatchRate(namespace utils.NameSpaceName) (utils.DispatchRate, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "dispatchRate")
	var rate utils.DispatchRate
	err := n.pulsar.Client.Get(endpoint, &rate)
	return rate, err
}
