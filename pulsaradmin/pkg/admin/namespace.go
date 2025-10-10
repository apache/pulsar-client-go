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
	"net/url"
	"strconv"
	"strings"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

// Namespaces is admin interface for namespaces management
type Namespaces interface {
	// GetNamespaces returns the list of all the namespaces for a certain tenant
	GetNamespaces(tenant string) ([]string, error)

	// GetNamespacesWithContext returns the list of all the namespaces for a certain tenant
	GetNamespacesWithContext(ctx context.Context, tenant string) ([]string, error)

	// GetTopics returns the list of all the topics under a certain namespace
	GetTopics(namespace string) ([]string, error)

	// GetTopicsWithContext returns the list of all the topics under a certain namespace
	GetTopicsWithContext(ctx context.Context, namespace string) ([]string, error)

	// GetPolicies returns the dump all the policies specified for a namespace
	GetPolicies(namespace string) (*utils.Policies, error)

	// GetPoliciesWithContext returns the dump all the policies specified for a namespace
	GetPoliciesWithContext(ctx context.Context, namespace string) (*utils.Policies, error)

	// CreateNamespace creates a new empty namespace with no policies attached
	CreateNamespace(namespace string) error

	// CreateNamespaceWithContext creates a new empty namespace with no policies attached
	CreateNamespaceWithContext(ctx context.Context, namespace string) error

	// CreateNsWithNumBundles creates a new empty namespace with no policies attached
	CreateNsWithNumBundles(namespace string, numBundles int) error

	// CreateNsWithNumBundlesWithContext creates a new empty namespace with no policies attached
	CreateNsWithNumBundlesWithContext(ctx context.Context, namespace string, numBundles int) error

	// CreateNsWithPolices creates a new namespace with the specified policies
	CreateNsWithPolices(namespace string, polices utils.Policies) error

	// CreateNsWithPolicesWithContext creates a new namespace with the specified policies
	CreateNsWithPolicesWithContext(ctx context.Context, namespace string, polices utils.Policies) error

	// CreateNsWithBundlesData creates a new empty namespace with no policies attached
	CreateNsWithBundlesData(namespace string, bundleData *utils.BundlesData) error

	// CreateNsWithBundlesDataWithContext creates a new empty namespace with no policies attached
	CreateNsWithBundlesDataWithContext(ctx context.Context, namespace string, bundleData *utils.BundlesData) error

	// DeleteNamespace deletes an existing namespace
	DeleteNamespace(namespace string) error

	// DeleteNamespaceWithContext deletes an existing namespace
	DeleteNamespaceWithContext(ctx context.Context, namespace string) error

	// DeleteNamespaceBundle deletes an existing bundle in a namespace
	DeleteNamespaceBundle(namespace string, bundleRange string) error

	// DeleteNamespaceBundleWithContext deletes an existing bundle in a namespace
	DeleteNamespaceBundleWithContext(ctx context.Context, namespace string, bundleRange string) error

	// SetNamespaceMessageTTL sets the messages Time to Live for all the topics within a namespace
	SetNamespaceMessageTTL(namespace string, ttlInSeconds int) error

	// SetNamespaceMessageTTLWithContext sets the messages Time to Live for all the topics within a namespace
	SetNamespaceMessageTTLWithContext(ctx context.Context, namespace string, ttlInSeconds int) error

	// GetNamespaceMessageTTL returns the message TTL for a namespace
	GetNamespaceMessageTTL(namespace string) (int, error)

	// GetNamespaceMessageTTLWithContext returns the message TTL for a namespace
	GetNamespaceMessageTTLWithContext(ctx context.Context, namespace string) (int, error)

	// GetRetention returns the retention configuration for a namespace
	GetRetention(namespace string) (*utils.RetentionPolicies, error)

	// GetRetentionWithContext returns the retention configuration for a namespace
	GetRetentionWithContext(ctx context.Context, namespace string) (*utils.RetentionPolicies, error)

	// SetRetention sets the retention configuration for all the topics on a namespace
	SetRetention(namespace string, policy utils.RetentionPolicies) error

	// SetRetentionWithContext sets the retention configuration for all the topics on a namespace
	SetRetentionWithContext(ctx context.Context, namespace string, policy utils.RetentionPolicies) error

	// GetBacklogQuotaMap returns backlog quota map on a namespace
	GetBacklogQuotaMap(namespace string) (map[utils.BacklogQuotaType]utils.BacklogQuota, error)

	// GetBacklogQuotaMapWithContext returns backlog quota map on a namespace
	GetBacklogQuotaMapWithContext(
		ctx context.Context,
		namespace string,
	) (map[utils.BacklogQuotaType]utils.BacklogQuota, error)

	// SetBacklogQuota sets a backlog quota for all the topics on a namespace
	SetBacklogQuota(namespace string, backlogQuota utils.BacklogQuota, backlogQuotaType utils.BacklogQuotaType) error

	// SetBacklogQuotaWithContext sets a backlog quota for all the topics on a namespace
	SetBacklogQuotaWithContext(
		ctx context.Context,
		namespace string,
		backlogQuota utils.BacklogQuota,
		backlogQuotaType utils.BacklogQuotaType,
	) error

	// RemoveBacklogQuota removes a backlog quota policy from a namespace
	RemoveBacklogQuota(namespace string) error

	// RemoveBacklogQuotaWithContext removes a backlog quota policy from a namespace
	RemoveBacklogQuotaWithContext(ctx context.Context, namespace string) error

	// GetTopicAutoCreation returns the topic auto-creation config for a namespace
	GetTopicAutoCreation(namespace utils.NameSpaceName) (*utils.TopicAutoCreationConfig, error)

	// GetTopicAutoCreationWithContext returns the topic auto-creation config for a namespace
	GetTopicAutoCreationWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
	) (*utils.TopicAutoCreationConfig, error)

	// SetTopicAutoCreation sets topic auto-creation config for a namespace, overriding broker settings
	SetTopicAutoCreation(namespace utils.NameSpaceName, config utils.TopicAutoCreationConfig) error

	// SetTopicAutoCreationWithContext sets topic auto-creation config for a namespace, overriding broker settings
	SetTopicAutoCreationWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
		config utils.TopicAutoCreationConfig,
	) error

	// RemoveTopicAutoCreation removes topic auto-creation config for a namespace, defaulting to broker settings
	RemoveTopicAutoCreation(namespace utils.NameSpaceName) error

	// RemoveTopicAutoCreationWithContext removes topic auto-creation config for a namespace, defaulting to broker settings
	RemoveTopicAutoCreationWithContext(ctx context.Context, namespace utils.NameSpaceName) error

	// SetSchemaValidationEnforced sets schema validation enforced for namespace
	SetSchemaValidationEnforced(namespace utils.NameSpaceName, schemaValidationEnforced bool) error

	// SetSchemaValidationEnforcedWithContext sets schema validation enforced for namespace
	SetSchemaValidationEnforcedWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
		schemaValidationEnforced bool,
	) error

	// GetSchemaValidationEnforced returns schema validation enforced for namespace
	GetSchemaValidationEnforced(namespace utils.NameSpaceName) (bool, error)

	// GetSchemaValidationEnforcedWithContext returns schema validation enforced for namespace
	GetSchemaValidationEnforcedWithContext(ctx context.Context, namespace utils.NameSpaceName) (bool, error)

	// SetSchemaAutoUpdateCompatibilityStrategy sets the strategy used to check the new schema provided
	// by a producer is compatible with the current schema before it is installed
	SetSchemaAutoUpdateCompatibilityStrategy(namespace utils.NameSpaceName,
		strategy utils.SchemaAutoUpdateCompatibilityStrategy) error

	// SetSchemaAutoUpdateCompatibilityStrategyWithContext sets the strategy used to check the new schema provided
	// by a producer is compatible with the current schema before it is installed
	SetSchemaAutoUpdateCompatibilityStrategyWithContext(ctx context.Context, namespace utils.NameSpaceName,
		strategy utils.SchemaAutoUpdateCompatibilityStrategy) error

	// GetSchemaAutoUpdateCompatibilityStrategy returns the strategy used to check the new schema provided
	// by a producer is compatible with the current schema before it is installed
	GetSchemaAutoUpdateCompatibilityStrategy(namespace utils.NameSpaceName) (
		utils.SchemaAutoUpdateCompatibilityStrategy, error)

	// GetSchemaAutoUpdateCompatibilityStrategyWithContext returns the strategy used to check the new schema provided
	// by a producer is compatible with the current schema before it is installed
	GetSchemaAutoUpdateCompatibilityStrategyWithContext(ctx context.Context, namespace utils.NameSpaceName) (
		utils.SchemaAutoUpdateCompatibilityStrategy, error)

	// SetSchemaCompatibilityStrategy sets the strategy used to check the new schema provided
	// by a producer is compatible with the current schema before it is installed
	SetSchemaCompatibilityStrategy(namespace utils.NameSpaceName,
		strategy utils.SchemaCompatibilityStrategy) error

	// SetSchemaCompatibilityStrategyWithContext sets the strategy used to check the new schema provided
	// by a producer is compatible with the current schema before it is installed
	SetSchemaCompatibilityStrategyWithContext(ctx context.Context, namespace utils.NameSpaceName,
		strategy utils.SchemaCompatibilityStrategy) error

	// GetSchemaCompatibilityStrategy returns the strategy used to check the new schema provided
	// by a producer is compatible with the current schema before it is installed
	GetSchemaCompatibilityStrategy(namespace utils.NameSpaceName) (utils.SchemaCompatibilityStrategy, error)

	// GetSchemaCompatibilityStrategyWithContext returns the strategy used to check the new schema provided
	// by a producer is compatible with the current schema before it is installed
	GetSchemaCompatibilityStrategyWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
	) (utils.SchemaCompatibilityStrategy, error)

	// ClearOffloadDeleteLag clears the offload deletion lag for a namespace.
	ClearOffloadDeleteLag(namespace utils.NameSpaceName) error

	// ClearOffloadDeleteLagWithContext clears the offload deletion lag for a namespace.
	ClearOffloadDeleteLagWithContext(ctx context.Context, namespace utils.NameSpaceName) error

	// SetOffloadDeleteLag sets the offload deletion lag for a namespace
	SetOffloadDeleteLag(namespace utils.NameSpaceName, timeMs int64) error

	// SetOffloadDeleteLagWithContext sets the offload deletion lag for a namespace
	SetOffloadDeleteLagWithContext(ctx context.Context, namespace utils.NameSpaceName, timeMs int64) error

	// GetOffloadDeleteLag returns the offload deletion lag for a namespace, in milliseconds
	GetOffloadDeleteLag(namespace utils.NameSpaceName) (int64, error)

	// GetOffloadDeleteLagWithContext returns the offload deletion lag for a namespace, in milliseconds
	GetOffloadDeleteLagWithContext(ctx context.Context, namespace utils.NameSpaceName) (int64, error)

	// SetOffloadThreshold sets the offloadThreshold for a namespace
	SetOffloadThreshold(namespace utils.NameSpaceName, threshold int64) error

	// SetOffloadThresholdWithContext sets the offloadThreshold for a namespace
	SetOffloadThresholdWithContext(ctx context.Context, namespace utils.NameSpaceName, threshold int64) error

	// GetOffloadThreshold returns the offloadThreshold for a namespace
	GetOffloadThreshold(namespace utils.NameSpaceName) (int64, error)

	// GetOffloadThresholdWithContext returns the offloadThreshold for a namespace
	GetOffloadThresholdWithContext(ctx context.Context, namespace utils.NameSpaceName) (int64, error)

	// SetOffloadThresholdInSeconds sets the offloadThresholdInSeconds for a namespace
	SetOffloadThresholdInSeconds(namespace utils.NameSpaceName, threshold int64) error

	// SetOffloadThresholdInSecondsWithContext sets the offloadThresholdInSeconds for a namespace
	SetOffloadThresholdInSecondsWithContext(ctx context.Context, namespace utils.NameSpaceName, threshold int64) error

	// GetOffloadThresholdInSeconds returns the offloadThresholdInSeconds for a namespace
	GetOffloadThresholdInSeconds(namespace utils.NameSpaceName) (int64, error)

	// GetOffloadThresholdInSecondsWithContext returns the offloadThresholdInSeconds for a namespace
	GetOffloadThresholdInSecondsWithContext(ctx context.Context, namespace utils.NameSpaceName) (int64, error)

	// SetCompactionThreshold sets the compactionThreshold for a namespace
	SetCompactionThreshold(namespace utils.NameSpaceName, threshold int64) error

	// SetCompactionThresholdWithContext sets the compactionThreshold for a namespace
	SetCompactionThresholdWithContext(ctx context.Context, namespace utils.NameSpaceName, threshold int64) error

	// GetCompactionThreshold returns the compactionThreshold for a namespace
	GetCompactionThreshold(namespace utils.NameSpaceName) (int64, error)

	// GetCompactionThresholdWithContext returns the compactionThreshold for a namespace
	GetCompactionThresholdWithContext(ctx context.Context, namespace utils.NameSpaceName) (int64, error)

	// SetMaxConsumersPerSubscription sets maxConsumersPerSubscription for a namespace.
	//nolint: revive // It's ok here to use a built-in function name (max)
	SetMaxConsumersPerSubscription(namespace utils.NameSpaceName, max int) error

	// SetMaxConsumersPerSubscriptionWithContext sets maxConsumersPerSubscription for a namespace.
	//nolint: revive // It's ok here to use a built-in function name (max)
	SetMaxConsumersPerSubscriptionWithContext(ctx context.Context, namespace utils.NameSpaceName, max int) error

	// GetMaxConsumersPerSubscription returns the maxConsumersPerSubscription for a namespace.
	GetMaxConsumersPerSubscription(namespace utils.NameSpaceName) (int, error)

	// GetMaxConsumersPerSubscriptionWithContext returns the maxConsumersPerSubscription for a namespace.
	GetMaxConsumersPerSubscriptionWithContext(ctx context.Context, namespace utils.NameSpaceName) (int, error)

	// SetMaxConsumersPerTopic sets maxConsumersPerTopic for a namespace.
	//nolint: revive // It's ok here to use a built-in function name (max)
	SetMaxConsumersPerTopic(namespace utils.NameSpaceName, max int) error

	// SetMaxConsumersPerTopicWithContext sets maxConsumersPerTopic for a namespace.
	//nolint: revive // It's ok here to use a built-in function name (max)
	SetMaxConsumersPerTopicWithContext(ctx context.Context, namespace utils.NameSpaceName, max int) error

	// GetMaxConsumersPerTopic returns the maxProducersPerTopic for a namespace.
	GetMaxConsumersPerTopic(namespace utils.NameSpaceName) (int, error)

	// GetMaxConsumersPerTopicWithContext returns the maxProducersPerTopic for a namespace.
	GetMaxConsumersPerTopicWithContext(ctx context.Context, namespace utils.NameSpaceName) (int, error)

	// SetMaxProducersPerTopic sets maxProducersPerTopic for a namespace.
	//nolint: revive // It's ok here to use a built-in function name (max)
	SetMaxProducersPerTopic(namespace utils.NameSpaceName, max int) error

	// SetMaxProducersPerTopicWithContext sets maxProducersPerTopic for a namespace.
	//nolint: revive // It's ok here to use a built-in function name (max)
	SetMaxProducersPerTopicWithContext(ctx context.Context, namespace utils.NameSpaceName, max int) error

	// GetMaxProducersPerTopic returns the maxProducersPerTopic for a namespace.
	GetMaxProducersPerTopic(namespace utils.NameSpaceName) (int, error)

	// GetMaxProducersPerTopicWithContext returns the maxProducersPerTopic for a namespace.
	GetMaxProducersPerTopicWithContext(ctx context.Context, namespace utils.NameSpaceName) (int, error)

	// SetMaxTopicsPerNamespace sets maxTopicsPerNamespace for a namespace.
	//nolint: revive // It's ok here to use a built-in function name (max)
	SetMaxTopicsPerNamespace(namespace utils.NameSpaceName, max int) error

	// SetMaxTopicsPerNamespaceWithContext sets maxTopicsPerNamespace for a namespace.
	//nolint: revive // It's ok here to use a built-in function name (max)
	SetMaxTopicsPerNamespaceWithContext(ctx context.Context, namespace utils.NameSpaceName, max int) error

	// GetMaxTopicsPerNamespace returns the maxTopicsPerNamespace for a namespace.
	GetMaxTopicsPerNamespace(namespace utils.NameSpaceName) (int, error)

	// GetMaxTopicsPerNamespaceWithContext returns the maxTopicsPerNamespace for a namespace.
	GetMaxTopicsPerNamespaceWithContext(ctx context.Context, namespace utils.NameSpaceName) (int, error)

	// RemoveMaxTopicsPerNamespace removes maxTopicsPerNamespace configuration for a namespace,
	// defaulting to broker settings
	RemoveMaxTopicsPerNamespace(namespace utils.NameSpaceName) error

	// RemoveMaxTopicsPerNamespaceWithContext removes maxTopicsPerNamespace configuration for a namespace,
	// defaulting to broker settings
	RemoveMaxTopicsPerNamespaceWithContext(ctx context.Context, namespace utils.NameSpaceName) error

	// GetNamespaceReplicationClusters returns the replication clusters for a namespace
	GetNamespaceReplicationClusters(namespace string) ([]string, error)

	// GetNamespaceReplicationClustersWithContext returns the replication clusters for a namespace
	GetNamespaceReplicationClustersWithContext(ctx context.Context, namespace string) ([]string, error)

	// SetNamespaceReplicationClusters returns the replication clusters for a namespace
	SetNamespaceReplicationClusters(namespace string, clusterIDs []string) error

	// SetNamespaceReplicationClustersWithContext returns the replication clusters for a namespace
	SetNamespaceReplicationClustersWithContext(ctx context.Context, namespace string, clusterIDs []string) error

	// SetNamespaceAntiAffinityGroup sets anti-affinity group name for a namespace
	SetNamespaceAntiAffinityGroup(namespace string, namespaceAntiAffinityGroup string) error

	// SetNamespaceAntiAffinityGroupWithContext sets anti-affinity group name for a namespace
	SetNamespaceAntiAffinityGroupWithContext(
		ctx context.Context,
		namespace string,
		namespaceAntiAffinityGroup string,
	) error

	// GetAntiAffinityNamespaces returns all namespaces that grouped with given anti-affinity group
	GetAntiAffinityNamespaces(tenant, cluster, namespaceAntiAffinityGroup string) ([]string, error)

	// GetAntiAffinityNamespacesWithContext returns all namespaces that grouped with given anti-affinity group
	GetAntiAffinityNamespacesWithContext(
		ctx context.Context,
		tenant, cluster,
		namespaceAntiAffinityGroup string,
	) ([]string, error)

	// GetNamespaceAntiAffinityGroup returns anti-affinity group name for a namespace
	GetNamespaceAntiAffinityGroup(namespace string) (string, error)

	// GetNamespaceAntiAffinityGroupWithContext returns anti-affinity group name for a namespace
	GetNamespaceAntiAffinityGroupWithContext(ctx context.Context, namespace string) (string, error)

	// DeleteNamespaceAntiAffinityGroup deletes anti-affinity group name for a namespace
	DeleteNamespaceAntiAffinityGroup(namespace string) error

	// DeleteNamespaceAntiAffinityGroupWithContext deletes anti-affinity group name for a namespace
	DeleteNamespaceAntiAffinityGroupWithContext(ctx context.Context, namespace string) error

	// SetDeduplicationStatus sets the deduplication status for all topics within a namespace
	// When deduplication is enabled, the broker will prevent to store the same Message multiple times
	SetDeduplicationStatus(namespace string, enableDeduplication bool) error

	// SetDeduplicationStatusWithContext sets the deduplication status for all topics within a namespace
	// When deduplication is enabled, the broker will prevent to store the same Message multiple times
	SetDeduplicationStatusWithContext(ctx context.Context, namespace string, enableDeduplication bool) error

	// SetPersistence sets the persistence configuration for all the topics on a namespace
	SetPersistence(namespace string, persistence utils.PersistencePolicies) error

	// SetPersistenceWithContext sets the persistence configuration for all the topics on a namespace
	SetPersistenceWithContext(ctx context.Context, namespace string, persistence utils.PersistencePolicies) error

	// GetPersistence returns the persistence configuration for a namespace
	GetPersistence(namespace string) (*utils.PersistencePolicies, error)

	// GetPersistenceWithContext returns the persistence configuration for a namespace
	GetPersistenceWithContext(ctx context.Context, namespace string) (*utils.PersistencePolicies, error)

	// SetBookieAffinityGroup sets bookie affinity group for a namespace to isolate namespace write to bookies that are
	// part of given affinity group
	SetBookieAffinityGroup(namespace string, bookieAffinityGroup utils.BookieAffinityGroupData) error

	// SetBookieAffinityGroupWithContext sets bookie affinity group for a namespace
	// to isolate namespace write to bookies that are part of given affinity group
	SetBookieAffinityGroupWithContext(
		ctx context.Context,
		namespace string,
		bookieAffinityGroup utils.BookieAffinityGroupData,
	) error

	// DeleteBookieAffinityGroup deletes bookie affinity group configured for a namespace
	DeleteBookieAffinityGroup(namespace string) error

	// DeleteBookieAffinityGroupWithContext deletes bookie affinity group configured for a namespace
	DeleteBookieAffinityGroupWithContext(ctx context.Context, namespace string) error

	// GetBookieAffinityGroup returns bookie affinity group configured for a namespace
	GetBookieAffinityGroup(namespace string) (*utils.BookieAffinityGroupData, error)

	// GetBookieAffinityGroupWithContext returns bookie affinity group configured for a namespace
	GetBookieAffinityGroupWithContext(ctx context.Context, namespace string) (*utils.BookieAffinityGroupData, error)

	// Unload a namespace from the current serving broker
	Unload(namespace string) error

	// UnloadWithContext a namespace from the current serving broker
	UnloadWithContext(ctx context.Context, namespace string) error

	// UnloadNamespaceBundle unloads namespace bundle
	UnloadNamespaceBundle(namespace, bundle string) error

	// UnloadNamespaceBundleWithContext unloads namespace bundle
	UnloadNamespaceBundleWithContext(ctx context.Context, namespace, bundle string) error

	// SplitNamespaceBundle splits namespace bundle
	SplitNamespaceBundle(namespace, bundle string, unloadSplitBundles bool) error

	// SplitNamespaceBundleWithContext splits namespace bundle
	SplitNamespaceBundleWithContext(ctx context.Context, namespace, bundle string, unloadSplitBundles bool) error

	// GetNamespacePermissions returns permissions on a namespace
	GetNamespacePermissions(namespace utils.NameSpaceName) (map[string][]utils.AuthAction, error)

	// GetNamespacePermissionsWithContext returns permissions on a namespace
	GetNamespacePermissionsWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
	) (map[string][]utils.AuthAction, error)

	// GrantNamespacePermission grants permission on a namespace.
	GrantNamespacePermission(namespace utils.NameSpaceName, role string, action []utils.AuthAction) error

	// GrantNamespacePermissionWithContext grants permission on a namespace.
	GrantNamespacePermissionWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
		role string,
		action []utils.AuthAction,
	) error

	// RevokeNamespacePermission revokes permissions on a namespace.
	RevokeNamespacePermission(namespace utils.NameSpaceName, role string) error

	// RevokeNamespacePermissionWithContext revokes permissions on a namespace.
	RevokeNamespacePermissionWithContext(ctx context.Context, namespace utils.NameSpaceName, role string) error

	// GrantSubPermission grants permission to role to access subscription's admin-api
	GrantSubPermission(namespace utils.NameSpaceName, sName string, roles []string) error

	// GrantSubPermissionWithContext grants permission to role to access subscription's admin-api
	GrantSubPermissionWithContext(ctx context.Context, namespace utils.NameSpaceName, sName string, roles []string) error

	// RevokeSubPermission revoke permissions on a subscription's admin-api access
	RevokeSubPermission(namespace utils.NameSpaceName, sName, role string) error

	// RevokeSubPermissionWithContext revoke permissions on a subscription's admin-api access
	RevokeSubPermissionWithContext(ctx context.Context, namespace utils.NameSpaceName, sName, role string) error

	// GetSubPermissions returns subscription permissions on a namespace
	GetSubPermissions(namespace utils.NameSpaceName) (map[string][]string, error)

	// GetSubPermissionsWithContext returns subscription permissions on a namespace
	GetSubPermissionsWithContext(ctx context.Context, namespace utils.NameSpaceName) (map[string][]string, error)

	// SetSubscriptionAuthMode sets the given subscription auth mode on all topics on a namespace
	SetSubscriptionAuthMode(namespace utils.NameSpaceName, mode utils.SubscriptionAuthMode) error

	// SetSubscriptionAuthModeWithContext sets the given subscription auth mode on all topics on a namespace
	SetSubscriptionAuthModeWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
		mode utils.SubscriptionAuthMode,
	) error

	// SetEncryptionRequiredStatus sets the encryption required status for all topics within a namespace
	SetEncryptionRequiredStatus(namespace utils.NameSpaceName, encrypt bool) error

	// SetEncryptionRequiredStatusWithContext sets the encryption required status for all topics within a namespace
	SetEncryptionRequiredStatusWithContext(ctx context.Context, namespace utils.NameSpaceName, encrypt bool) error

	// UnsubscribeNamespace unsubscribe the given subscription on all topics on a namespace
	UnsubscribeNamespace(namespace utils.NameSpaceName, sName string) error

	// UnsubscribeNamespaceWithContext unsubscribe the given subscription on all topics on a namespace
	UnsubscribeNamespaceWithContext(ctx context.Context, namespace utils.NameSpaceName, sName string) error

	// UnsubscribeNamespaceBundle unsubscribe the given subscription on all topics on a namespace bundle
	UnsubscribeNamespaceBundle(namespace utils.NameSpaceName, bundle, sName string) error

	// UnsubscribeNamespaceBundleWithContext unsubscribe the given subscription on all topics on a namespace bundle
	UnsubscribeNamespaceBundleWithContext(ctx context.Context, namespace utils.NameSpaceName, bundle, sName string) error

	// ClearNamespaceBundleBacklogForSubscription clears backlog for a given subscription on all
	// topics on a namespace bundle
	ClearNamespaceBundleBacklogForSubscription(namespace utils.NameSpaceName, bundle, sName string) error

	// ClearNamespaceBundleBacklogForSubscriptionWithContext clears backlog for a given subscription on all
	// topics on a namespace bundle
	ClearNamespaceBundleBacklogForSubscriptionWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
		bundle,
		sName string,
	) error

	// ClearNamespaceBundleBacklog clears backlog for all topics on a namespace bundle
	ClearNamespaceBundleBacklog(namespace utils.NameSpaceName, bundle string) error

	// ClearNamespaceBundleBacklogWithContext clears backlog for all topics on a namespace bundle
	ClearNamespaceBundleBacklogWithContext(ctx context.Context, namespace utils.NameSpaceName, bundle string) error

	// ClearNamespaceBacklogForSubscription clears backlog for a given subscription on all topics on a namespace
	ClearNamespaceBacklogForSubscription(namespace utils.NameSpaceName, sName string) error

	// ClearNamespaceBacklogForSubscriptionWithContext clears backlog for a given subscription on all topics on a namespace
	ClearNamespaceBacklogForSubscriptionWithContext(ctx context.Context, namespace utils.NameSpaceName, sName string) error

	// ClearNamespaceBacklog clears backlog for all topics on a namespace
	ClearNamespaceBacklog(namespace utils.NameSpaceName) error

	// ClearNamespaceBacklogWithContext clears backlog for all topics on a namespace
	ClearNamespaceBacklogWithContext(ctx context.Context, namespace utils.NameSpaceName) error

	// SetReplicatorDispatchRate sets replicator-Message-dispatch-rate (Replicators under this namespace
	// can dispatch this many messages per second)
	SetReplicatorDispatchRate(namespace utils.NameSpaceName, rate utils.DispatchRate) error

	// SetReplicatorDispatchRateWithContext sets replicator-Message-dispatch-rate (Replicators under this namespace
	// can dispatch this many messages per second)
	SetReplicatorDispatchRateWithContext(ctx context.Context, namespace utils.NameSpaceName, rate utils.DispatchRate) error

	// GetReplicatorDispatchRate returns replicator-Message-dispatch-rate (Replicators under this namespace
	// can dispatch this many messages per second)
	GetReplicatorDispatchRate(namespace utils.NameSpaceName) (utils.DispatchRate, error)

	// GetReplicatorDispatchRateWithContext returns replicator-Message-dispatch-rate (Replicators under this namespace
	// can dispatch this many messages per second)
	GetReplicatorDispatchRateWithContext(ctx context.Context, namespace utils.NameSpaceName) (utils.DispatchRate, error)

	// SetSubscriptionDispatchRate sets subscription-Message-dispatch-rate (subscriptions under this namespace
	// can dispatch this many messages per second)
	SetSubscriptionDispatchRate(namespace utils.NameSpaceName, rate utils.DispatchRate) error

	// SetSubscriptionDispatchRateWithContext sets subscription-Message-dispatch-rate (subscriptions under this namespace
	// can dispatch this many messages per second)
	SetSubscriptionDispatchRateWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
		rate utils.DispatchRate,
	) error

	// GetSubscriptionDispatchRate returns subscription-Message-dispatch-rate (subscriptions under this namespace
	// can dispatch this many messages per second)
	GetSubscriptionDispatchRate(namespace utils.NameSpaceName) (utils.DispatchRate, error)

	// GetSubscriptionDispatchRateWithContext returns subscription-Message-dispatch-rate
	// (subscriptions under this namespace can dispatch this many messages per second)
	GetSubscriptionDispatchRateWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
	) (utils.DispatchRate, error)

	// SetSubscribeRate sets namespace-subscribe-rate (topics under this namespace will limit by subscribeRate)
	SetSubscribeRate(namespace utils.NameSpaceName, rate utils.SubscribeRate) error

	// SetSubscribeRateWithContext sets namespace-subscribe-rate (topics under this namespace will limit by subscribeRate)
	SetSubscribeRateWithContext(ctx context.Context, namespace utils.NameSpaceName, rate utils.SubscribeRate) error

	// GetSubscribeRate returns namespace-subscribe-rate (topics under this namespace allow subscribe
	// times per consumer in a period)
	GetSubscribeRate(namespace utils.NameSpaceName) (utils.SubscribeRate, error)

	// GetSubscribeRateWithContext returns namespace-subscribe-rate (topics under this namespace allow subscribe
	// times per consumer in a period)
	GetSubscribeRateWithContext(ctx context.Context, namespace utils.NameSpaceName) (utils.SubscribeRate, error)

	// SetDispatchRate sets Message-dispatch-rate (topics under this namespace can dispatch
	// this many messages per second)
	SetDispatchRate(namespace utils.NameSpaceName, rate utils.DispatchRate) error

	// SetDispatchRateWithContext sets Message-dispatch-rate (topics under this namespace can dispatch
	// this many messages per second)
	SetDispatchRateWithContext(ctx context.Context, namespace utils.NameSpaceName, rate utils.DispatchRate) error

	// GetDispatchRate returns Message-dispatch-rate (topics under this namespace can dispatch
	// this many messages per second)
	GetDispatchRate(namespace utils.NameSpaceName) (utils.DispatchRate, error)

	// GetDispatchRateWithContext returns Message-dispatch-rate (topics under this namespace can dispatch
	// this many messages per second)
	GetDispatchRateWithContext(ctx context.Context, namespace utils.NameSpaceName) (utils.DispatchRate, error)

	// SetPublishRate sets the maximum rate or number of messages that producers can publish to topics in this namespace
	SetPublishRate(namespace utils.NameSpaceName, pubRate utils.PublishRate) error

	// SetPublishRateWithContext sets the maximum rate
	// or number of messages that producers can publish to topics in this namespace
	SetPublishRateWithContext(ctx context.Context, namespace utils.NameSpaceName, pubRate utils.PublishRate) error

	// GetPublishRate gets the maximum rate or number of messages that producer can publish to topics in the namespace
	GetPublishRate(namespace utils.NameSpaceName) (utils.PublishRate, error)

	// GetPublishRateWithContext gets the maximum rate
	// or number of messages that producer can publish to topics in the namespace
	GetPublishRateWithContext(ctx context.Context, namespace utils.NameSpaceName) (utils.PublishRate, error)

	// SetIsAllowAutoUpdateSchema sets whether to allow auto update schema on a namespace
	SetIsAllowAutoUpdateSchema(namespace utils.NameSpaceName, isAllowAutoUpdateSchema bool) error

	// SetIsAllowAutoUpdateSchemaWithContext sets whether to allow auto update schema on a namespace
	SetIsAllowAutoUpdateSchemaWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
		isAllowAutoUpdateSchema bool,
	) error

	// GetIsAllowAutoUpdateSchema gets whether to allow auto update schema on a namespace
	GetIsAllowAutoUpdateSchema(namespace utils.NameSpaceName) (bool, error)

	// GetIsAllowAutoUpdateSchemaWithContext gets whether to allow auto update schema on a namespace
	GetIsAllowAutoUpdateSchemaWithContext(ctx context.Context, namespace utils.NameSpaceName) (bool, error)

	// GetInactiveTopicPolicies gets the inactive topic policies on a namespace
	GetInactiveTopicPolicies(namespace utils.NameSpaceName) (utils.InactiveTopicPolicies, error)

	// GetInactiveTopicPoliciesWithContext gets the inactive topic policies on a namespace
	GetInactiveTopicPoliciesWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
	) (utils.InactiveTopicPolicies, error)

	// RemoveInactiveTopicPolicies removes inactive topic policies from a namespace
	RemoveInactiveTopicPolicies(namespace utils.NameSpaceName) error

	// RemoveInactiveTopicPoliciesWithContext removes inactive topic policies from a namespace
	RemoveInactiveTopicPoliciesWithContext(ctx context.Context, namespace utils.NameSpaceName) error

	// SetInactiveTopicPolicies sets the inactive topic policies on a namespace
	SetInactiveTopicPolicies(namespace utils.NameSpaceName, data utils.InactiveTopicPolicies) error

	// SetInactiveTopicPoliciesWithContext sets the inactive topic policies on a namespace
	SetInactiveTopicPoliciesWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
		data utils.InactiveTopicPolicies,
	) error

	// GetSubscriptionExpirationTime gets the subscription expiration time on a namespace. Returns -1 if not set
	GetSubscriptionExpirationTime(namespace utils.NameSpaceName) (int, error)

	// GetSubscriptionExpirationTimeWithContext gets the subscription expiration time on a namespace. Returns -1 if not set
	GetSubscriptionExpirationTimeWithContext(ctx context.Context, namespace utils.NameSpaceName) (int, error)

	// SetSubscriptionExpirationTime sets the subscription expiration time on a namespace
	SetSubscriptionExpirationTime(namespace utils.NameSpaceName, expirationTimeInMinutes int) error

	// SetSubscriptionExpirationTimeWithContext sets the subscription expiration time on a namespace
	SetSubscriptionExpirationTimeWithContext(
		ctx context.Context,
		namespace utils.NameSpaceName,
		expirationTimeInMinutes int,
	) error

	// RemoveSubscriptionExpirationTime removes subscription expiration time from a namespace,
	// defaulting to broker settings
	RemoveSubscriptionExpirationTime(namespace utils.NameSpaceName) error

	// RemoveSubscriptionExpirationTimeWithContext removes subscription expiration time from a namespace,
	// defaulting to broker settings
	RemoveSubscriptionExpirationTimeWithContext(ctx context.Context, namespace utils.NameSpaceName) error

	// UpdateProperties updates the properties of a namespace
	UpdateProperties(namespace utils.NameSpaceName, properties map[string]string) error

	// UpdatePropertiesWithContext updates the properties of a namespace
	UpdatePropertiesWithContext(ctx context.Context, namespace utils.NameSpaceName, properties map[string]string) error

	// GetProperties returns the properties of a namespace
	GetProperties(namespace utils.NameSpaceName) (map[string]string, error)

	// GetPropertiesWithContext returns the properties of a namespace
	GetPropertiesWithContext(ctx context.Context, namespace utils.NameSpaceName) (map[string]string, error)

	// RemoveProperties clears the properties of a namespace
	RemoveProperties(namespace utils.NameSpaceName) error

	// RemovePropertiesWithContext clears the properties of a namespace
	RemovePropertiesWithContext(ctx context.Context, namespace utils.NameSpaceName) error
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
	return n.GetNamespacesWithContext(context.Background(), tenant)
}

func (n *namespaces) GetNamespacesWithContext(ctx context.Context, tenant string) ([]string, error) {
	var namespaces []string
	endpoint := n.pulsar.endpoint(n.basePath, tenant)
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &namespaces)
	return namespaces, err
}

func (n *namespaces) GetTopics(namespace string) ([]string, error) {
	return n.GetTopicsWithContext(context.Background(), namespace)
}

func (n *namespaces) GetTopicsWithContext(ctx context.Context, namespace string) ([]string, error) {
	var topics []string
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String(), "topics")
	err = n.pulsar.Client.GetWithContext(ctx, endpoint, &topics)
	return topics, err
}

func (n *namespaces) GetPolicies(namespace string) (*utils.Policies, error) {
	return n.GetPoliciesWithContext(context.Background(), namespace)
}

func (n *namespaces) GetPoliciesWithContext(ctx context.Context, namespace string) (*utils.Policies, error) {
	var police utils.Policies
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String())
	err = n.pulsar.Client.GetWithContext(ctx, endpoint, &police)
	return &police, err
}

func (n *namespaces) CreateNsWithNumBundles(namespace string, numBundles int) error {
	return n.CreateNsWithNumBundlesWithContext(context.Background(), namespace, numBundles)
}

func (n *namespaces) CreateNsWithNumBundlesWithContext(ctx context.Context, namespace string, numBundles int) error {
	return n.CreateNsWithBundlesDataWithContext(ctx, namespace, utils.NewBundlesDataWithNumBundles(numBundles))
}

func (n *namespaces) CreateNsWithPolices(namespace string, policies utils.Policies) error {
	return n.CreateNsWithPolicesWithContext(context.Background(), namespace, policies)
}

func (n *namespaces) CreateNsWithPolicesWithContext(
	ctx context.Context,
	namespace string,
	policies utils.Policies,
) error {
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String())
	return n.pulsar.Client.PutWithContext(ctx, endpoint, &policies)
}

func (n *namespaces) CreateNsWithBundlesData(namespace string, bundleData *utils.BundlesData) error {
	return n.CreateNsWithBundlesDataWithContext(context.Background(), namespace, bundleData)
}

func (n *namespaces) CreateNsWithBundlesDataWithContext(
	ctx context.Context,
	namespace string,
	bundleData *utils.BundlesData,
) error {
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String())
	polices := new(utils.Policies)
	polices.Bundles = bundleData

	return n.pulsar.Client.PutWithContext(ctx, endpoint, &polices)
}

func (n *namespaces) CreateNamespace(namespace string) error {
	return n.CreateNamespaceWithContext(context.Background(), namespace)
}

func (n *namespaces) CreateNamespaceWithContext(ctx context.Context, namespace string) error {
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String())
	return n.pulsar.Client.PutWithContext(ctx, endpoint, nil)
}

func (n *namespaces) DeleteNamespace(namespace string) error {
	return n.DeleteNamespaceWithContext(context.Background(), namespace)
}

func (n *namespaces) DeleteNamespaceWithContext(ctx context.Context, namespace string) error {
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String())
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (n *namespaces) DeleteNamespaceBundle(namespace string, bundleRange string) error {
	return n.DeleteNamespaceBundleWithContext(context.Background(), namespace, bundleRange)
}

func (n *namespaces) DeleteNamespaceBundleWithContext(ctx context.Context, namespace string, bundleRange string) error {
	ns, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, ns.String(), bundleRange)
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (n *namespaces) GetNamespaceMessageTTL(namespace string) (int, error) {
	return n.GetNamespaceMessageTTLWithContext(context.Background(), namespace)
}

func (n *namespaces) GetNamespaceMessageTTLWithContext(ctx context.Context, namespace string) (int, error) {
	var ttl int
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return 0, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "messageTTL")
	err = n.pulsar.Client.GetWithContext(ctx, endpoint, &ttl)
	return ttl, err
}

func (n *namespaces) SetNamespaceMessageTTL(namespace string, ttlInSeconds int) error {
	return n.SetNamespaceMessageTTLWithContext(context.Background(), namespace, ttlInSeconds)
}

func (n *namespaces) SetNamespaceMessageTTLWithContext(ctx context.Context, namespace string, ttlInSeconds int) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}

	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "messageTTL")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, &ttlInSeconds)
}

func (n *namespaces) SetRetention(namespace string, policy utils.RetentionPolicies) error {
	return n.SetRetentionWithContext(context.Background(), namespace, policy)
}

func (n *namespaces) SetRetentionWithContext(
	ctx context.Context,
	namespace string,
	policy utils.RetentionPolicies,
) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "retention")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, &policy)
}

func (n *namespaces) GetRetention(namespace string) (*utils.RetentionPolicies, error) {
	return n.GetRetentionWithContext(context.Background(), namespace)
}

func (n *namespaces) GetRetentionWithContext(ctx context.Context, namespace string) (*utils.RetentionPolicies, error) {
	var policy utils.RetentionPolicies
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "retention")
	err = n.pulsar.Client.GetWithContext(ctx, endpoint, &policy)
	return &policy, err
}

func (n *namespaces) GetBacklogQuotaMap(namespace string) (map[utils.BacklogQuotaType]utils.BacklogQuota, error) {
	return n.GetBacklogQuotaMapWithContext(context.Background(), namespace)
}

func (n *namespaces) GetBacklogQuotaMapWithContext(
	ctx context.Context,
	namespace string,
) (map[utils.BacklogQuotaType]utils.BacklogQuota, error) {
	var backlogQuotaMap map[utils.BacklogQuotaType]utils.BacklogQuota
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "backlogQuotaMap")
	err = n.pulsar.Client.GetWithContext(ctx, endpoint, &backlogQuotaMap)
	return backlogQuotaMap, err
}

func (n *namespaces) SetBacklogQuota(namespace string, backlogQuota utils.BacklogQuota,
	backlogQuotaType utils.BacklogQuotaType) error {
	return n.SetBacklogQuotaWithContext(context.Background(), namespace, backlogQuota, backlogQuotaType)
}

func (n *namespaces) SetBacklogQuotaWithContext(ctx context.Context, namespace string, backlogQuota utils.BacklogQuota,
	backlogQuotaType utils.BacklogQuotaType) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "backlogQuota")
	params := make(map[string]string)
	params["backlogQuotaType"] = string(backlogQuotaType)
	return n.pulsar.Client.PostWithQueryParamsWithContext(ctx, endpoint, &backlogQuota, params)
}

func (n *namespaces) RemoveBacklogQuota(namespace string) error {
	return n.RemoveBacklogQuotaWithContext(context.Background(), namespace)
}

func (n *namespaces) RemoveBacklogQuotaWithContext(ctx context.Context, namespace string) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "backlogQuota")
	params := map[string]string{
		"backlogQuotaType": string(utils.DestinationStorage),
	}
	return n.pulsar.Client.DeleteWithQueryParamsWithContext(ctx, endpoint, params)
}

func (n *namespaces) GetTopicAutoCreation(namespace utils.NameSpaceName) (*utils.TopicAutoCreationConfig, error) {
	return n.GetTopicAutoCreationWithContext(context.Background(), namespace)
}

func (n *namespaces) GetTopicAutoCreationWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (*utils.TopicAutoCreationConfig, error) {
	var topicAutoCreation utils.TopicAutoCreationConfig
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "autoTopicCreation")
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &topicAutoCreation)
	return &topicAutoCreation, err
}

func (n *namespaces) SetTopicAutoCreation(namespace utils.NameSpaceName, config utils.TopicAutoCreationConfig) error {
	return n.SetTopicAutoCreationWithContext(context.Background(), namespace, config)
}

func (n *namespaces) SetTopicAutoCreationWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	config utils.TopicAutoCreationConfig,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "autoTopicCreation")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, &config)
}

func (n *namespaces) RemoveTopicAutoCreation(namespace utils.NameSpaceName) error {
	return n.RemoveTopicAutoCreationWithContext(context.Background(), namespace)
}

func (n *namespaces) RemoveTopicAutoCreationWithContext(ctx context.Context, namespace utils.NameSpaceName) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "autoTopicCreation")
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (n *namespaces) SetSchemaValidationEnforced(namespace utils.NameSpaceName, schemaValidationEnforced bool) error {
	return n.SetSchemaValidationEnforcedWithContext(context.Background(), namespace, schemaValidationEnforced)
}

func (n *namespaces) SetSchemaValidationEnforcedWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	schemaValidationEnforced bool,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "schemaValidationEnforced")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, schemaValidationEnforced)
}

func (n *namespaces) GetSchemaValidationEnforced(namespace utils.NameSpaceName) (bool, error) {
	return n.GetSchemaValidationEnforcedWithContext(context.Background(), namespace)
}

func (n *namespaces) GetSchemaValidationEnforcedWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (bool, error) {
	var result bool
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "schemaValidationEnforced")
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &result)
	return result, err
}

func (n *namespaces) SetSchemaAutoUpdateCompatibilityStrategy(namespace utils.NameSpaceName,
	strategy utils.SchemaAutoUpdateCompatibilityStrategy) error {
	return n.SetSchemaAutoUpdateCompatibilityStrategyWithContext(context.Background(), namespace, strategy)
}

func (n *namespaces) SetSchemaAutoUpdateCompatibilityStrategyWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	strategy utils.SchemaAutoUpdateCompatibilityStrategy,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "schemaAutoUpdateCompatibilityStrategy")
	return n.pulsar.Client.PutWithContext(ctx, endpoint, strategy.String())
}

func (n *namespaces) GetSchemaAutoUpdateCompatibilityStrategy(namespace utils.NameSpaceName) (
	utils.SchemaAutoUpdateCompatibilityStrategy, error) {
	return n.GetSchemaAutoUpdateCompatibilityStrategyWithContext(context.Background(), namespace)
}

func (n *namespaces) GetSchemaAutoUpdateCompatibilityStrategyWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (
	utils.SchemaAutoUpdateCompatibilityStrategy, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "schemaAutoUpdateCompatibilityStrategy")
	b, err := n.pulsar.Client.GetWithQueryParamsWithContext(ctx, endpoint, nil, nil, false)
	if err != nil {
		return "", err
	}
	s, err := utils.ParseSchemaAutoUpdateCompatibilityStrategy(strings.ReplaceAll(string(b), "\"", ""))
	if err != nil {
		return "", err
	}
	return s, nil
}

func (n *namespaces) SetSchemaCompatibilityStrategy(namespace utils.NameSpaceName,
	strategy utils.SchemaCompatibilityStrategy) error {
	return n.SetSchemaCompatibilityStrategyWithContext(context.Background(), namespace, strategy)
}

func (n *namespaces) SetSchemaCompatibilityStrategyWithContext(ctx context.Context, namespace utils.NameSpaceName,
	strategy utils.SchemaCompatibilityStrategy) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "schemaCompatibilityStrategy")
	return n.pulsar.Client.PutWithContext(ctx, endpoint, strategy.String())
}

func (n *namespaces) GetSchemaCompatibilityStrategy(namespace utils.NameSpaceName) (
	utils.SchemaCompatibilityStrategy, error) {
	return n.GetSchemaCompatibilityStrategyWithContext(context.Background(), namespace)
}

func (n *namespaces) GetSchemaCompatibilityStrategyWithContext(ctx context.Context, namespace utils.NameSpaceName) (
	utils.SchemaCompatibilityStrategy, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "schemaCompatibilityStrategy")
	b, err := n.pulsar.Client.GetWithQueryParamsWithContext(ctx, endpoint, nil, nil, false)
	if err != nil {
		return "", err
	}
	s, err := utils.ParseSchemaCompatibilityStrategy(strings.ReplaceAll(string(b), "\"", ""))
	if err != nil {
		return "", err
	}
	return s, nil
}

func (n *namespaces) ClearOffloadDeleteLag(namespace utils.NameSpaceName) error {
	return n.ClearOffloadDeleteLagWithContext(context.Background(), namespace)
}

func (n *namespaces) ClearOffloadDeleteLagWithContext(ctx context.Context, namespace utils.NameSpaceName) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "offloadDeletionLagMs")
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (n *namespaces) SetOffloadDeleteLag(namespace utils.NameSpaceName, timeMs int64) error {
	return n.SetOffloadDeleteLagWithContext(context.Background(), namespace, timeMs)
}

func (n *namespaces) SetOffloadDeleteLagWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	timeMs int64,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "offloadDeletionLagMs")
	return n.pulsar.Client.PutWithContext(ctx, endpoint, timeMs)
}

func (n *namespaces) GetOffloadDeleteLag(namespace utils.NameSpaceName) (int64, error) {
	return n.GetOffloadDeleteLagWithContext(context.Background(), namespace)
}

func (n *namespaces) GetOffloadDeleteLagWithContext(ctx context.Context, namespace utils.NameSpaceName) (int64, error) {
	var result int64
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "offloadDeletionLagMs")
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &result)
	return result, err
}

// nolint: revive // It's ok here to use a built-in function name (max)
func (n *namespaces) SetMaxConsumersPerSubscription(namespace utils.NameSpaceName, max int) error {
	return n.SetMaxConsumersPerSubscriptionWithContext(context.Background(), namespace, max)
}

// nolint: revive // It's ok here to use a built-in function name (max)
func (n *namespaces) SetMaxConsumersPerSubscriptionWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	max int,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxConsumersPerSubscription")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, max)
}

func (n *namespaces) GetMaxConsumersPerSubscription(namespace utils.NameSpaceName) (int, error) {
	return n.GetMaxConsumersPerSubscriptionWithContext(context.Background(), namespace)
}

func (n *namespaces) GetMaxConsumersPerSubscriptionWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (int, error) {
	var result int
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxConsumersPerSubscription")
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &result)
	return result, err
}

func (n *namespaces) SetOffloadThreshold(namespace utils.NameSpaceName, threshold int64) error {
	return n.SetOffloadThresholdWithContext(context.Background(), namespace, threshold)
}

func (n *namespaces) SetOffloadThresholdWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	threshold int64,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "offloadThreshold")
	return n.pulsar.Client.PutWithContext(ctx, endpoint, threshold)
}

func (n *namespaces) GetOffloadThreshold(namespace utils.NameSpaceName) (int64, error) {
	return n.GetOffloadThresholdWithContext(context.Background(), namespace)
}

func (n *namespaces) GetOffloadThresholdWithContext(ctx context.Context, namespace utils.NameSpaceName) (int64, error) {
	var result int64
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "offloadThreshold")
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &result)
	return result, err
}

func (n *namespaces) SetOffloadThresholdInSeconds(namespace utils.NameSpaceName, threshold int64) error {
	return n.SetOffloadThresholdInSecondsWithContext(context.Background(), namespace, threshold)
}

func (n *namespaces) SetOffloadThresholdInSecondsWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	threshold int64,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "offloadThresholdInSeconds")
	return n.pulsar.Client.PutWithContext(ctx, endpoint, threshold)
}

func (n *namespaces) GetOffloadThresholdInSeconds(namespace utils.NameSpaceName) (int64, error) {
	return n.GetOffloadThresholdInSecondsWithContext(context.Background(), namespace)
}

func (n *namespaces) GetOffloadThresholdInSecondsWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (int64, error) {
	var result int64
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "offloadThresholdInSeconds")
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &result)
	return result, err
}

// nolint: revive // It's ok here to use a built-in function name (max)
func (n *namespaces) SetMaxConsumersPerTopic(namespace utils.NameSpaceName, max int) error {
	return n.SetMaxConsumersPerTopicWithContext(context.Background(), namespace, max)
}

// nolint: revive // It's ok here to use a built-in function name (max)
func (n *namespaces) SetMaxConsumersPerTopicWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	max int,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxConsumersPerTopic")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, max)
}

func (n *namespaces) GetMaxConsumersPerTopic(namespace utils.NameSpaceName) (int, error) {
	return n.GetMaxConsumersPerTopicWithContext(context.Background(), namespace)
}

func (n *namespaces) GetMaxConsumersPerTopicWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (int, error) {
	var result int
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxConsumersPerTopic")
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &result)
	return result, err
}

func (n *namespaces) SetCompactionThreshold(namespace utils.NameSpaceName, threshold int64) error {
	return n.SetCompactionThresholdWithContext(context.Background(), namespace, threshold)
}

func (n *namespaces) SetCompactionThresholdWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	threshold int64,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "compactionThreshold")
	return n.pulsar.Client.PutWithContext(ctx, endpoint, threshold)
}

func (n *namespaces) GetCompactionThreshold(namespace utils.NameSpaceName) (int64, error) {
	return n.GetCompactionThresholdWithContext(context.Background(), namespace)
}

func (n *namespaces) GetCompactionThresholdWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (int64, error) {
	var result int64
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "compactionThreshold")
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &result)
	return result, err
}

// nolint: revive // It's ok here to use a built-in function name (max)
func (n *namespaces) SetMaxProducersPerTopic(namespace utils.NameSpaceName, max int) error {
	return n.SetMaxProducersPerTopicWithContext(context.Background(), namespace, max)
}

// nolint: revive // It's ok here to use a built-in function name (max)
func (n *namespaces) SetMaxProducersPerTopicWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	max int,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxProducersPerTopic")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, max)
}

func (n *namespaces) GetMaxProducersPerTopic(namespace utils.NameSpaceName) (int, error) {
	return n.GetMaxProducersPerTopicWithContext(context.Background(), namespace)
}

func (n *namespaces) GetMaxProducersPerTopicWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (int, error) {
	var result int
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxProducersPerTopic")
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &result)
	return result, err
}

func (n *namespaces) GetNamespaceReplicationClusters(namespace string) ([]string, error) {
	return n.GetNamespaceReplicationClustersWithContext(context.Background(), namespace)
}

func (n *namespaces) GetNamespaceReplicationClustersWithContext(
	ctx context.Context,
	namespace string,
) ([]string, error) {
	var data []string
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "replication")
	err = n.pulsar.Client.GetWithContext(ctx, endpoint, &data)
	return data, err
}

func (n *namespaces) SetNamespaceReplicationClusters(namespace string, clusterIDs []string) error {
	return n.SetNamespaceReplicationClustersWithContext(context.Background(), namespace, clusterIDs)
}

func (n *namespaces) SetNamespaceReplicationClustersWithContext(
	ctx context.Context,
	namespace string,
	clusterIDs []string,
) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "replication")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, &clusterIDs)
}

func (n *namespaces) SetNamespaceAntiAffinityGroup(namespace string, namespaceAntiAffinityGroup string) error {
	return n.SetNamespaceAntiAffinityGroupWithContext(context.Background(), namespace, namespaceAntiAffinityGroup)
}

func (n *namespaces) SetNamespaceAntiAffinityGroupWithContext(
	ctx context.Context,
	namespace string,
	namespaceAntiAffinityGroup string,
) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "antiAffinity")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, namespaceAntiAffinityGroup)
}

func (n *namespaces) GetAntiAffinityNamespaces(tenant, cluster, namespaceAntiAffinityGroup string) ([]string, error) {
	return n.GetAntiAffinityNamespacesWithContext(context.Background(), tenant, cluster, namespaceAntiAffinityGroup)
}

func (n *namespaces) GetAntiAffinityNamespacesWithContext(
	ctx context.Context,
	tenant,
	cluster,
	namespaceAntiAffinityGroup string,
) ([]string, error) {
	var data []string
	endpoint := n.pulsar.endpoint(n.basePath, cluster, "antiAffinity", namespaceAntiAffinityGroup)
	params := map[string]string{
		"property": tenant,
	}
	_, err := n.pulsar.Client.GetWithQueryParamsWithContext(ctx, endpoint, &data, params, false)
	return data, err
}

func (n *namespaces) GetNamespaceAntiAffinityGroup(namespace string) (string, error) {
	return n.GetNamespaceAntiAffinityGroupWithContext(context.Background(), namespace)
}

func (n *namespaces) GetNamespaceAntiAffinityGroupWithContext(ctx context.Context, namespace string) (string, error) {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return "", err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "antiAffinity")
	data, err := n.pulsar.Client.GetWithQueryParamsWithContext(ctx, endpoint, nil, nil, false)
	return string(data), err
}

func (n *namespaces) DeleteNamespaceAntiAffinityGroup(namespace string) error {
	return n.DeleteNamespaceAntiAffinityGroupWithContext(context.Background(), namespace)
}

func (n *namespaces) DeleteNamespaceAntiAffinityGroupWithContext(ctx context.Context, namespace string) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "antiAffinity")
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (n *namespaces) SetDeduplicationStatus(namespace string, enableDeduplication bool) error {
	return n.SetDeduplicationStatusWithContext(context.Background(), namespace, enableDeduplication)
}

func (n *namespaces) SetDeduplicationStatusWithContext(
	ctx context.Context,
	namespace string,
	enableDeduplication bool,
) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "deduplication")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, enableDeduplication)
}

func (n *namespaces) SetPersistence(namespace string, persistence utils.PersistencePolicies) error {
	return n.SetPersistenceWithContext(context.Background(), namespace, persistence)
}

func (n *namespaces) SetPersistenceWithContext(
	ctx context.Context,
	namespace string,
	persistence utils.PersistencePolicies,
) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "persistence")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, &persistence)
}

func (n *namespaces) SetBookieAffinityGroup(namespace string, bookieAffinityGroup utils.BookieAffinityGroupData) error {
	return n.SetBookieAffinityGroupWithContext(context.Background(), namespace, bookieAffinityGroup)
}

func (n *namespaces) SetBookieAffinityGroupWithContext(
	ctx context.Context,
	namespace string,
	bookieAffinityGroup utils.BookieAffinityGroupData,
) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "persistence", "bookieAffinity")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, &bookieAffinityGroup)
}

func (n *namespaces) DeleteBookieAffinityGroup(namespace string) error {
	return n.DeleteBookieAffinityGroupWithContext(context.Background(), namespace)
}

func (n *namespaces) DeleteBookieAffinityGroupWithContext(ctx context.Context, namespace string) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "persistence", "bookieAffinity")
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (n *namespaces) GetBookieAffinityGroup(namespace string) (*utils.BookieAffinityGroupData, error) {
	return n.GetBookieAffinityGroupWithContext(context.Background(), namespace)
}

func (n *namespaces) GetBookieAffinityGroupWithContext(
	ctx context.Context,
	namespace string,
) (*utils.BookieAffinityGroupData, error) {
	var data utils.BookieAffinityGroupData
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "persistence", "bookieAffinity")
	err = n.pulsar.Client.GetWithContext(ctx, endpoint, &data)
	return &data, err
}

func (n *namespaces) GetPersistence(namespace string) (*utils.PersistencePolicies, error) {
	return n.GetPersistenceWithContext(context.Background(), namespace)
}

func (n *namespaces) GetPersistenceWithContext(
	ctx context.Context,
	namespace string,
) (*utils.PersistencePolicies, error) {
	var persistence utils.PersistencePolicies
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "persistence")
	err = n.pulsar.Client.GetWithContext(ctx, endpoint, &persistence)
	return &persistence, err
}

func (n *namespaces) Unload(namespace string) error {
	return n.UnloadWithContext(context.Background(), namespace)
}

func (n *namespaces) UnloadWithContext(ctx context.Context, namespace string) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), "unload")
	return n.pulsar.Client.PutWithContext(ctx, endpoint, nil)
}

func (n *namespaces) UnloadNamespaceBundle(namespace, bundle string) error {
	return n.UnloadNamespaceBundleWithContext(context.Background(), namespace, bundle)
}

func (n *namespaces) UnloadNamespaceBundleWithContext(ctx context.Context, namespace, bundle string) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), bundle, "unload")
	return n.pulsar.Client.PutWithContext(ctx, endpoint, nil)
}

func (n *namespaces) SplitNamespaceBundle(namespace, bundle string, unloadSplitBundles bool) error {
	return n.SplitNamespaceBundleWithContext(context.Background(), namespace, bundle, unloadSplitBundles)
}

func (n *namespaces) SplitNamespaceBundleWithContext(
	ctx context.Context,
	namespace,
	bundle string,
	unloadSplitBundles bool,
) error {
	nsName, err := utils.GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.pulsar.endpoint(n.basePath, nsName.String(), bundle, "split")
	params := map[string]string{
		"unload": strconv.FormatBool(unloadSplitBundles),
	}
	return n.pulsar.Client.PutWithQueryParamsWithContext(ctx, endpoint, nil, nil, params)
}

func (n *namespaces) GetNamespacePermissions(namespace utils.NameSpaceName) (map[string][]utils.AuthAction, error) {
	return n.GetNamespacePermissionsWithContext(context.Background(), namespace)
}

func (n *namespaces) GetNamespacePermissionsWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (map[string][]utils.AuthAction, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "permissions")
	var permissions map[string][]utils.AuthAction
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &permissions)
	return permissions, err
}

func (n *namespaces) GrantNamespacePermission(namespace utils.NameSpaceName, role string,
	action []utils.AuthAction) error {
	return n.GrantNamespacePermissionWithContext(context.Background(), namespace, role, action)
}

func (n *namespaces) GrantNamespacePermissionWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	role string,
	action []utils.AuthAction) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "permissions", role)
	s := make([]string, 0)
	for _, v := range action {
		s = append(s, v.String())
	}
	return n.pulsar.Client.PostWithContext(ctx, endpoint, s)
}

func (n *namespaces) RevokeNamespacePermission(namespace utils.NameSpaceName, role string) error {
	return n.RevokeNamespacePermissionWithContext(context.Background(), namespace, role)
}

func (n *namespaces) RevokeNamespacePermissionWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	role string,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "permissions", role)
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (n *namespaces) GrantSubPermission(namespace utils.NameSpaceName, sName string, roles []string) error {
	return n.GrantSubPermissionWithContext(context.Background(), namespace, sName, roles)
}

func (n *namespaces) GrantSubPermissionWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	sName string,
	roles []string,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "permissions",
		"subscription", sName)
	return n.pulsar.Client.PostWithContext(ctx, endpoint, roles)
}

func (n *namespaces) RevokeSubPermission(namespace utils.NameSpaceName, sName, role string) error {
	return n.RevokeSubPermissionWithContext(context.Background(), namespace, sName, role)
}

func (n *namespaces) RevokeSubPermissionWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	sName,
	role string,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "permissions",
		sName, role)
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (n *namespaces) GetSubPermissions(namespace utils.NameSpaceName) (map[string][]string, error) {
	return n.GetSubPermissionsWithContext(context.Background(), namespace)
}

func (n *namespaces) GetSubPermissionsWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (map[string][]string, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "permissions", "subscription")
	var permissions map[string][]string
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &permissions)
	return permissions, err
}

func (n *namespaces) SetSubscriptionAuthMode(namespace utils.NameSpaceName, mode utils.SubscriptionAuthMode) error {
	return n.SetSubscriptionAuthModeWithContext(context.Background(), namespace, mode)
}

func (n *namespaces) SetSubscriptionAuthModeWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	mode utils.SubscriptionAuthMode,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscriptionAuthMode")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, mode.String())
}

func (n *namespaces) SetEncryptionRequiredStatus(namespace utils.NameSpaceName, encrypt bool) error {
	return n.SetEncryptionRequiredStatusWithContext(context.Background(), namespace, encrypt)
}

func (n *namespaces) SetEncryptionRequiredStatusWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	encrypt bool,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "encryptionRequired")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, strconv.FormatBool(encrypt))
}

func (n *namespaces) UnsubscribeNamespace(namespace utils.NameSpaceName, sName string) error {
	return n.UnsubscribeNamespaceWithContext(context.Background(), namespace, sName)
}

func (n *namespaces) UnsubscribeNamespaceWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	sName string,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "unsubscribe", url.QueryEscape(sName))
	return n.pulsar.Client.PostWithContext(ctx, endpoint, nil)
}

func (n *namespaces) UnsubscribeNamespaceBundle(namespace utils.NameSpaceName, bundle, sName string) error {
	return n.UnsubscribeNamespaceBundleWithContext(context.Background(), namespace, bundle, sName)
}

func (n *namespaces) UnsubscribeNamespaceBundleWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	bundle,
	sName string,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), bundle, "unsubscribe", url.QueryEscape(sName))
	return n.pulsar.Client.PostWithContext(ctx, endpoint, nil)
}

func (n *namespaces) ClearNamespaceBundleBacklogForSubscription(namespace utils.NameSpaceName,
	bundle, sName string) error {
	return n.ClearNamespaceBundleBacklogForSubscriptionWithContext(context.Background(), namespace, bundle, sName)
}

func (n *namespaces) ClearNamespaceBundleBacklogForSubscriptionWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	bundle, sName string) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), bundle, "clearBacklog", url.QueryEscape(sName))
	return n.pulsar.Client.PostWithContext(ctx, endpoint, nil)
}

func (n *namespaces) ClearNamespaceBundleBacklog(namespace utils.NameSpaceName, bundle string) error {
	return n.ClearNamespaceBundleBacklogWithContext(context.Background(), namespace, bundle)
}

func (n *namespaces) ClearNamespaceBundleBacklogWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	bundle string,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), bundle, "clearBacklog")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, nil)
}

func (n *namespaces) ClearNamespaceBacklogForSubscription(namespace utils.NameSpaceName, sName string) error {
	return n.ClearNamespaceBacklogForSubscriptionWithContext(context.Background(), namespace, sName)
}

func (n *namespaces) ClearNamespaceBacklogForSubscriptionWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	sName string,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "clearBacklog", url.QueryEscape(sName))
	return n.pulsar.Client.PostWithContext(ctx, endpoint, nil)
}

func (n *namespaces) ClearNamespaceBacklog(namespace utils.NameSpaceName) error {
	return n.ClearNamespaceBacklogWithContext(context.Background(), namespace)
}

func (n *namespaces) ClearNamespaceBacklogWithContext(ctx context.Context, namespace utils.NameSpaceName) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "clearBacklog")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, nil)
}

func (n *namespaces) SetReplicatorDispatchRate(namespace utils.NameSpaceName, rate utils.DispatchRate) error {
	return n.SetReplicatorDispatchRateWithContext(context.Background(), namespace, rate)
}

func (n *namespaces) SetReplicatorDispatchRateWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	rate utils.DispatchRate,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "replicatorDispatchRate")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, rate)
}

func (n *namespaces) GetReplicatorDispatchRate(namespace utils.NameSpaceName) (utils.DispatchRate, error) {
	return n.GetReplicatorDispatchRateWithContext(context.Background(), namespace)
}

func (n *namespaces) GetReplicatorDispatchRateWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (utils.DispatchRate, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "replicatorDispatchRate")
	var rate utils.DispatchRate
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &rate)
	return rate, err
}

func (n *namespaces) SetSubscriptionDispatchRate(namespace utils.NameSpaceName, rate utils.DispatchRate) error {
	return n.SetSubscriptionDispatchRateWithContext(context.Background(), namespace, rate)
}

func (n *namespaces) SetSubscriptionDispatchRateWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	rate utils.DispatchRate,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscriptionDispatchRate")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, rate)
}

func (n *namespaces) GetSubscriptionDispatchRate(namespace utils.NameSpaceName) (utils.DispatchRate, error) {
	return n.GetSubscriptionDispatchRateWithContext(context.Background(), namespace)
}

func (n *namespaces) GetSubscriptionDispatchRateWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (utils.DispatchRate, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscriptionDispatchRate")
	var rate utils.DispatchRate
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &rate)
	return rate, err
}

func (n *namespaces) SetSubscribeRate(namespace utils.NameSpaceName, rate utils.SubscribeRate) error {
	return n.SetSubscribeRateWithContext(context.Background(), namespace, rate)
}

func (n *namespaces) SetSubscribeRateWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	rate utils.SubscribeRate,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscribeRate")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, rate)
}

func (n *namespaces) GetSubscribeRate(namespace utils.NameSpaceName) (utils.SubscribeRate, error) {
	return n.GetSubscribeRateWithContext(context.Background(), namespace)
}

func (n *namespaces) GetSubscribeRateWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (utils.SubscribeRate, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscribeRate")
	var rate utils.SubscribeRate
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &rate)
	return rate, err
}

func (n *namespaces) SetDispatchRate(namespace utils.NameSpaceName, rate utils.DispatchRate) error {
	return n.SetDispatchRateWithContext(context.Background(), namespace, rate)
}

func (n *namespaces) SetDispatchRateWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	rate utils.DispatchRate) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "dispatchRate")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, rate)
}

func (n *namespaces) GetDispatchRate(namespace utils.NameSpaceName) (utils.DispatchRate, error) {
	return n.GetDispatchRateWithContext(context.Background(), namespace)
}

func (n *namespaces) GetDispatchRateWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (utils.DispatchRate, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "dispatchRate")
	var rate utils.DispatchRate
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &rate)
	return rate, err
}

func (n *namespaces) SetPublishRate(namespace utils.NameSpaceName, pubRate utils.PublishRate) error {
	return n.SetPublishRateWithContext(context.Background(), namespace, pubRate)
}

func (n *namespaces) SetPublishRateWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	pubRate utils.PublishRate,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "publishRate")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, pubRate)
}

func (n *namespaces) GetPublishRate(namespace utils.NameSpaceName) (utils.PublishRate, error) {
	return n.GetPublishRateWithContext(context.Background(), namespace)
}

func (n *namespaces) GetPublishRateWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (utils.PublishRate, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "publishRate")
	var pubRate utils.PublishRate
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &pubRate)
	return pubRate, err
}

func (n *namespaces) SetIsAllowAutoUpdateSchema(namespace utils.NameSpaceName, isAllowAutoUpdateSchema bool) error {
	return n.SetIsAllowAutoUpdateSchemaWithContext(context.Background(), namespace, isAllowAutoUpdateSchema)
}

func (n *namespaces) SetIsAllowAutoUpdateSchemaWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	isAllowAutoUpdateSchema bool,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "isAllowAutoUpdateSchema")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, &isAllowAutoUpdateSchema)
}

func (n *namespaces) GetIsAllowAutoUpdateSchema(namespace utils.NameSpaceName) (bool, error) {
	return n.GetIsAllowAutoUpdateSchemaWithContext(context.Background(), namespace)
}

func (n *namespaces) GetIsAllowAutoUpdateSchemaWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (bool, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "isAllowAutoUpdateSchema")
	var result bool
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &result)
	return result, err
}

func (n *namespaces) GetInactiveTopicPolicies(namespace utils.NameSpaceName) (utils.InactiveTopicPolicies, error) {
	return n.GetInactiveTopicPoliciesWithContext(context.Background(), namespace)
}

func (n *namespaces) GetInactiveTopicPoliciesWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (utils.InactiveTopicPolicies, error) {
	var out utils.InactiveTopicPolicies
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "inactiveTopicPolicies")
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &out)
	return out, err
}

func (n *namespaces) RemoveInactiveTopicPolicies(namespace utils.NameSpaceName) error {
	return n.RemoveInactiveTopicPoliciesWithContext(context.Background(), namespace)
}

func (n *namespaces) RemoveInactiveTopicPoliciesWithContext(ctx context.Context, namespace utils.NameSpaceName) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "inactiveTopicPolicies")
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (n *namespaces) SetInactiveTopicPolicies(namespace utils.NameSpaceName, data utils.InactiveTopicPolicies) error {
	return n.SetInactiveTopicPoliciesWithContext(context.Background(), namespace, data)
}

func (n *namespaces) SetInactiveTopicPoliciesWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	data utils.InactiveTopicPolicies,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "inactiveTopicPolicies")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, data)
}

func (n *namespaces) GetSubscriptionExpirationTime(namespace utils.NameSpaceName) (int, error) {
	return n.GetSubscriptionExpirationTimeWithContext(context.Background(), namespace)
}

func (n *namespaces) GetSubscriptionExpirationTimeWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (int, error) {
	var result = -1

	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscriptionExpirationTime")
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &result)
	return result, err
}

func (n *namespaces) SetSubscriptionExpirationTime(namespace utils.NameSpaceName,
	subscriptionExpirationTimeInMinutes int) error {
	return n.SetSubscriptionExpirationTimeWithContext(context.Background(), namespace, subscriptionExpirationTimeInMinutes)
}

func (n *namespaces) SetSubscriptionExpirationTimeWithContext(ctx context.Context, namespace utils.NameSpaceName,
	subscriptionExpirationTimeInMinutes int) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscriptionExpirationTime")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, &subscriptionExpirationTimeInMinutes)
}

func (n *namespaces) RemoveSubscriptionExpirationTime(namespace utils.NameSpaceName) error {
	return n.RemoveSubscriptionExpirationTimeWithContext(context.Background(), namespace)
}

func (n *namespaces) RemoveSubscriptionExpirationTimeWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "subscriptionExpirationTime")
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (n *namespaces) UpdateProperties(namespace utils.NameSpaceName, properties map[string]string) error {
	return n.UpdatePropertiesWithContext(context.Background(), namespace, properties)
}

func (n *namespaces) UpdatePropertiesWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	properties map[string]string,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "properties")
	return n.pulsar.Client.PutWithContext(ctx, endpoint, properties)
}

func (n *namespaces) GetProperties(namespace utils.NameSpaceName) (map[string]string, error) {
	return n.GetPropertiesWithContext(context.Background(), namespace)
}

func (n *namespaces) GetPropertiesWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (map[string]string, error) {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "properties")
	properties := make(map[string]string)
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &properties)
	return properties, err
}

func (n *namespaces) RemoveProperties(namespace utils.NameSpaceName) error {
	return n.RemovePropertiesWithContext(context.Background(), namespace)
}

func (n *namespaces) RemovePropertiesWithContext(ctx context.Context, namespace utils.NameSpaceName) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "properties")
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

// nolint: revive // It's ok here to use a built-in function name (max)
func (n *namespaces) SetMaxTopicsPerNamespace(namespace utils.NameSpaceName, max int) error {
	return n.SetMaxTopicsPerNamespaceWithContext(context.Background(), namespace, max)
}

// nolint: revive // It's ok here to use a built-in function name (max)
func (n *namespaces) SetMaxTopicsPerNamespaceWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
	max int,
) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxTopicsPerNamespace")
	return n.pulsar.Client.PostWithContext(ctx, endpoint, max)
}

func (n *namespaces) GetMaxTopicsPerNamespace(namespace utils.NameSpaceName) (int, error) {
	return n.GetMaxTopicsPerNamespaceWithContext(context.Background(), namespace)
}

func (n *namespaces) GetMaxTopicsPerNamespaceWithContext(
	ctx context.Context,
	namespace utils.NameSpaceName,
) (int, error) {
	var result int
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxTopicsPerNamespace")
	err := n.pulsar.Client.GetWithContext(ctx, endpoint, &result)
	return result, err
}

func (n *namespaces) RemoveMaxTopicsPerNamespace(namespace utils.NameSpaceName) error {
	return n.RemoveMaxTopicsPerNamespaceWithContext(context.Background(), namespace)
}

func (n *namespaces) RemoveMaxTopicsPerNamespaceWithContext(ctx context.Context, namespace utils.NameSpaceName) error {
	endpoint := n.pulsar.endpoint(n.basePath, namespace.String(), "maxTopicsPerNamespace")
	return n.pulsar.Client.DeleteWithContext(ctx, endpoint)
}
