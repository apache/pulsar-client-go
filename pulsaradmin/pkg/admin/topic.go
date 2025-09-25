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
	"fmt"
	"strconv"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

// Topics is admin interface for topics management
type Topics interface {
	// Create creates a partitioned or non-partitioned topic
	//
	// @param topic
	//        topicName struct
	// @param partitions
	//        number of topic partitions,
	//        when setting to 0, it will create a non-partitioned topic
	Create(topic utils.TopicName, partitions int) error

	// CreateWithContext creates a partitioned or non-partitioned topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param partitions
	//        number of topic partitions,
	//        when setting to 0, it will create a non-partitioned topic
	CreateWithContext(ctx context.Context, topic utils.TopicName, partitions int) error

	// CreateWithProperties creates a partitioned or non-partitioned topic with specific properties
	//
	// @param topic
	//        topicName struct
	// @param partitions
	//        number of topic partitions,
	//        when setting to 0, it will create a non-partitioned topic
	// @param meta
	//        topic properties
	CreateWithProperties(topic utils.TopicName, partitions int, meta map[string]string) error

	// CreateWithPropertiesWithContext creates a partitioned or non-partitioned topic with specific properties
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param partitions
	//        number of topic partitions,
	//        when setting to 0, it will create a non-partitioned topic
	// @param meta
	//        topic properties
	CreateWithPropertiesWithContext(ctx context.Context, topic utils.TopicName, partitions int, meta map[string]string) error

	// GetProperties returns the properties of a topic
	GetProperties(topic utils.TopicName) (map[string]string, error)

	// GetPropertiesWithContext returns the properties of a topic
	GetPropertiesWithContext(ctx context.Context, topic utils.TopicName) (map[string]string, error)

	// UpdateProperties updates the properties of a topic
	UpdateProperties(topic utils.TopicName, properties map[string]string) error

	// UpdatePropertiesWithContext updates the properties of a topic
	UpdatePropertiesWithContext(ctx context.Context, topic utils.TopicName, properties map[string]string) error

	// RemoveProperty removes a property with the given key of a topic
	RemoveProperty(topic utils.TopicName, key string) error

	// RemovePropertyWithContext removes a property with the given key of a topic
	RemovePropertyWithContext(ctx context.Context, topic utils.TopicName, key string) error

	// Delete deletes a topic, this function can delete both partitioned or non-partitioned topic
	//
	// @param topic
	//        topicName struct
	// @param force
	//        delete topic forcefully
	// @param nonPartitioned
	//        when set to true, topic will be treated as a non-partitioned topic
	//        Otherwise it will be treated as a partitioned topic
	Delete(topic utils.TopicName, force bool, nonPartitioned bool) error

	// DeleteWithContext deletes a topic, this function can delete both partitioned or non-partitioned topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param force
	//        delete topic forcefully
	// @param nonPartitioned
	//        when set to true, topic will be treated as a non-partitioned topic
	//        Otherwise it will be treated as a partitioned topic
	DeleteWithContext(ctx context.Context, topic utils.TopicName, force bool, nonPartitioned bool) error

	// Update updates number of partitions of a non-global partitioned topic
	// It requires partitioned-topic to be already exist and number of new partitions must be greater than existing
	// number of partitions. Decrementing number of partitions requires deletion of topic which is not supported.
	//
	// @param topic
	//        topicName struct
	// @param partitions
	//        number of new partitions of already exist partitioned-topic
	Update(topic utils.TopicName, partitions int) error

	// UpdateWithContext updates number of partitions of a non-global partitioned topic
	// It requires partitioned-topic to be already exist and number of new partitions must be greater than existing
	// number of partitions. Decrementing number of partitions requires deletion of topic which is not supported.
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param partitions
	//        number of new partitions of already exist partitioned-topic
	UpdateWithContext(ctx context.Context, topic utils.TopicName, partitions int) error

	// GetMetadata returns metadata of a partitioned topic
	GetMetadata(utils.TopicName) (utils.PartitionedTopicMetadata, error)

	// GetMetadataWithContext returns metadata of a partitioned topic
	GetMetadataWithContext(context.Context, utils.TopicName) (utils.PartitionedTopicMetadata, error)

	// List returns the list of topics under a namespace
	List(utils.NameSpaceName) ([]string, []string, error)

	// ListWithContext returns the list of topics under a namespace
	ListWithContext(context.Context, utils.NameSpaceName) ([]string, []string, error)

	// GetInternalInfo returns the internal metadata info for the topic
	GetInternalInfo(utils.TopicName) (utils.ManagedLedgerInfo, error)

	// GetInternalInfoWithContext returns the internal metadata info for the topic
	GetInternalInfoWithContext(context.Context, utils.TopicName) (utils.ManagedLedgerInfo, error)

	// GetPermissions returns permissions on a topic
	// Retrieve the effective permissions for a topic. These permissions are defined by the permissions set at the
	// namespace level combined (union) with any eventual specific permission set on the topic.
	GetPermissions(utils.TopicName) (map[string][]utils.AuthAction, error)

	// GetPermissionsWithContext returns permissions on a topic
	// Retrieve the effective permissions for a topic. These permissions are defined by the permissions set at the
	// namespace level combined (union) with any eventual specific permission set on the topic.
	GetPermissionsWithContext(context.Context, utils.TopicName) (map[string][]utils.AuthAction, error)

	// GrantPermission grants a new permission to a client role on a single topic
	//
	// @param topic
	//        topicName struct
	// @param role
	//        client role to which grant permission
	// @param action
	//        auth actions (e.g. produce and consume)
	GrantPermission(topic utils.TopicName, role string, action []utils.AuthAction) error

	// GrantPermissionWithContext grants a new permission to a client role on a single topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param role
	//        client role to which grant permission
	// @param action
	//        auth actions (e.g. produce and consume)
	GrantPermissionWithContext(ctx context.Context, topic utils.TopicName, role string, action []utils.AuthAction) error

	// RevokePermission revokes permissions to a client role on a single topic. If the permission
	// was not set at the topic level, but rather at the namespace level, this operation will
	// return an error (HTTP status code 412).
	//
	// @param topic
	//        topicName struct
	// @param role
	//        client role to which remove permissions
	RevokePermission(topic utils.TopicName, role string) error

	// RevokePermissionWithContext revokes permissions to a client role on a single topic. If the permission
	// was not set at the topic level, but rather at the namespace level, this operation will
	// return an error (HTTP status code 412).
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param role
	//        client role to which remove permissions
	RevokePermissionWithContext(ctx context.Context, topic utils.TopicName, role string) error

	// Lookup returns the broker URL that serves the topic
	Lookup(utils.TopicName) (utils.LookupData, error)

	// LookupWithContext returns the broker URL that serves the topic
	LookupWithContext(context.Context, utils.TopicName) (utils.LookupData, error)

	// GetBundleRange returns a bundle range of a topic
	GetBundleRange(utils.TopicName) (string, error)

	// GetBundleRangeWithContext returns a bundle range of a topic
	GetBundleRangeWithContext(context.Context, utils.TopicName) (string, error)

	// GetLastMessageID returns the last commit message Id of a topic
	GetLastMessageID(utils.TopicName) (utils.MessageID, error)

	// GetLastMessageIDWithContext returns the last commit message Id of a topic
	GetLastMessageIDWithContext(context.Context, utils.TopicName) (utils.MessageID, error)

	// GetMessageID returns the message Id by timestamp(ms) of a topic
	//
	// @param topic
	//        topicName struct
	// @param timestamp
	//        absolute timestamp (in ms)
	GetMessageID(topic utils.TopicName, timestamp int64) (utils.MessageID, error)

	// GetMessageIDWithContext returns the message Id by timestamp(ms) of a topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param timestamp
	//        absolute timestamp (in ms)
	GetMessageIDWithContext(ctx context.Context, topic utils.TopicName, timestamp int64) (utils.MessageID, error)

	// GetStats returns the stats for the topic.
	//
	// All the rates are computed over a 1-minute window and are relative the last completed 1-minute period
	GetStats(utils.TopicName) (utils.TopicStats, error)

	// GetStatsWithContext returns the stats for the topic.
	//
	// All the rates are computed over a 1-minute window and are relative the last completed 1-minute period
	GetStatsWithContext(context.Context, utils.TopicName) (utils.TopicStats, error)

	// GetStatsWithOption returns the stats for the topic
	//
	// All the rates are computed over a 1-minute window and are relative the last completed 1-minute period
	//
	// @param topic
	//        topicName struct
	// @param option
	//        request option, e.g. get_precise_backlog or subscription_backlog_size
	GetStatsWithOption(topic utils.TopicName, option utils.GetStatsOptions) (utils.TopicStats, error)

	// GetStatsWithOptionWithContext returns the stats for the topic
	//
	// All the rates are computed over a 1-minute window and are relative the last completed 1-minute period
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param option
	//        request option, e.g. get_precise_backlog or subscription_backlog_size
	GetStatsWithOptionWithContext(ctx context.Context, topic utils.TopicName, option utils.GetStatsOptions) (utils.TopicStats, error)

	// GetInternalStats returns the internal stats for the topic.
	GetInternalStats(utils.TopicName) (utils.PersistentTopicInternalStats, error)

	// GetInternalStatsWithContext returns the internal stats for the topic.
	GetInternalStatsWithContext(context.Context, utils.TopicName) (utils.PersistentTopicInternalStats, error)

	// GetPartitionedStats returns the stats for the partitioned topic
	//
	// All the rates are computed over a 1-minute window and are relative the last completed 1-minute period
	//
	// @param topic
	//        topicName struct
	// @param perPartition
	//        flag to get stats per partition
	GetPartitionedStats(topic utils.TopicName, perPartition bool) (utils.PartitionedTopicStats, error)

	// GetPartitionedStatsWithContext returns the stats for the partitioned topic
	//
	// All the rates are computed over a 1-minute window and are relative the last completed 1-minute period
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param perPartition
	//        flag to get stats per partition
	GetPartitionedStatsWithContext(ctx context.Context, topic utils.TopicName, perPartition bool) (utils.PartitionedTopicStats, error)

	// GetPartitionedStatsWithOption returns the stats for the partitioned topic
	//
	// All the rates are computed over a 1-minute window and are relative the last completed 1-minute period
	//
	// @param topic
	//        topicName struct
	// @param perPartition
	//        flag to get stats per partition
	// @param option
	//        request option, e.g. get_precise_backlog or subscription_backlog_size
	GetPartitionedStatsWithOption(
		topic utils.TopicName,
		perPartition bool,
		option utils.GetStatsOptions,
	) (utils.PartitionedTopicStats, error)

	// GetPartitionedStatsWithOptionWithContext returns the stats for the partitioned topic
	//
	// All the rates are computed over a 1-minute window and are relative the last completed 1-minute period
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param perPartition
	//        flag to get stats per partition
	// @param option
	//        request option, e.g. get_precise_backlog or subscription_backlog_size
	GetPartitionedStatsWithOptionWithContext(ctx context.Context,
		topic utils.TopicName,
		perPartition bool,
		option utils.GetStatsOptions,
	) (utils.PartitionedTopicStats, error)

	// Terminate terminates the topic and prevent any more messages being published on it
	Terminate(utils.TopicName) (utils.MessageID, error)

	// TerminateWithContext terminates the topic and prevent any more messages being published on it
	TerminateWithContext(context.Context, utils.TopicName) (utils.MessageID, error)

	// Offload triggers offloading messages in topic to longterm storage
	Offload(utils.TopicName, utils.MessageID) error

	// OffloadWithContext triggers offloading messages in topic to longterm storage
	OffloadWithContext(context.Context, utils.TopicName, utils.MessageID) error

	// OffloadStatus checks the status of an ongoing offloading operation for a topic
	OffloadStatus(utils.TopicName) (utils.OffloadProcessStatus, error)

	// OffloadStatusWithContext checks the status of an ongoing offloading operation for a topic
	OffloadStatusWithContext(context.Context, utils.TopicName) (utils.OffloadProcessStatus, error)

	// Unload a topic
	Unload(utils.TopicName) error

	// UnloadWithContext a topic
	UnloadWithContext(context.Context, utils.TopicName) error

	// Compact triggers compaction to run for a topic. A single topic can only have one instance of compaction
	// running at any time. Any attempt to trigger another will be met with a ConflictException.
	Compact(utils.TopicName) error

	// CompactWithContext triggers compaction to run for a topic. A single topic can only have one instance of compaction
	// running at any time. Any attempt to trigger another will be met with a ConflictException.
	CompactWithContext(context.Context, utils.TopicName) error

	// CompactStatus checks the status of an ongoing compaction for a topic
	CompactStatus(utils.TopicName) (utils.LongRunningProcessStatus, error)

	// CompactStatusWithContext checks the status of an ongoing compaction for a topic
	CompactStatusWithContext(context.Context, utils.TopicName) (utils.LongRunningProcessStatus, error)

	// GetMessageTTL returns the message TTL for a topic
	GetMessageTTL(utils.TopicName) (int, error)

	// GetMessageTTLWithContext returns the message TTL for a topic
	GetMessageTTLWithContext(context.Context, utils.TopicName) (int, error)

	// SetMessageTTL sets the message TTL for a topic
	//
	// @param topic
	//        topicName struct
	// @param messageTTL
	//        Message TTL in second
	SetMessageTTL(topic utils.TopicName, messageTTL int) error

	// SetMessageTTLWithContext sets the message TTL for a topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param messageTTL
	//        Message TTL in second
	SetMessageTTLWithContext(ctx context.Context, topic utils.TopicName, messageTTL int) error

	// RemoveMessageTTL removes the message TTL for a topic
	RemoveMessageTTL(utils.TopicName) error

	// RemoveMessageTTLWithContext removes the message TTL for a topic
	RemoveMessageTTLWithContext(context.Context, utils.TopicName) error

	// GetMaxProducers Get max number of producers for a topic
	GetMaxProducers(utils.TopicName) (int, error)

	// GetMaxProducersWithContext Get max number of producers for a topic
	GetMaxProducersWithContext(context.Context, utils.TopicName) (int, error)

	// SetMaxProducers sets max number of producers for a topic
	//
	// @param topic
	//        topicName struct
	// @param maxProducers
	//        max number of producer
	SetMaxProducers(topic utils.TopicName, maxProducers int) error

	// SetMaxProducersWithContext sets max number of producers for a topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param maxProducers
	//        max number of producer
	SetMaxProducersWithContext(ctx context.Context, topic utils.TopicName, maxProducers int) error

	// RemoveMaxProducers removes max number of producers for a topic
	RemoveMaxProducers(utils.TopicName) error

	// RemoveMaxProducersWithContext removes max number of producers for a topic
	RemoveMaxProducersWithContext(context.Context, utils.TopicName) error

	// GetMaxConsumers returns max number of consumers for a topic
	GetMaxConsumers(utils.TopicName) (int, error)

	// GetMaxConsumersWithContext returns max number of consumers for a topic
	GetMaxConsumersWithContext(context.Context, utils.TopicName) (int, error)

	// SetMaxConsumers sets max number of consumers for a topic
	//
	// @param topic
	//        topicName struct
	// @param maxConsumers
	//        max number of consumer
	SetMaxConsumers(topic utils.TopicName, maxConsumers int) error

	// SetMaxConsumersWithContext sets max number of consumers for a topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param maxConsumers
	//        max number of consumer
	SetMaxConsumersWithContext(ctx context.Context, topic utils.TopicName, maxConsumers int) error

	// RemoveMaxConsumers removes max number of consumers for a topic
	RemoveMaxConsumers(utils.TopicName) error

	// RemoveMaxConsumersWithContext removes max number of consumers for a topic
	RemoveMaxConsumersWithContext(context.Context, utils.TopicName) error

	// GetMaxUnackMessagesPerConsumer returns max unacked messages policy on consumer for a topic
	GetMaxUnackMessagesPerConsumer(utils.TopicName) (int, error)

	// GetMaxUnackMessagesPerConsumerWithContext returns max unacked messages policy on consumer for a topic
	GetMaxUnackMessagesPerConsumerWithContext(context.Context, utils.TopicName) (int, error)

	// SetMaxUnackMessagesPerConsumer sets max unacked messages policy on consumer for a topic
	//
	// @param topic
	//        topicName struct
	// @param maxUnackedNum
	//        max unAcked messages on each consumer
	SetMaxUnackMessagesPerConsumer(topic utils.TopicName, maxUnackedNum int) error

	// SetMaxUnackMessagesPerConsumerWithContext sets max unacked messages policy on consumer for a topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param maxUnackedNum
	//        max unAcked messages on each consumer
	SetMaxUnackMessagesPerConsumerWithContext(ctx context.Context, topic utils.TopicName, maxUnackedNum int) error

	// RemoveMaxUnackMessagesPerConsumer removes max unacked messages policy on consumer for a topic
	RemoveMaxUnackMessagesPerConsumer(utils.TopicName) error

	// RemoveMaxUnackMessagesPerConsumerWithContext removes max unacked messages policy on consumer for a topic
	RemoveMaxUnackMessagesPerConsumerWithContext(context.Context, utils.TopicName) error

	// GetMaxUnackMessagesPerSubscription returns max unacked messages policy on subscription for a topic
	GetMaxUnackMessagesPerSubscription(utils.TopicName) (int, error)

	// GetMaxUnackMessagesPerSubscriptionWithContext returns max unacked messages policy on subscription for a topic
	GetMaxUnackMessagesPerSubscriptionWithContext(context.Context, utils.TopicName) (int, error)

	// SetMaxUnackMessagesPerSubscription sets max unacked messages policy on subscription for a topic
	//
	// @param topic
	//        topicName struct
	// @param maxUnackedNum
	//        max unAcked messages on subscription of a topic
	SetMaxUnackMessagesPerSubscription(topic utils.TopicName, maxUnackedNum int) error

	// SetMaxUnackMessagesPerSubscriptionWithContext sets max unacked messages policy on subscription for a topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param maxUnackedNum
	//        max unAcked messages on subscription of a topic
	SetMaxUnackMessagesPerSubscriptionWithContext(ctx context.Context, topic utils.TopicName, maxUnackedNum int) error

	// RemoveMaxUnackMessagesPerSubscription removes max unacked messages policy on subscription for a topic
	RemoveMaxUnackMessagesPerSubscription(utils.TopicName) error

	// RemoveMaxUnackMessagesPerSubscriptionWithContext removes max unacked messages policy on subscription for a topic
	RemoveMaxUnackMessagesPerSubscriptionWithContext(context.Context, utils.TopicName) error

	// GetPersistence returns the persistence policies for a topic
	GetPersistence(utils.TopicName) (*utils.PersistenceData, error)

	// GetPersistenceWithContext returns the persistence policies for a topic
	GetPersistenceWithContext(context.Context, utils.TopicName) (*utils.PersistenceData, error)

	// SetPersistence sets the persistence policies for a topic
	SetPersistence(utils.TopicName, utils.PersistenceData) error

	// SetPersistenceWithContext sets the persistence policies for a topic
	SetPersistenceWithContext(context.Context, utils.TopicName, utils.PersistenceData) error

	// RemovePersistence removes the persistence policies for a topic
	RemovePersistence(utils.TopicName) error

	// RemovePersistenceWithContext removes the persistence policies for a topic
	RemovePersistenceWithContext(context.Context, utils.TopicName) error

	// GetDelayedDelivery returns the delayed delivery policy for a topic
	GetDelayedDelivery(utils.TopicName) (*utils.DelayedDeliveryData, error)

	// GetDelayedDeliveryWithContext returns the delayed delivery policy for a topic
	GetDelayedDeliveryWithContext(context.Context, utils.TopicName) (*utils.DelayedDeliveryData, error)

	// SetDelayedDelivery sets the delayed delivery policy on a topic
	SetDelayedDelivery(utils.TopicName, utils.DelayedDeliveryData) error

	// SetDelayedDeliveryWithContext sets the delayed delivery policy on a topic
	SetDelayedDeliveryWithContext(context.Context, utils.TopicName, utils.DelayedDeliveryData) error

	// RemoveDelayedDelivery removes the delayed delivery policy on a topic
	RemoveDelayedDelivery(utils.TopicName) error

	// RemoveDelayedDeliveryWithContext removes the delayed delivery policy on a topic
	RemoveDelayedDeliveryWithContext(context.Context, utils.TopicName) error

	// GetDispatchRate returns message dispatch rate for a topic
	GetDispatchRate(utils.TopicName) (*utils.DispatchRateData, error)

	// GetDispatchRateWithContext returns message dispatch rate for a topic
	GetDispatchRateWithContext(context.Context, utils.TopicName) (*utils.DispatchRateData, error)

	// SetDispatchRate sets message dispatch rate for a topic
	SetDispatchRate(utils.TopicName, utils.DispatchRateData) error

	// SetDispatchRateWithContext sets message dispatch rate for a topic
	SetDispatchRateWithContext(context.Context, utils.TopicName, utils.DispatchRateData) error

	// RemoveDispatchRate removes message dispatch rate for a topic
	RemoveDispatchRate(utils.TopicName) error

	// RemoveDispatchRateWithContext removes message dispatch rate for a topic
	RemoveDispatchRateWithContext(context.Context, utils.TopicName) error

	// GetPublishRate returns message publish rate for a topic
	GetPublishRate(utils.TopicName) (*utils.PublishRateData, error)

	// GetPublishRateWithContext returns message publish rate for a topic
	GetPublishRateWithContext(context.Context, utils.TopicName) (*utils.PublishRateData, error)

	// SetPublishRate sets message publish rate for a topic
	SetPublishRate(utils.TopicName, utils.PublishRateData) error

	// SetPublishRateWithContext sets message publish rate for a topic
	SetPublishRateWithContext(context.Context, utils.TopicName, utils.PublishRateData) error

	// RemovePublishRate removes message publish rate for a topic
	RemovePublishRate(utils.TopicName) error

	// RemovePublishRateWithContext removes message publish rate for a topic
	RemovePublishRateWithContext(context.Context, utils.TopicName) error

	// GetDeduplicationStatus returns the deduplication policy for a topic
	GetDeduplicationStatus(utils.TopicName) (bool, error)

	// GetDeduplicationStatusWithContext returns the deduplication policy for a topic
	GetDeduplicationStatusWithContext(context.Context, utils.TopicName) (bool, error)

	// SetDeduplicationStatus sets the deduplication policy for a topic
	//
	// @param topic
	//        topicName struct
	// @param enabled
	//        set enable or disable deduplication of the topic
	SetDeduplicationStatus(topic utils.TopicName, enabled bool) error

	// SetDeduplicationStatusWithContext sets the deduplication policy for a topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param enabled
	//        set enable or disable deduplication of the topic
	SetDeduplicationStatusWithContext(ctx context.Context, topic utils.TopicName, enabled bool) error

	// RemoveDeduplicationStatus removes the deduplication policy for a topic
	RemoveDeduplicationStatus(utils.TopicName) error

	// RemoveDeduplicationStatusWithContext removes the deduplication policy for a topic
	RemoveDeduplicationStatusWithContext(context.Context, utils.TopicName) error

	// GetRetention returns the retention configuration for a topic
	//
	// @param topic
	//        topicName struct
	// @param applied
	//        when set to true, function will try to find policy applied to this topic
	//        in namespace or broker level, if no policy set in topic level
	GetRetention(topic utils.TopicName, applied bool) (*utils.RetentionPolicies, error)

	// GetRetentionWithContext returns the retention configuration for a topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param applied
	//        when set to true, function will try to find policy applied to this topic
	//        in namespace or broker level, if no policy set in topic level
	GetRetentionWithContext(ctx context.Context, topic utils.TopicName, applied bool) (*utils.RetentionPolicies, error)

	// RemoveRetention removes the retention configuration on a topic
	RemoveRetention(utils.TopicName) error

	// RemoveRetentionWithContext removes the retention configuration on a topic
	RemoveRetentionWithContext(context.Context, utils.TopicName) error

	// SetRetention sets the retention policy for a topic
	SetRetention(utils.TopicName, utils.RetentionPolicies) error

	// SetRetentionWithContext sets the retention policy for a topic
	SetRetentionWithContext(context.Context, utils.TopicName, utils.RetentionPolicies) error

	// GetCompactionThreshold returns the compaction threshold for a topic.
	//
	// i.e. The maximum number of bytes can have before compaction is triggered.
	//
	// @param topic
	//        topicName struct
	// @param applied
	//        when set to true, function will try to find policy applied to this topic
	//        in namespace or broker level, if no policy set in topic level
	GetCompactionThreshold(topic utils.TopicName, applied bool) (int64, error)

	// GetCompactionThresholdWithContext returns the compaction threshold for a topic.
	//
	// i.e. The maximum number of bytes can have before compaction is triggered.
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param applied
	//        when set to true, function will try to find policy applied to this topic
	//        in namespace or broker level, if no policy set in topic level
	GetCompactionThresholdWithContext(ctx context.Context, topic utils.TopicName, applied bool) (int64, error)

	// SetCompactionThreshold sets the compaction threshold for a topic
	//
	// @param topic
	//        topicName struct
	// @param threshold
	//        maximum number of backlog bytes before compaction is triggered
	SetCompactionThreshold(topic utils.TopicName, threshold int64) error

	// SetCompactionThresholdWithContext sets the compaction threshold for a topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param threshold
	//        maximum number of backlog bytes before compaction is triggered
	SetCompactionThresholdWithContext(ctx context.Context, topic utils.TopicName, threshold int64) error

	// RemoveCompactionThreshold removes compaction threshold for a topic
	RemoveCompactionThreshold(utils.TopicName) error

	// RemoveCompactionThresholdWithContext removes compaction threshold for a topic
	RemoveCompactionThresholdWithContext(context.Context, utils.TopicName) error

	// GetBacklogQuotaMap returns backlog quota map for a topic
	//
	// @param topic
	//        topicName struct
	// @param applied
	//        when set to true, function will try to find policy applied to this topic
	//        in namespace or broker level, if no policy set in topic level
	GetBacklogQuotaMap(topic utils.TopicName, applied bool) (map[utils.BacklogQuotaType]utils.BacklogQuota, error)

	// GetBacklogQuotaMapWithContext returns backlog quota map for a topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param applied
	//        when set to true, function will try to find policy applied to this topic
	//        in namespace or broker level, if no policy set in topic level
	GetBacklogQuotaMapWithContext(ctx context.Context, topic utils.TopicName, applied bool) (map[utils.BacklogQuotaType]utils.BacklogQuota, error)

	// SetBacklogQuota sets a backlog quota for a topic
	SetBacklogQuota(utils.TopicName, utils.BacklogQuota, utils.BacklogQuotaType) error

	// SetBacklogQuotaWithContext sets a backlog quota for a topic
	SetBacklogQuotaWithContext(context.Context, utils.TopicName, utils.BacklogQuota, utils.BacklogQuotaType) error

	// RemoveBacklogQuota removes a backlog quota policy from a topic
	RemoveBacklogQuota(utils.TopicName, utils.BacklogQuotaType) error

	// RemoveBacklogQuotaWithContext removes a backlog quota policy from a topic
	RemoveBacklogQuotaWithContext(context.Context, utils.TopicName, utils.BacklogQuotaType) error

	// GetInactiveTopicPolicies returns the inactive topic policies on a topic
	//
	// @param topic
	//        topicName struct
	// @param applied
	//        when set to true, function will try to find policy applied to this topic
	//        in namespace or broker level, if no policy set in topic level
	GetInactiveTopicPolicies(topic utils.TopicName, applied bool) (utils.InactiveTopicPolicies, error)

	// GetInactiveTopicPoliciesWithContext returns the inactive topic policies on a topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param applied
	//        when set to true, function will try to find policy applied to this topic
	//        in namespace or broker level, if no policy set in topic level
	GetInactiveTopicPoliciesWithContext(ctx context.Context, topic utils.TopicName, applied bool) (utils.InactiveTopicPolicies, error)

	// RemoveInactiveTopicPolicies removes inactive topic policies from a topic
	RemoveInactiveTopicPolicies(utils.TopicName) error

	// RemoveInactiveTopicPoliciesWithContext removes inactive topic policies from a topic
	RemoveInactiveTopicPoliciesWithContext(context.Context, utils.TopicName) error

	// SetInactiveTopicPolicies sets the inactive topic policies on a topic
	SetInactiveTopicPolicies(topic utils.TopicName, data utils.InactiveTopicPolicies) error

	// SetInactiveTopicPoliciesWithContext sets the inactive topic policies on a topic
	SetInactiveTopicPoliciesWithContext(ctx context.Context, topic utils.TopicName, data utils.InactiveTopicPolicies) error

	// GetReplicationClusters returns the replication clusters of a topic
	GetReplicationClusters(topic utils.TopicName) ([]string, error)

	// GetReplicationClustersWithContext returns the replication clusters of a topic
	GetReplicationClustersWithContext(ctx context.Context, topic utils.TopicName) ([]string, error)

	// SetReplicationClusters sets the replication clusters on a topic
	//
	// @param topic
	//        topicName struct
	// @param data
	//        list of replication cluster id
	SetReplicationClusters(topic utils.TopicName, data []string) error

	// SetReplicationClustersWithContext sets the replication clusters on a topic
	//
	// @param ctx
	//        context used for the request
	// @param topic
	//        topicName struct
	// @param data
	//        list of replication cluster id
	SetReplicationClustersWithContext(ctx context.Context, topic utils.TopicName, data []string) error

	// GetSubscribeRate returns subscribe rate configuration for a topic
	GetSubscribeRate(utils.TopicName) (*utils.SubscribeRate, error)

	// GetSubscribeRateWithContext returns subscribe rate configuration for a topic
	GetSubscribeRateWithContext(context.Context, utils.TopicName) (*utils.SubscribeRate, error)

	// SetSubscribeRate sets subscribe rate configuration for a topic
	SetSubscribeRate(utils.TopicName, utils.SubscribeRate) error

	// SetSubscribeRateWithContext sets subscribe rate configuration for a topic
	SetSubscribeRateWithContext(context.Context, utils.TopicName, utils.SubscribeRate) error

	// RemoveSubscribeRate removes subscribe rate configuration for a topic
	RemoveSubscribeRate(utils.TopicName) error

	// RemoveSubscribeRateWithContext removes subscribe rate configuration for a topic
	RemoveSubscribeRateWithContext(context.Context, utils.TopicName) error

	// GetSubscriptionDispatchRate returns subscription dispatch rate for a topic
	GetSubscriptionDispatchRate(utils.TopicName) (*utils.DispatchRateData, error)

	// GetSubscriptionDispatchRateWithContext returns subscription dispatch rate for a topic
	GetSubscriptionDispatchRateWithContext(context.Context, utils.TopicName) (*utils.DispatchRateData, error)

	// SetSubscriptionDispatchRate sets subscription dispatch rate for a topic
	SetSubscriptionDispatchRate(utils.TopicName, utils.DispatchRateData) error

	// SetSubscriptionDispatchRateWithContext sets subscription dispatch rate for a topic
	SetSubscriptionDispatchRateWithContext(context.Context, utils.TopicName, utils.DispatchRateData) error

	// RemoveSubscriptionDispatchRate removes subscription dispatch rate for a topic
	RemoveSubscriptionDispatchRate(utils.TopicName) error

	// RemoveSubscriptionDispatchRateWithContext removes subscription dispatch rate for a topic
	RemoveSubscriptionDispatchRateWithContext(context.Context, utils.TopicName) error

	// GetMaxConsumersPerSubscription returns max consumers per subscription for a topic
	GetMaxConsumersPerSubscription(utils.TopicName) (int, error)

	// GetMaxConsumersPerSubscriptionWithContext returns max consumers per subscription for a topic
	GetMaxConsumersPerSubscriptionWithContext(context.Context, utils.TopicName) (int, error)

	// SetMaxConsumersPerSubscription sets max consumers per subscription for a topic
	SetMaxConsumersPerSubscription(utils.TopicName, int) error

	// SetMaxConsumersPerSubscriptionWithContext sets max consumers per subscription for a topic
	SetMaxConsumersPerSubscriptionWithContext(context.Context, utils.TopicName, int) error

	// RemoveMaxConsumersPerSubscription removes max consumers per subscription for a topic
	RemoveMaxConsumersPerSubscription(utils.TopicName) error

	// RemoveMaxConsumersPerSubscriptionWithContext removes max consumers per subscription for a topic
	RemoveMaxConsumersPerSubscriptionWithContext(context.Context, utils.TopicName) error

	// GetMaxMessageSize returns max message size for a topic
	GetMaxMessageSize(utils.TopicName) (int, error)

	// GetMaxMessageSizeWithContext returns max message size for a topic
	GetMaxMessageSizeWithContext(context.Context, utils.TopicName) (int, error)

	// SetMaxMessageSize sets max message size for a topic
	SetMaxMessageSize(utils.TopicName, int) error

	// SetMaxMessageSizeWithContext sets max message size for a topic
	SetMaxMessageSizeWithContext(context.Context, utils.TopicName, int) error

	// RemoveMaxMessageSize removes max message size for a topic
	RemoveMaxMessageSize(utils.TopicName) error

	// RemoveMaxMessageSizeWithContext removes max message size for a topic
	RemoveMaxMessageSizeWithContext(context.Context, utils.TopicName) error

	// GetMaxSubscriptionsPerTopic returns max subscriptions per topic
	GetMaxSubscriptionsPerTopic(utils.TopicName) (int, error)

	// GetMaxSubscriptionsPerTopicWithContext returns max subscriptions per topic
	GetMaxSubscriptionsPerTopicWithContext(context.Context, utils.TopicName) (int, error)

	// SetMaxSubscriptionsPerTopic sets max subscriptions per topic
	SetMaxSubscriptionsPerTopic(utils.TopicName, int) error

	// SetMaxSubscriptionsPerTopicWithContext sets max subscriptions per topic
	SetMaxSubscriptionsPerTopicWithContext(context.Context, utils.TopicName, int) error

	// RemoveMaxSubscriptionsPerTopic removes max subscriptions per topic
	RemoveMaxSubscriptionsPerTopic(utils.TopicName) error

	// RemoveMaxSubscriptionsPerTopicWithContext removes max subscriptions per topic
	RemoveMaxSubscriptionsPerTopicWithContext(context.Context, utils.TopicName) error

	// GetSchemaValidationEnforced returns schema validation enforced flag for a topic
	GetSchemaValidationEnforced(utils.TopicName) (bool, error)

	// GetSchemaValidationEnforcedWithContext returns schema validation enforced flag for a topic
	GetSchemaValidationEnforcedWithContext(context.Context, utils.TopicName) (bool, error)

	// SetSchemaValidationEnforced sets schema validation enforced flag for a topic
	SetSchemaValidationEnforced(utils.TopicName, bool) error

	// SetSchemaValidationEnforcedWithContext sets schema validation enforced flag for a topic
	SetSchemaValidationEnforcedWithContext(context.Context, utils.TopicName, bool) error

	// RemoveSchemaValidationEnforced removes schema validation enforced flag for a topic
	RemoveSchemaValidationEnforced(utils.TopicName) error

	// RemoveSchemaValidationEnforcedWithContext removes schema validation enforced flag for a topic
	RemoveSchemaValidationEnforcedWithContext(context.Context, utils.TopicName) error

	// GetDeduplicationSnapshotInterval returns deduplication snapshot interval for a topic
	GetDeduplicationSnapshotInterval(utils.TopicName) (int, error)

	// GetDeduplicationSnapshotIntervalWithContext returns deduplication snapshot interval for a topic
	GetDeduplicationSnapshotIntervalWithContext(context.Context, utils.TopicName) (int, error)

	// SetDeduplicationSnapshotInterval sets deduplication snapshot interval for a topic
	SetDeduplicationSnapshotInterval(utils.TopicName, int) error

	// SetDeduplicationSnapshotIntervalWithContext sets deduplication snapshot interval for a topic
	SetDeduplicationSnapshotIntervalWithContext(context.Context, utils.TopicName, int) error

	// RemoveDeduplicationSnapshotInterval removes deduplication snapshot interval for a topic
	RemoveDeduplicationSnapshotInterval(utils.TopicName) error

	// RemoveDeduplicationSnapshotIntervalWithContext removes deduplication snapshot interval for a topic
	RemoveDeduplicationSnapshotIntervalWithContext(context.Context, utils.TopicName) error

	// GetReplicatorDispatchRate returns replicator dispatch rate for a topic
	GetReplicatorDispatchRate(utils.TopicName) (*utils.DispatchRateData, error)

	// GetReplicatorDispatchRateWithContext returns replicator dispatch rate for a topic
	GetReplicatorDispatchRateWithContext(context.Context, utils.TopicName) (*utils.DispatchRateData, error)

	// SetReplicatorDispatchRate sets replicator dispatch rate for a topic
	SetReplicatorDispatchRate(utils.TopicName, utils.DispatchRateData) error

	// SetReplicatorDispatchRateWithContext sets replicator dispatch rate for a topic
	SetReplicatorDispatchRateWithContext(context.Context, utils.TopicName, utils.DispatchRateData) error

	// RemoveReplicatorDispatchRate removes replicator dispatch rate for a topic
	RemoveReplicatorDispatchRate(utils.TopicName) error

	// RemoveReplicatorDispatchRateWithContext removes replicator dispatch rate for a topic
	RemoveReplicatorDispatchRateWithContext(context.Context, utils.TopicName) error

	// GetOffloadPolicies returns offload policies for a topic
	GetOffloadPolicies(utils.TopicName) (*utils.OffloadPolicies, error)

	// GetOffloadPoliciesWithContext returns offload policies for a topic
	GetOffloadPoliciesWithContext(context.Context, utils.TopicName) (*utils.OffloadPolicies, error)

	// SetOffloadPolicies sets offload policies for a topic
	SetOffloadPolicies(utils.TopicName, utils.OffloadPolicies) error

	// SetOffloadPoliciesWithContext sets offload policies for a topic
	SetOffloadPoliciesWithContext(context.Context, utils.TopicName, utils.OffloadPolicies) error

	// RemoveOffloadPolicies removes offload policies for a topic
	RemoveOffloadPolicies(utils.TopicName) error

	// RemoveOffloadPoliciesWithContext removes offload policies for a topic
	RemoveOffloadPoliciesWithContext(context.Context, utils.TopicName) error

	// GetAutoSubscriptionCreation returns auto subscription creation override for a topic
	GetAutoSubscriptionCreation(utils.TopicName) (*utils.AutoSubscriptionCreationOverride, error)

	// GetAutoSubscriptionCreationWithContext returns auto subscription creation override for a topic
	GetAutoSubscriptionCreationWithContext(context.Context, utils.TopicName) (*utils.AutoSubscriptionCreationOverride, error)

	// SetAutoSubscriptionCreation sets auto subscription creation override for a topic
	SetAutoSubscriptionCreation(utils.TopicName,
		utils.AutoSubscriptionCreationOverride) error

	// SetAutoSubscriptionCreationWithContext sets auto subscription creation override for a topic
	SetAutoSubscriptionCreationWithContext(context.Context, utils.TopicName,
		utils.AutoSubscriptionCreationOverride) error

	// RemoveAutoSubscriptionCreation Remove auto subscription creation override for a topic
	RemoveAutoSubscriptionCreation(utils.TopicName) error

	// RemoveAutoSubscriptionCreationWithContext Remove auto subscription creation override for a topic
	RemoveAutoSubscriptionCreationWithContext(context.Context, utils.TopicName) error

	// GetSchemaCompatibilityStrategy returns schema compatibility strategy for a topic
	GetSchemaCompatibilityStrategy(utils.TopicName) (utils.SchemaCompatibilityStrategy, error)

	// GetSchemaCompatibilityStrategyWithContext returns schema compatibility strategy for a topic
	GetSchemaCompatibilityStrategyWithContext(context.Context, utils.TopicName) (utils.SchemaCompatibilityStrategy, error)

	// SetSchemaCompatibilityStrategy sets schema compatibility strategy for a topic
	SetSchemaCompatibilityStrategy(utils.TopicName,
		utils.SchemaCompatibilityStrategy) error

	// SetSchemaCompatibilityStrategyWithContext sets schema compatibility strategy for a topic
	SetSchemaCompatibilityStrategyWithContext(context.Context, utils.TopicName,
		utils.SchemaCompatibilityStrategy) error

	// RemoveSchemaCompatibilityStrategy removes schema compatibility strategy for a topic
	RemoveSchemaCompatibilityStrategy(utils.TopicName) error

	// RemoveSchemaCompatibilityStrategyWithContext removes schema compatibility strategy for a topic
	RemoveSchemaCompatibilityStrategyWithContext(context.Context, utils.TopicName) error
}

type topics struct {
	pulsar            *pulsarClient
	basePath          string
	persistentPath    string
	nonPersistentPath string
	lookupPath        string
}

// Check whether the topics struct implements the Topics interface.
var _ Topics = &topics{}

// Topics is used to access the topics endpoints
func (c *pulsarClient) Topics() Topics {
	return &topics{
		pulsar:            c,
		basePath:          "",
		persistentPath:    "/persistent",
		nonPersistentPath: "/non-persistent",
		lookupPath:        "/lookup/v2/topic",
	}
}

func (t *topics) Create(topic utils.TopicName, partitions int) error {
	return t.CreateWithContext(context.Background(), topic, partitions)
}

func (t *topics) CreateWithContext(ctx context.Context, topic utils.TopicName, partitions int) error {
	return t.CreateWithPropertiesWithContext(ctx, topic, partitions, nil)
}

func (t *topics) CreateWithProperties(topic utils.TopicName, partitions int, meta map[string]string) error {
	return t.CreateWithPropertiesWithContext(context.Background(), topic, partitions, meta)
}

func (t *topics) CreateWithPropertiesWithContext(ctx context.Context, topic utils.TopicName, partitions int, meta map[string]string) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "partitions")
	if partitions == 0 {
		endpoint = t.pulsar.endpoint(t.basePath, topic.GetRestPath())
		return t.pulsar.Client.Put(ctx, endpoint, meta)
	}
	data := struct {
		Meta       map[string]string `json:"properties"`
		Partitions int               `json:"partitions"`
	}{
		Meta:       meta,
		Partitions: partitions,
	}
	return t.pulsar.Client.PutWithCustomMediaType(ctx, endpoint, &data, nil, nil, rest.PartitionedTopicMetaJSON)
}

func (t *topics) GetProperties(topic utils.TopicName) (map[string]string, error) {
	return t.GetPropertiesWithContext(context.Background(), topic)
}

func (t *topics) GetPropertiesWithContext(ctx context.Context, topic utils.TopicName) (map[string]string, error) {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "properties")
	var properties map[string]string
	err := t.pulsar.Client.Get(ctx, endpoint, &properties)
	return properties, err
}

func (t *topics) UpdateProperties(topic utils.TopicName, properties map[string]string) error {
	return t.UpdatePropertiesWithContext(context.Background(), topic, properties)
}

func (t *topics) UpdatePropertiesWithContext(ctx context.Context, topic utils.TopicName, properties map[string]string) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "properties")
	return t.pulsar.Client.Put(ctx, endpoint, properties)
}

func (t *topics) RemoveProperty(topic utils.TopicName, key string) error {
	return t.RemovePropertyWithContext(context.Background(), topic, key)
}

func (t *topics) RemovePropertyWithContext(ctx context.Context, topic utils.TopicName, key string) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "properties")
	return t.pulsar.Client.DeleteWithQueryParams(ctx, endpoint, map[string]string{"key": key})
}

func (t *topics) Delete(topic utils.TopicName, force bool, nonPartitioned bool) error {
	return t.DeleteWithContext(context.Background(), topic, force, nonPartitioned)
}

func (t *topics) DeleteWithContext(ctx context.Context, topic utils.TopicName, force bool, nonPartitioned bool) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "partitions")
	if nonPartitioned {
		endpoint = t.pulsar.endpoint(t.basePath, topic.GetRestPath())
	}
	params := map[string]string{
		"force": strconv.FormatBool(force),
	}
	return t.pulsar.Client.DeleteWithQueryParams(ctx, endpoint, params)
}

func (t *topics) Update(topic utils.TopicName, partitions int) error {
	return t.UpdateWithContext(context.Background(), topic, partitions)
}

func (t *topics) UpdateWithContext(ctx context.Context, topic utils.TopicName, partitions int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "partitions")
	return t.pulsar.Client.Post(ctx, endpoint, partitions)
}

func (t *topics) GetMetadata(topic utils.TopicName) (utils.PartitionedTopicMetadata, error) {
	return t.GetMetadataWithContext(context.Background(), topic)
}

func (t *topics) GetMetadataWithContext(ctx context.Context, topic utils.TopicName) (utils.PartitionedTopicMetadata, error) {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "partitions")
	var partitionedMeta utils.PartitionedTopicMetadata
	err := t.pulsar.Client.Get(ctx, endpoint, &partitionedMeta)
	return partitionedMeta, err
}

func (t *topics) List(namespace utils.NameSpaceName) ([]string, []string, error) {
	return t.ListWithContext(context.Background(), namespace)
}

func (t *topics) ListWithContext(ctx context.Context, namespace utils.NameSpaceName) ([]string, []string, error) {
	var partitionedTopics, nonPartitionedTopics []string
	partitionedTopicsChan := make(chan []string)
	nonPartitionedTopicsChan := make(chan []string)
	errChan := make(chan error)

	pp := t.pulsar.endpoint(t.persistentPath, namespace.String(), "partitioned")
	np := t.pulsar.endpoint(t.nonPersistentPath, namespace.String(), "partitioned")
	p := t.pulsar.endpoint(t.persistentPath, namespace.String())
	n := t.pulsar.endpoint(t.nonPersistentPath, namespace.String())

	go t.getTopics(ctx, pp, partitionedTopicsChan, errChan)
	go t.getTopics(ctx, np, partitionedTopicsChan, errChan)
	go t.getTopics(ctx, p, nonPartitionedTopicsChan, errChan)
	go t.getTopics(ctx, n, nonPartitionedTopicsChan, errChan)

	requestCount := 4
	for {
		select {
		case err := <-errChan:
			if err != nil {
				return nil, nil, err
			}
			continue
		case pTopic := <-partitionedTopicsChan:
			requestCount--
			partitionedTopics = append(partitionedTopics, pTopic...)
		case npTopic := <-nonPartitionedTopicsChan:
			requestCount--
			nonPartitionedTopics = append(nonPartitionedTopics, npTopic...)
		}
		if requestCount == 0 {
			break
		}
	}
	return partitionedTopics, nonPartitionedTopics, nil
}

func (t *topics) getTopics(ctx context.Context, endpoint string, out chan<- []string, err chan<- error) {
	var topics []string
	err <- t.pulsar.Client.Get(ctx, endpoint, &topics)
	out <- topics
}

func (t *topics) GetInternalInfo(topic utils.TopicName) (utils.ManagedLedgerInfo, error) {
	return t.GetInternalInfoWithContext(context.Background(), topic)
}

func (t *topics) GetInternalInfoWithContext(ctx context.Context, topic utils.TopicName) (utils.ManagedLedgerInfo, error) {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "internal-info")
	var info utils.ManagedLedgerInfo
	err := t.pulsar.Client.Get(ctx, endpoint, &info)
	return info, err
}

func (t *topics) GetPermissions(topic utils.TopicName) (map[string][]utils.AuthAction, error) {
	return t.GetPermissionsWithContext(context.Background(), topic)
}

func (t *topics) GetPermissionsWithContext(ctx context.Context, topic utils.TopicName) (map[string][]utils.AuthAction, error) {
	var permissions map[string][]utils.AuthAction
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "permissions")
	err := t.pulsar.Client.Get(ctx, endpoint, &permissions)
	return permissions, err
}

func (t *topics) GrantPermission(topic utils.TopicName, role string, action []utils.AuthAction) error {
	return t.GrantPermissionWithContext(context.Background(), topic, role, action)
}

func (t *topics) GrantPermissionWithContext(ctx context.Context, topic utils.TopicName, role string, action []utils.AuthAction) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "permissions", role)
	s := []string{}
	for _, v := range action {
		s = append(s, v.String())
	}
	return t.pulsar.Client.Post(ctx, endpoint, s)
}

func (t *topics) RevokePermission(topic utils.TopicName, role string) error {
	return t.RevokePermissionWithContext(context.Background(), topic, role)
}

func (t *topics) RevokePermissionWithContext(ctx context.Context, topic utils.TopicName, role string) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "permissions", role)
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) Lookup(topic utils.TopicName) (utils.LookupData, error) {
	return t.LookupWithContext(context.Background(), topic)
}

func (t *topics) LookupWithContext(ctx context.Context, topic utils.TopicName) (utils.LookupData, error) {
	var lookup utils.LookupData
	endpoint := fmt.Sprintf("%s/%s", t.lookupPath, topic.GetRestPath())
	err := t.pulsar.Client.Get(ctx, endpoint, &lookup)
	return lookup, err
}

func (t *topics) GetBundleRange(topic utils.TopicName) (string, error) {
	return t.GetBundleRangeWithContext(context.Background(), topic)
}

func (t *topics) GetBundleRangeWithContext(ctx context.Context, topic utils.TopicName) (string, error) {
	endpoint := fmt.Sprintf("%s/%s/%s", t.lookupPath, topic.GetRestPath(), "bundle")
	data, err := t.pulsar.Client.GetWithQueryParams(ctx, endpoint, nil, nil, false)
	return string(data), err
}

func (t *topics) GetLastMessageID(topic utils.TopicName) (utils.MessageID, error) {
	return t.GetLastMessageIDWithContext(context.Background(), topic)
}

func (t *topics) GetLastMessageIDWithContext(ctx context.Context, topic utils.TopicName) (utils.MessageID, error) {
	var messageID utils.MessageID
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "lastMessageId")
	err := t.pulsar.Client.Get(ctx, endpoint, &messageID)
	return messageID, err
}

func (t *topics) GetMessageID(topic utils.TopicName, timestamp int64) (utils.MessageID, error) {
	return t.GetMessageIDWithContext(context.Background(), topic, timestamp)
}

func (t *topics) GetMessageIDWithContext(ctx context.Context, topic utils.TopicName, timestamp int64) (utils.MessageID, error) {
	var messageID utils.MessageID
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "messageid", strconv.FormatInt(timestamp, 10))
	err := t.pulsar.Client.Get(ctx, endpoint, &messageID)
	return messageID, err
}

func (t *topics) GetStats(topic utils.TopicName) (utils.TopicStats, error) {
	return t.GetStatsWithContext(context.Background(), topic)
}

func (t *topics) GetStatsWithContext(ctx context.Context, topic utils.TopicName) (utils.TopicStats, error) {
	var stats utils.TopicStats
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "stats")
	err := t.pulsar.Client.Get(ctx, endpoint, &stats)
	return stats, err
}

func (t *topics) GetStatsWithOption(topic utils.TopicName, option utils.GetStatsOptions) (utils.TopicStats, error) {
	return t.GetStatsWithOptionWithContext(context.Background(), topic, option)
}

func (t *topics) GetStatsWithOptionWithContext(ctx context.Context, topic utils.TopicName, option utils.GetStatsOptions) (utils.TopicStats, error) {
	var stats utils.TopicStats
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "stats")
	params := map[string]string{
		"getPreciseBacklog":        strconv.FormatBool(option.GetPreciseBacklog),
		"subscriptionBacklogSize":  strconv.FormatBool(option.SubscriptionBacklogSize),
		"getEarliestTimeInBacklog": strconv.FormatBool(option.GetEarliestTimeInBacklog),
		"excludePublishers":        strconv.FormatBool(option.ExcludePublishers),
		"excludeConsumers":         strconv.FormatBool(option.ExcludeConsumers),
	}
	_, err := t.pulsar.Client.GetWithQueryParams(ctx, endpoint, &stats, params, true)
	return stats, err
}

func (t *topics) GetInternalStats(topic utils.TopicName) (utils.PersistentTopicInternalStats, error) {
	return t.GetInternalStatsWithContext(context.Background(), topic)
}

func (t *topics) GetInternalStatsWithContext(ctx context.Context, topic utils.TopicName) (utils.PersistentTopicInternalStats, error) {
	var stats utils.PersistentTopicInternalStats
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "internalStats")
	err := t.pulsar.Client.Get(ctx, endpoint, &stats)
	return stats, err
}

func (t *topics) GetPartitionedStats(topic utils.TopicName, perPartition bool) (utils.PartitionedTopicStats, error) {
	return t.GetPartitionedStatsWithContext(context.Background(), topic, perPartition)
}

func (t *topics) GetPartitionedStatsWithContext(ctx context.Context, topic utils.TopicName, perPartition bool) (utils.PartitionedTopicStats, error) {
	var stats utils.PartitionedTopicStats
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "partitioned-stats")
	params := map[string]string{
		"perPartition": strconv.FormatBool(perPartition),
	}
	_, err := t.pulsar.Client.GetWithQueryParams(ctx, endpoint, &stats, params, true)
	return stats, err
}

func (t *topics) GetPartitionedStatsWithOption(topic utils.TopicName, perPartition bool,
	option utils.GetStatsOptions) (utils.PartitionedTopicStats, error) {
	return t.GetPartitionedStatsWithOptionWithContext(context.Background(), topic, perPartition, option)
}

func (t *topics) GetPartitionedStatsWithOptionWithContext(ctx context.Context, topic utils.TopicName, perPartition bool,
	option utils.GetStatsOptions) (utils.PartitionedTopicStats, error) {
	var stats utils.PartitionedTopicStats
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "partitioned-stats")
	params := map[string]string{
		"perPartition":             strconv.FormatBool(perPartition),
		"getPreciseBacklog":        strconv.FormatBool(option.GetPreciseBacklog),
		"subscriptionBacklogSize":  strconv.FormatBool(option.SubscriptionBacklogSize),
		"getEarliestTimeInBacklog": strconv.FormatBool(option.GetEarliestTimeInBacklog),
		"excludePublishers":        strconv.FormatBool(option.ExcludePublishers),
		"excludeConsumers":         strconv.FormatBool(option.ExcludeConsumers),
	}
	_, err := t.pulsar.Client.GetWithQueryParams(ctx, endpoint, &stats, params, true)
	return stats, err
}

func (t *topics) Terminate(topic utils.TopicName) (utils.MessageID, error) {
	return t.TerminateWithContext(context.Background(), topic)
}

func (t *topics) TerminateWithContext(ctx context.Context, topic utils.TopicName) (utils.MessageID, error) {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "terminate")
	var messageID utils.MessageID
	err := t.pulsar.Client.PostWithObj(ctx, endpoint, nil, &messageID)
	return messageID, err
}

func (t *topics) Offload(topic utils.TopicName, messageID utils.MessageID) error {
	return t.OffloadWithContext(context.Background(), topic, messageID)
}

func (t *topics) OffloadWithContext(ctx context.Context, topic utils.TopicName, messageID utils.MessageID) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "offload")
	return t.pulsar.Client.Put(ctx, endpoint, messageID)
}

func (t *topics) OffloadStatus(topic utils.TopicName) (utils.OffloadProcessStatus, error) {
	return t.OffloadStatusWithContext(context.Background(), topic)
}

func (t *topics) OffloadStatusWithContext(ctx context.Context, topic utils.TopicName) (utils.OffloadProcessStatus, error) {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "offload")
	var status utils.OffloadProcessStatus
	err := t.pulsar.Client.Get(ctx, endpoint, &status)
	return status, err
}

func (t *topics) Unload(topic utils.TopicName) error {
	return t.UnloadWithContext(context.Background(), topic)
}

func (t *topics) UnloadWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "unload")
	return t.pulsar.Client.Put(ctx, endpoint, nil)
}

func (t *topics) Compact(topic utils.TopicName) error {
	return t.CompactWithContext(context.Background(), topic)
}

func (t *topics) CompactWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "compaction")
	return t.pulsar.Client.Put(ctx, endpoint, nil)
}

func (t *topics) CompactStatus(topic utils.TopicName) (utils.LongRunningProcessStatus, error) {
	return t.CompactStatusWithContext(context.Background(), topic)
}

func (t *topics) CompactStatusWithContext(ctx context.Context, topic utils.TopicName) (utils.LongRunningProcessStatus, error) {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "compaction")
	var status utils.LongRunningProcessStatus
	err := t.pulsar.Client.Get(ctx, endpoint, &status)
	return status, err
}

func (t *topics) GetMessageTTL(topic utils.TopicName) (int, error) {
	return t.GetMessageTTLWithContext(context.Background(), topic)
}

func (t *topics) GetMessageTTLWithContext(ctx context.Context, topic utils.TopicName) (int, error) {
	var ttl int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "messageTTL")
	err := t.pulsar.Client.Get(ctx, endpoint, &ttl)
	return ttl, err
}

func (t *topics) SetMessageTTL(topic utils.TopicName, messageTTL int) error {
	return t.SetMessageTTLWithContext(context.Background(), topic, messageTTL)
}

func (t *topics) SetMessageTTLWithContext(ctx context.Context, topic utils.TopicName, messageTTL int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "messageTTL")
	var params = make(map[string]string)
	params["messageTTL"] = strconv.Itoa(messageTTL)
	err := t.pulsar.Client.PostWithQueryParams(ctx, endpoint, nil, params)
	return err
}

func (t *topics) RemoveMessageTTL(topic utils.TopicName) error {
	return t.RemoveMessageTTLWithContext(context.Background(), topic)
}

func (t *topics) RemoveMessageTTLWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "messageTTL")
	var params = make(map[string]string)
	params["messageTTL"] = strconv.Itoa(0)
	err := t.pulsar.Client.DeleteWithQueryParams(ctx, endpoint, params)
	return err
}

func (t *topics) GetMaxProducers(topic utils.TopicName) (int, error) {
	return t.GetMaxProducersWithContext(context.Background(), topic)
}

func (t *topics) GetMaxProducersWithContext(ctx context.Context, topic utils.TopicName) (int, error) {
	var maxProducers int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxProducers")
	err := t.pulsar.Client.Get(ctx, endpoint, &maxProducers)
	return maxProducers, err
}

func (t *topics) SetMaxProducers(topic utils.TopicName, maxProducers int) error {
	return t.SetMaxProducersWithContext(context.Background(), topic, maxProducers)
}

func (t *topics) SetMaxProducersWithContext(ctx context.Context, topic utils.TopicName, maxProducers int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxProducers")
	err := t.pulsar.Client.Post(ctx, endpoint, &maxProducers)
	return err
}

func (t *topics) RemoveMaxProducers(topic utils.TopicName) error {
	return t.RemoveMaxProducersWithContext(context.Background(), topic)
}

func (t *topics) RemoveMaxProducersWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxProducers")
	err := t.pulsar.Client.Delete(ctx, endpoint)
	return err
}

func (t *topics) GetMaxConsumers(topic utils.TopicName) (int, error) {
	return t.GetMaxConsumersWithContext(context.Background(), topic)
}

func (t *topics) GetMaxConsumersWithContext(ctx context.Context, topic utils.TopicName) (int, error) {
	var maxConsumers int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxConsumers")
	err := t.pulsar.Client.Get(ctx, endpoint, &maxConsumers)
	return maxConsumers, err
}

func (t *topics) SetMaxConsumers(topic utils.TopicName, maxConsumers int) error {
	return t.SetMaxConsumersWithContext(context.Background(), topic, maxConsumers)
}

func (t *topics) SetMaxConsumersWithContext(ctx context.Context, topic utils.TopicName, maxConsumers int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxConsumers")
	err := t.pulsar.Client.Post(ctx, endpoint, &maxConsumers)
	return err
}

func (t *topics) RemoveMaxConsumers(topic utils.TopicName) error {
	return t.RemoveMaxConsumersWithContext(context.Background(), topic)
}

func (t *topics) RemoveMaxConsumersWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxConsumers")
	err := t.pulsar.Client.Delete(ctx, endpoint)
	return err
}

func (t *topics) GetMaxUnackMessagesPerConsumer(topic utils.TopicName) (int, error) {
	return t.GetMaxUnackMessagesPerConsumerWithContext(context.Background(), topic)
}

func (t *topics) GetMaxUnackMessagesPerConsumerWithContext(ctx context.Context, topic utils.TopicName) (int, error) {
	var maxNum int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnConsumer")
	err := t.pulsar.Client.Get(ctx, endpoint, &maxNum)
	return maxNum, err
}

func (t *topics) SetMaxUnackMessagesPerConsumer(topic utils.TopicName, maxUnackedNum int) error {
	return t.SetMaxUnackMessagesPerConsumerWithContext(context.Background(), topic, maxUnackedNum)
}

func (t *topics) SetMaxUnackMessagesPerConsumerWithContext(ctx context.Context, topic utils.TopicName, maxUnackedNum int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnConsumer")
	return t.pulsar.Client.Post(ctx, endpoint, &maxUnackedNum)
}

func (t *topics) RemoveMaxUnackMessagesPerConsumer(topic utils.TopicName) error {
	return t.RemoveMaxUnackMessagesPerConsumerWithContext(context.Background(), topic)
}

func (t *topics) RemoveMaxUnackMessagesPerConsumerWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnConsumer")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetMaxUnackMessagesPerSubscription(topic utils.TopicName) (int, error) {
	return t.GetMaxUnackMessagesPerSubscriptionWithContext(context.Background(), topic)
}

func (t *topics) GetMaxUnackMessagesPerSubscriptionWithContext(ctx context.Context, topic utils.TopicName) (int, error) {
	var maxNum int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnSubscription")
	err := t.pulsar.Client.Get(ctx, endpoint, &maxNum)
	return maxNum, err
}

func (t *topics) SetMaxUnackMessagesPerSubscription(topic utils.TopicName, maxUnackedNum int) error {
	return t.SetMaxUnackMessagesPerSubscriptionWithContext(context.Background(), topic, maxUnackedNum)
}

func (t *topics) SetMaxUnackMessagesPerSubscriptionWithContext(ctx context.Context, topic utils.TopicName, maxUnackedNum int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnSubscription")
	return t.pulsar.Client.Post(ctx, endpoint, &maxUnackedNum)
}

func (t *topics) RemoveMaxUnackMessagesPerSubscription(topic utils.TopicName) error {
	return t.RemoveMaxUnackMessagesPerSubscriptionWithContext(context.Background(), topic)
}

func (t *topics) RemoveMaxUnackMessagesPerSubscriptionWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnSubscription")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetPersistence(topic utils.TopicName) (*utils.PersistenceData, error) {
	return t.GetPersistenceWithContext(context.Background(), topic)
}

func (t *topics) GetPersistenceWithContext(ctx context.Context, topic utils.TopicName) (*utils.PersistenceData, error) {
	var persistenceData utils.PersistenceData
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "persistence")
	err := t.pulsar.Client.Get(ctx, endpoint, &persistenceData)
	return &persistenceData, err
}

func (t *topics) SetPersistence(topic utils.TopicName, persistenceData utils.PersistenceData) error {
	return t.SetPersistenceWithContext(context.Background(), topic, persistenceData)
}

func (t *topics) SetPersistenceWithContext(ctx context.Context, topic utils.TopicName, persistenceData utils.PersistenceData) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "persistence")
	return t.pulsar.Client.Post(ctx, endpoint, &persistenceData)
}

func (t *topics) RemovePersistence(topic utils.TopicName) error {
	return t.RemovePersistenceWithContext(context.Background(), topic)
}

func (t *topics) RemovePersistenceWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "persistence")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetDelayedDelivery(topic utils.TopicName) (*utils.DelayedDeliveryData, error) {
	return t.GetDelayedDeliveryWithContext(context.Background(), topic)
}

func (t *topics) GetDelayedDeliveryWithContext(ctx context.Context, topic utils.TopicName) (*utils.DelayedDeliveryData, error) {
	var delayedDeliveryData utils.DelayedDeliveryData
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "delayedDelivery")
	err := t.pulsar.Client.Get(ctx, endpoint, &delayedDeliveryData)
	return &delayedDeliveryData, err
}

func (t *topics) SetDelayedDelivery(topic utils.TopicName, delayedDeliveryData utils.DelayedDeliveryData) error {
	return t.SetDelayedDeliveryWithContext(context.Background(), topic, delayedDeliveryData)
}

func (t *topics) SetDelayedDeliveryWithContext(ctx context.Context, topic utils.TopicName, delayedDeliveryData utils.DelayedDeliveryData) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "delayedDelivery")
	return t.pulsar.Client.Post(ctx, endpoint, &delayedDeliveryData)
}

func (t *topics) RemoveDelayedDelivery(topic utils.TopicName) error {
	return t.RemoveDelayedDeliveryWithContext(context.Background(), topic)
}

func (t *topics) RemoveDelayedDeliveryWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "delayedDelivery")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetDispatchRate(topic utils.TopicName) (*utils.DispatchRateData, error) {
	return t.GetDispatchRateWithContext(context.Background(), topic)
}

func (t *topics) GetDispatchRateWithContext(ctx context.Context, topic utils.TopicName) (*utils.DispatchRateData, error) {
	var dispatchRateData utils.DispatchRateData
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "dispatchRate")
	err := t.pulsar.Client.Get(ctx, endpoint, &dispatchRateData)
	return &dispatchRateData, err
}

func (t *topics) SetDispatchRate(topic utils.TopicName, dispatchRateData utils.DispatchRateData) error {
	return t.SetDispatchRateWithContext(context.Background(), topic, dispatchRateData)
}

func (t *topics) SetDispatchRateWithContext(ctx context.Context, topic utils.TopicName, dispatchRateData utils.DispatchRateData) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "dispatchRate")
	return t.pulsar.Client.Post(ctx, endpoint, &dispatchRateData)
}

func (t *topics) RemoveDispatchRate(topic utils.TopicName) error {
	return t.RemoveDispatchRateWithContext(context.Background(), topic)
}

func (t *topics) RemoveDispatchRateWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "dispatchRate")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetPublishRate(topic utils.TopicName) (*utils.PublishRateData, error) {
	return t.GetPublishRateWithContext(context.Background(), topic)
}

func (t *topics) GetPublishRateWithContext(ctx context.Context, topic utils.TopicName) (*utils.PublishRateData, error) {
	var publishRateData utils.PublishRateData
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "publishRate")
	err := t.pulsar.Client.Get(ctx, endpoint, &publishRateData)
	return &publishRateData, err
}

func (t *topics) SetPublishRate(topic utils.TopicName, publishRateData utils.PublishRateData) error {
	return t.SetPublishRateWithContext(context.Background(), topic, publishRateData)
}

func (t *topics) SetPublishRateWithContext(ctx context.Context, topic utils.TopicName, publishRateData utils.PublishRateData) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "publishRate")
	return t.pulsar.Client.Post(ctx, endpoint, &publishRateData)
}

func (t *topics) RemovePublishRate(topic utils.TopicName) error {
	return t.RemovePublishRateWithContext(context.Background(), topic)
}

func (t *topics) RemovePublishRateWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "publishRate")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetDeduplicationStatus(topic utils.TopicName) (bool, error) {
	return t.GetDeduplicationStatusWithContext(context.Background(), topic)
}

func (t *topics) GetDeduplicationStatusWithContext(ctx context.Context, topic utils.TopicName) (bool, error) {
	var enabled bool
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "deduplicationEnabled")
	err := t.pulsar.Client.Get(ctx, endpoint, &enabled)
	return enabled, err
}

func (t *topics) SetDeduplicationStatus(topic utils.TopicName, enabled bool) error {
	return t.SetDeduplicationStatusWithContext(context.Background(), topic, enabled)
}

func (t *topics) SetDeduplicationStatusWithContext(ctx context.Context, topic utils.TopicName, enabled bool) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "deduplicationEnabled")
	return t.pulsar.Client.Post(ctx, endpoint, enabled)
}

func (t *topics) RemoveDeduplicationStatus(topic utils.TopicName) error {
	return t.RemoveDeduplicationStatusWithContext(context.Background(), topic)
}

func (t *topics) RemoveDeduplicationStatusWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "deduplicationEnabled")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetRetention(topic utils.TopicName, applied bool) (*utils.RetentionPolicies, error) {
	return t.GetRetentionWithContext(context.Background(), topic, applied)
}

func (t *topics) GetRetentionWithContext(ctx context.Context, topic utils.TopicName, applied bool) (*utils.RetentionPolicies, error) {
	var policy utils.RetentionPolicies
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "retention")
	_, err := t.pulsar.Client.GetWithQueryParams(ctx, endpoint, &policy, map[string]string{
		"applied": strconv.FormatBool(applied),
	}, true)
	return &policy, err
}

func (t *topics) RemoveRetention(topic utils.TopicName) error {
	return t.RemoveRetentionWithContext(context.Background(), topic)
}

func (t *topics) RemoveRetentionWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "retention")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) SetRetention(topic utils.TopicName, data utils.RetentionPolicies) error {
	return t.SetRetentionWithContext(context.Background(), topic, data)
}

func (t *topics) SetRetentionWithContext(ctx context.Context, topic utils.TopicName, data utils.RetentionPolicies) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "retention")
	return t.pulsar.Client.Post(ctx, endpoint, data)
}

func (t *topics) GetCompactionThreshold(topic utils.TopicName, applied bool) (int64, error) {
	return t.GetCompactionThresholdWithContext(context.Background(), topic, applied)
}

func (t *topics) GetCompactionThresholdWithContext(ctx context.Context, topic utils.TopicName, applied bool) (int64, error) {
	var threshold int64
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "compactionThreshold")
	_, err := t.pulsar.Client.GetWithQueryParams(ctx, endpoint, &threshold, map[string]string{
		"applied": strconv.FormatBool(applied),
	}, true)
	return threshold, err
}

func (t *topics) SetCompactionThreshold(topic utils.TopicName, threshold int64) error {
	return t.SetCompactionThresholdWithContext(context.Background(), topic, threshold)
}

func (t *topics) SetCompactionThresholdWithContext(ctx context.Context, topic utils.TopicName, threshold int64) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "compactionThreshold")
	err := t.pulsar.Client.Post(ctx, endpoint, threshold)
	return err
}

func (t *topics) RemoveCompactionThreshold(topic utils.TopicName) error {
	return t.RemoveCompactionThresholdWithContext(context.Background(), topic)
}

func (t *topics) RemoveCompactionThresholdWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "compactionThreshold")
	err := t.pulsar.Client.Delete(ctx, endpoint)
	return err
}

func (t *topics) GetBacklogQuotaMap(topic utils.TopicName, applied bool) (map[utils.BacklogQuotaType]utils.BacklogQuota,
	error) {
	return t.GetBacklogQuotaMapWithContext(context.Background(), topic, applied)
}

func (t *topics) GetBacklogQuotaMapWithContext(ctx context.Context, topic utils.TopicName, applied bool) (map[utils.BacklogQuotaType]utils.BacklogQuota,
	error) {
	var backlogQuotaMap map[utils.BacklogQuotaType]utils.BacklogQuota
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "backlogQuotaMap")

	queryParams := map[string]string{"applied": strconv.FormatBool(applied)}
	_, err := t.pulsar.Client.GetWithQueryParams(ctx, endpoint, &backlogQuotaMap, queryParams, true)

	return backlogQuotaMap, err
}

func (t *topics) SetBacklogQuota(topic utils.TopicName, backlogQuota utils.BacklogQuota,
	backlogQuotaType utils.BacklogQuotaType) error {
	return t.SetBacklogQuotaWithContext(context.Background(), topic, backlogQuota, backlogQuotaType)
}

func (t *topics) SetBacklogQuotaWithContext(ctx context.Context, topic utils.TopicName, backlogQuota utils.BacklogQuota,
	backlogQuotaType utils.BacklogQuotaType) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "backlogQuota")
	params := make(map[string]string)
	params["backlogQuotaType"] = string(backlogQuotaType)
	return t.pulsar.Client.PostWithQueryParams(ctx, endpoint, &backlogQuota, params)
}

func (t *topics) RemoveBacklogQuota(topic utils.TopicName, backlogQuotaType utils.BacklogQuotaType) error {
	return t.RemoveBacklogQuotaWithContext(context.Background(), topic, backlogQuotaType)
}

func (t *topics) RemoveBacklogQuotaWithContext(ctx context.Context, topic utils.TopicName, backlogQuotaType utils.BacklogQuotaType) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "backlogQuota")
	return t.pulsar.Client.DeleteWithQueryParams(ctx, endpoint, map[string]string{
		"backlogQuotaType": string(backlogQuotaType),
	})
}

func (t *topics) GetInactiveTopicPolicies(topic utils.TopicName, applied bool) (utils.InactiveTopicPolicies, error) {
	return t.GetInactiveTopicPoliciesWithContext(context.Background(), topic, applied)
}

func (t *topics) GetInactiveTopicPoliciesWithContext(ctx context.Context, topic utils.TopicName, applied bool) (utils.InactiveTopicPolicies, error) {
	var out utils.InactiveTopicPolicies
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "inactiveTopicPolicies")
	_, err := t.pulsar.Client.GetWithQueryParams(ctx, endpoint, &out, map[string]string{
		"applied": strconv.FormatBool(applied),
	}, true)
	return out, err
}

func (t *topics) RemoveInactiveTopicPolicies(topic utils.TopicName) error {
	return t.RemoveInactiveTopicPoliciesWithContext(context.Background(), topic)
}

func (t *topics) RemoveInactiveTopicPoliciesWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "inactiveTopicPolicies")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) SetInactiveTopicPolicies(topic utils.TopicName, data utils.InactiveTopicPolicies) error {
	return t.SetInactiveTopicPoliciesWithContext(context.Background(), topic, data)
}

func (t *topics) SetInactiveTopicPoliciesWithContext(ctx context.Context, topic utils.TopicName, data utils.InactiveTopicPolicies) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "inactiveTopicPolicies")
	return t.pulsar.Client.Post(ctx, endpoint, data)
}

func (t *topics) SetReplicationClusters(topic utils.TopicName, data []string) error {
	return t.SetReplicationClustersWithContext(context.Background(), topic, data)
}

func (t *topics) SetReplicationClustersWithContext(ctx context.Context, topic utils.TopicName, data []string) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "replication")
	return t.pulsar.Client.Post(ctx, endpoint, data)
}

func (t *topics) GetReplicationClusters(topic utils.TopicName) ([]string, error) {
	return t.GetReplicationClustersWithContext(context.Background(), topic)
}

func (t *topics) GetReplicationClustersWithContext(ctx context.Context, topic utils.TopicName) ([]string, error) {
	var data []string
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "replication")
	err := t.pulsar.Client.Get(ctx, endpoint, &data)
	return data, err
}

func (t *topics) GetSubscribeRate(topic utils.TopicName) (*utils.SubscribeRate, error) {
	return t.GetSubscribeRateWithContext(context.Background(), topic)
}

func (t *topics) GetSubscribeRateWithContext(ctx context.Context, topic utils.TopicName) (*utils.SubscribeRate, error) {
	var subscribeRate utils.SubscribeRate
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "subscribeRate")
	err := t.pulsar.Client.Get(ctx, endpoint, &subscribeRate)
	return &subscribeRate, err
}

func (t *topics) SetSubscribeRate(topic utils.TopicName, subscribeRate utils.SubscribeRate) error {
	return t.SetSubscribeRateWithContext(context.Background(), topic, subscribeRate)
}

func (t *topics) SetSubscribeRateWithContext(ctx context.Context, topic utils.TopicName, subscribeRate utils.SubscribeRate) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "subscribeRate")
	return t.pulsar.Client.Post(ctx, endpoint, &subscribeRate)
}

func (t *topics) RemoveSubscribeRate(topic utils.TopicName) error {
	return t.RemoveSubscribeRateWithContext(context.Background(), topic)
}

func (t *topics) RemoveSubscribeRateWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "subscribeRate")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetSubscriptionDispatchRate(topic utils.TopicName) (*utils.DispatchRateData, error) {
	return t.GetSubscriptionDispatchRateWithContext(context.Background(), topic)
}

func (t *topics) GetSubscriptionDispatchRateWithContext(ctx context.Context, topic utils.TopicName) (*utils.DispatchRateData, error) {
	var dispatchRate utils.DispatchRateData
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "subscriptionDispatchRate")
	err := t.pulsar.Client.Get(ctx, endpoint, &dispatchRate)
	return &dispatchRate, err
}

func (t *topics) SetSubscriptionDispatchRate(topic utils.TopicName, dispatchRate utils.DispatchRateData) error {
	return t.SetSubscriptionDispatchRateWithContext(context.Background(), topic, dispatchRate)
}

func (t *topics) SetSubscriptionDispatchRateWithContext(ctx context.Context, topic utils.TopicName, dispatchRate utils.DispatchRateData) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "subscriptionDispatchRate")
	return t.pulsar.Client.Post(ctx, endpoint, &dispatchRate)
}

func (t *topics) RemoveSubscriptionDispatchRate(topic utils.TopicName) error {
	return t.RemoveSubscriptionDispatchRateWithContext(context.Background(), topic)
}

func (t *topics) RemoveSubscriptionDispatchRateWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "subscriptionDispatchRate")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetMaxConsumersPerSubscription(topic utils.TopicName) (int, error) {
	return t.GetMaxConsumersPerSubscriptionWithContext(context.Background(), topic)
}

func (t *topics) GetMaxConsumersPerSubscriptionWithContext(ctx context.Context, topic utils.TopicName) (int, error) {
	var maxConsumers int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxConsumersPerSubscription")
	err := t.pulsar.Client.Get(ctx, endpoint, &maxConsumers)
	return maxConsumers, err
}

func (t *topics) SetMaxConsumersPerSubscription(topic utils.TopicName, maxConsumers int) error {
	return t.SetMaxConsumersPerSubscriptionWithContext(context.Background(), topic, maxConsumers)
}

func (t *topics) SetMaxConsumersPerSubscriptionWithContext(ctx context.Context, topic utils.TopicName, maxConsumers int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxConsumersPerSubscription")
	return t.pulsar.Client.Post(ctx, endpoint, &maxConsumers)
}

func (t *topics) RemoveMaxConsumersPerSubscription(topic utils.TopicName) error {
	return t.RemoveMaxConsumersPerSubscriptionWithContext(context.Background(), topic)
}

func (t *topics) RemoveMaxConsumersPerSubscriptionWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxConsumersPerSubscription")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetMaxMessageSize(topic utils.TopicName) (int, error) {
	return t.GetMaxMessageSizeWithContext(context.Background(), topic)
}

func (t *topics) GetMaxMessageSizeWithContext(ctx context.Context, topic utils.TopicName) (int, error) {
	var maxMessageSize int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxMessageSize")
	err := t.pulsar.Client.Get(ctx, endpoint, &maxMessageSize)
	return maxMessageSize, err
}

func (t *topics) SetMaxMessageSize(topic utils.TopicName, maxMessageSize int) error {
	return t.SetMaxMessageSizeWithContext(context.Background(), topic, maxMessageSize)
}

func (t *topics) SetMaxMessageSizeWithContext(ctx context.Context, topic utils.TopicName, maxMessageSize int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxMessageSize")
	return t.pulsar.Client.Post(ctx, endpoint, &maxMessageSize)
}

func (t *topics) RemoveMaxMessageSize(topic utils.TopicName) error {
	return t.RemoveMaxMessageSizeWithContext(context.Background(), topic)
}

func (t *topics) RemoveMaxMessageSizeWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxMessageSize")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetMaxSubscriptionsPerTopic(topic utils.TopicName) (int, error) {
	return t.GetMaxSubscriptionsPerTopicWithContext(context.Background(), topic)
}

func (t *topics) GetMaxSubscriptionsPerTopicWithContext(ctx context.Context, topic utils.TopicName) (int, error) {
	var maxSubscriptions int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxSubscriptionsPerTopic")
	err := t.pulsar.Client.Get(ctx, endpoint, &maxSubscriptions)
	return maxSubscriptions, err
}

func (t *topics) SetMaxSubscriptionsPerTopic(topic utils.TopicName, maxSubscriptions int) error {
	return t.SetMaxSubscriptionsPerTopicWithContext(context.Background(), topic, maxSubscriptions)
}

func (t *topics) SetMaxSubscriptionsPerTopicWithContext(ctx context.Context, topic utils.TopicName, maxSubscriptions int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxSubscriptionsPerTopic")
	return t.pulsar.Client.Post(ctx, endpoint, &maxSubscriptions)
}

func (t *topics) RemoveMaxSubscriptionsPerTopic(topic utils.TopicName) error {
	return t.RemoveMaxSubscriptionsPerTopicWithContext(context.Background(), topic)
}

func (t *topics) RemoveMaxSubscriptionsPerTopicWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxSubscriptionsPerTopic")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetSchemaValidationEnforced(topic utils.TopicName) (bool, error) {
	return t.GetSchemaValidationEnforcedWithContext(context.Background(), topic)
}

func (t *topics) GetSchemaValidationEnforcedWithContext(ctx context.Context, topic utils.TopicName) (bool, error) {
	var enabled bool
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "schemaValidationEnforced")
	err := t.pulsar.Client.Get(ctx, endpoint, &enabled)
	return enabled, err
}

func (t *topics) SetSchemaValidationEnforced(topic utils.TopicName, enabled bool) error {
	return t.SetSchemaValidationEnforcedWithContext(context.Background(), topic, enabled)
}

func (t *topics) SetSchemaValidationEnforcedWithContext(ctx context.Context, topic utils.TopicName, enabled bool) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "schemaValidationEnforced")
	return t.pulsar.Client.Post(ctx, endpoint, enabled)
}

func (t *topics) RemoveSchemaValidationEnforced(topic utils.TopicName) error {
	return t.RemoveSchemaValidationEnforcedWithContext(context.Background(), topic)
}

func (t *topics) RemoveSchemaValidationEnforcedWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "schemaValidationEnforced")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetDeduplicationSnapshotInterval(topic utils.TopicName) (int, error) {
	return t.GetDeduplicationSnapshotIntervalWithContext(context.Background(), topic)
}

func (t *topics) GetDeduplicationSnapshotIntervalWithContext(ctx context.Context, topic utils.TopicName) (int, error) {
	var interval int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "deduplicationSnapshotInterval")
	err := t.pulsar.Client.Get(ctx, endpoint, &interval)
	return interval, err
}

func (t *topics) SetDeduplicationSnapshotInterval(topic utils.TopicName, interval int) error {
	return t.SetDeduplicationSnapshotIntervalWithContext(context.Background(), topic, interval)
}

func (t *topics) SetDeduplicationSnapshotIntervalWithContext(ctx context.Context, topic utils.TopicName, interval int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "deduplicationSnapshotInterval")
	return t.pulsar.Client.Post(ctx, endpoint, &interval)
}

func (t *topics) RemoveDeduplicationSnapshotInterval(topic utils.TopicName) error {
	return t.RemoveDeduplicationSnapshotIntervalWithContext(context.Background(), topic)
}

func (t *topics) RemoveDeduplicationSnapshotIntervalWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "deduplicationSnapshotInterval")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetReplicatorDispatchRate(topic utils.TopicName) (*utils.DispatchRateData, error) {
	return t.GetReplicatorDispatchRateWithContext(context.Background(), topic)
}

func (t *topics) GetReplicatorDispatchRateWithContext(ctx context.Context, topic utils.TopicName) (*utils.DispatchRateData, error) {
	var dispatchRate utils.DispatchRateData
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "replicatorDispatchRate")
	err := t.pulsar.Client.Get(ctx, endpoint, &dispatchRate)
	return &dispatchRate, err
}

func (t *topics) SetReplicatorDispatchRate(topic utils.TopicName, dispatchRate utils.DispatchRateData) error {
	return t.SetReplicatorDispatchRateWithContext(context.Background(), topic, dispatchRate)
}

func (t *topics) SetReplicatorDispatchRateWithContext(ctx context.Context, topic utils.TopicName, dispatchRate utils.DispatchRateData) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "replicatorDispatchRate")
	return t.pulsar.Client.Post(ctx, endpoint, &dispatchRate)
}

func (t *topics) RemoveReplicatorDispatchRate(topic utils.TopicName) error {
	return t.RemoveReplicatorDispatchRateWithContext(context.Background(), topic)
}

func (t *topics) RemoveReplicatorDispatchRateWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "replicatorDispatchRate")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetAutoSubscriptionCreation(topic utils.TopicName) (*utils.AutoSubscriptionCreationOverride, error) {
	return t.GetAutoSubscriptionCreationWithContext(context.Background(), topic)
}

func (t *topics) GetAutoSubscriptionCreationWithContext(ctx context.Context, topic utils.TopicName) (*utils.AutoSubscriptionCreationOverride, error) {
	var autoSubCreation utils.AutoSubscriptionCreationOverride
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "autoSubscriptionCreation")
	err := t.pulsar.Client.Get(ctx, endpoint, &autoSubCreation)
	return &autoSubCreation, err
}

func (t *topics) SetAutoSubscriptionCreation(topic utils.TopicName,
	autoSubCreation utils.AutoSubscriptionCreationOverride) error {
	return t.SetAutoSubscriptionCreationWithContext(context.Background(), topic, autoSubCreation)
}

func (t *topics) SetAutoSubscriptionCreationWithContext(ctx context.Context, topic utils.TopicName,
	autoSubCreation utils.AutoSubscriptionCreationOverride) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "autoSubscriptionCreation")
	return t.pulsar.Client.Post(ctx, endpoint, &autoSubCreation)
}

func (t *topics) RemoveAutoSubscriptionCreation(topic utils.TopicName) error {
	return t.RemoveAutoSubscriptionCreationWithContext(context.Background(), topic)
}

func (t *topics) RemoveAutoSubscriptionCreationWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "autoSubscriptionCreation")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetSchemaCompatibilityStrategy(topic utils.TopicName) (utils.SchemaCompatibilityStrategy, error) {
	return t.GetSchemaCompatibilityStrategyWithContext(context.Background(), topic)
}

func (t *topics) GetSchemaCompatibilityStrategyWithContext(ctx context.Context, topic utils.TopicName) (utils.SchemaCompatibilityStrategy, error) {
	var strategy utils.SchemaCompatibilityStrategy
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "schemaCompatibilityStrategy")
	err := t.pulsar.Client.Get(ctx, endpoint, &strategy)
	return strategy, err
}

func (t *topics) SetSchemaCompatibilityStrategy(topic utils.TopicName,
	strategy utils.SchemaCompatibilityStrategy) error {
	return t.SetSchemaCompatibilityStrategyWithContext(context.Background(), topic, strategy)
}

func (t *topics) SetSchemaCompatibilityStrategyWithContext(ctx context.Context, topic utils.TopicName,
	strategy utils.SchemaCompatibilityStrategy) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "schemaCompatibilityStrategy")
	return t.pulsar.Client.Put(ctx, endpoint, strategy)
}

func (t *topics) RemoveSchemaCompatibilityStrategy(topic utils.TopicName) error {
	return t.RemoveSchemaCompatibilityStrategyWithContext(context.Background(), topic)
}

func (t *topics) RemoveSchemaCompatibilityStrategyWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "schemaCompatibilityStrategy")
	return t.pulsar.Client.Delete(ctx, endpoint)
}

func (t *topics) GetOffloadPolicies(topic utils.TopicName) (*utils.OffloadPolicies, error) {
	return t.GetOffloadPoliciesWithContext(context.Background(), topic)
}

func (t *topics) GetOffloadPoliciesWithContext(ctx context.Context, topic utils.TopicName) (*utils.OffloadPolicies, error) {
	var offloadPolicies utils.OffloadPolicies
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "offloadPolicies")
	err := t.pulsar.Client.Get(ctx, endpoint, &offloadPolicies)
	return &offloadPolicies, err
}

func (t *topics) SetOffloadPolicies(topic utils.TopicName, offloadPolicies utils.OffloadPolicies) error {
	return t.SetOffloadPoliciesWithContext(context.Background(), topic, offloadPolicies)
}

func (t *topics) SetOffloadPoliciesWithContext(ctx context.Context, topic utils.TopicName, offloadPolicies utils.OffloadPolicies) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "offloadPolicies")
	return t.pulsar.Client.Post(ctx, endpoint, &offloadPolicies)
}

func (t *topics) RemoveOffloadPolicies(topic utils.TopicName) error {
	return t.RemoveOffloadPoliciesWithContext(context.Background(), topic)
}

func (t *topics) RemoveOffloadPoliciesWithContext(ctx context.Context, topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "offloadPolicies")
	return t.pulsar.Client.Delete(ctx, endpoint)
}
