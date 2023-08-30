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
	"fmt"
	"strconv"
)

// Topics is admin interface for topics management
type Topics interface {
	// Create a topic
	Create(TopicName, int) error

	// Delete a topic
	Delete(TopicName, bool, bool) error

	// Update number of partitions of a non-global partitioned topic
	// It requires partitioned-topic to be already exist and number of new partitions must be greater than existing
	// number of partitions. Decrementing number of partitions requires deletion of topic which is not supported.
	Update(TopicName, int) error

	// GetMetadata returns metadata of a partitioned topic
	GetMetadata(TopicName) (PartitionedTopicMetadata, error)

	// List returns the list of topics under a namespace
	List(NameSpaceName) ([]string, []string, error)

	// GetInternalInfo returns the internal metadata info for the topic
	GetInternalInfo(TopicName) (ManagedLedgerInfo, error)

	// GetPermissions returns permissions on a topic
	// Retrieve the effective permissions for a topic. These permissions are defined by the permissions set at the
	// namespace level combined (union) with any eventual specific permission set on the topic.
	GetPermissions(TopicName) (map[string][]AuthAction, error)

	// GrantPermission grants a new permission to a client role on a single topic
	GrantPermission(TopicName, string, []AuthAction) error

	// RevokePermission revokes permissions to a client role on a single topic. If the permission
	// was not set at the topic level, but rather at the namespace level, this operation will
	// return an error (HTTP status code 412).
	RevokePermission(TopicName, string) error

	// Lookup a topic returns the broker URL that serves the topic
	Lookup(TopicName) (LookupData, error)

	// GetBundleRange returns a bundle range of a topic
	GetBundleRange(TopicName) (string, error)

	// GetLastMessageID returns the last commit message Id of a topic
	GetLastMessageID(TopicName) (MessageID, error)

	// GetMessageID returns the message Id by timestamp(ms) of a topic
	GetMessageID(TopicName, int64) (MessageID, error)

	// GetStats returns the stats for the topic
	// All the rates are computed over a 1 minute window and are relative the last completed 1 minute period
	GetStats(TopicName) (TopicStats, error)

	// GetInternalStats returns the internal stats for the topic.
	GetInternalStats(TopicName) (PersistentTopicInternalStats, error)

	// GetPartitionedStats returns the stats for the partitioned topic
	// All the rates are computed over a 1 minute window and are relative the last completed 1 minute period
	GetPartitionedStats(TopicName, bool) (PartitionedTopicStats, error)

	// Terminate the topic and prevent any more messages being published on it
	Terminate(TopicName) (MessageID, error)

	// Offload triggers offloading messages in topic to longterm storage
	Offload(TopicName, MessageID) error

	// OffloadStatus checks the status of an ongoing offloading operation for a topic
	OffloadStatus(TopicName) (OffloadProcessStatus, error)

	// Unload a topic
	Unload(TopicName) error

	// Compact triggers compaction to run for a topic. A single topic can only have one instance of compaction
	// running at any time. Any attempt to trigger another will be met with a ConflictException.
	Compact(TopicName) error

	// CompactStatus checks the status of an ongoing compaction for a topic
	CompactStatus(TopicName) (LongRunningProcessStatus, error)

	// GetMessageTTL Get the message TTL for a topic
	GetMessageTTL(TopicName) (int, error)

	// SetMessageTTL Set the message TTL for a topic
	SetMessageTTL(TopicName, int) error

	// RemoveMessageTTL Remove the message TTL for a topic
	RemoveMessageTTL(TopicName) error

	// GetMaxProducers Get max number of producers for a topic
	GetMaxProducers(TopicName) (int, error)

	// SetMaxProducers Set max number of producers for a topic
	SetMaxProducers(TopicName, int) error

	// RemoveMaxProducers Remove max number of producers for a topic
	RemoveMaxProducers(TopicName) error

	// GetMaxConsumers Get max number of consumers for a topic
	GetMaxConsumers(TopicName) (int, error)

	// SetMaxConsumers Set max number of consumers for a topic
	SetMaxConsumers(TopicName, int) error

	// RemoveMaxConsumers Remove max number of consumers for a topic
	RemoveMaxConsumers(TopicName) error

	// GetMaxUnackMessagesPerConsumer Get max unacked messages policy on consumer for a topic
	GetMaxUnackMessagesPerConsumer(TopicName) (int, error)

	// SetMaxUnackMessagesPerConsumer Set max unacked messages policy on consumer for a topic
	SetMaxUnackMessagesPerConsumer(TopicName, int) error

	// RemoveMaxUnackMessagesPerConsumer Remove max unacked messages policy on consumer for a topic
	RemoveMaxUnackMessagesPerConsumer(TopicName) error

	// GetMaxUnackMessagesPerSubscription Get max unacked messages policy on subscription for a topic
	GetMaxUnackMessagesPerSubscription(TopicName) (int, error)

	// SetMaxUnackMessagesPerSubscription Set max unacked messages policy on subscription for a topic
	SetMaxUnackMessagesPerSubscription(TopicName, int) error

	// RemoveMaxUnackMessagesPerSubscription Remove max unacked messages policy on subscription for a topic
	RemoveMaxUnackMessagesPerSubscription(TopicName) error

	// GetPersistence Get the persistence policies for a topic
	GetPersistence(TopicName) (*PersistenceData, error)

	// SetPersistence Set the persistence policies for a topic
	SetPersistence(TopicName, PersistenceData) error

	// RemovePersistence Remove the persistence policies for a topic
	RemovePersistence(TopicName) error

	// GetDelayedDelivery Get the delayed delivery policy for a topic
	GetDelayedDelivery(TopicName) (*DelayedDeliveryData, error)

	// SetDelayedDelivery Set the delayed delivery policy on a topic
	SetDelayedDelivery(TopicName, DelayedDeliveryData) error

	// RemoveDelayedDelivery Remove the delayed delivery policy on a topic
	RemoveDelayedDelivery(TopicName) error

	// GetDispatchRate Get message dispatch rate for a topic
	GetDispatchRate(TopicName) (*DispatchRateData, error)

	// SetDispatchRate Set message dispatch rate for a topic
	SetDispatchRate(TopicName, DispatchRateData) error

	// RemoveDispatchRate Remove message dispatch rate for a topic
	RemoveDispatchRate(TopicName) error

	// GetPublishRate Get message publish rate for a topic
	GetPublishRate(TopicName) (*PublishRateData, error)

	// SetPublishRate Set message publish rate for a topic
	SetPublishRate(TopicName, PublishRateData) error

	// RemovePublishRate Remove message publish rate for a topic
	RemovePublishRate(TopicName) error

	// GetDeduplicationStatus Get the deduplication policy for a topic
	GetDeduplicationStatus(TopicName) (bool, error)

	// SetDeduplicationStatus Set the deduplication policy for a topic
	SetDeduplicationStatus(TopicName, bool) error

	// RemoveDeduplicationStatus Remove the deduplication policy for a topic
	RemoveDeduplicationStatus(TopicName) error

	// GetRetention returns the retention configuration for a topic
	GetRetention(TopicName, bool) (*RetentionPolicies, error)

	// RemoveRetention removes the retention configuration on a topic
	RemoveRetention(TopicName) error

	// SetRetention sets the retention policy for a topic
	SetRetention(TopicName, RetentionPolicies) error

	// Get the compaction threshold for a topic
	GetCompactionThreshold(topic TopicName, applied bool) (int64, error)

	// Set the compaction threshold for a topic
	SetCompactionThreshold(topic TopicName, threshold int64) error

	// Remove compaction threshold for a topic
	RemoveCompactionThreshold(TopicName) error

	// GetBacklogQuotaMap returns backlog quota map for a topic
	GetBacklogQuotaMap(topic TopicName, applied bool) (map[BacklogQuotaType]BacklogQuota, error)

	// SetBacklogQuota sets a backlog quota for a topic
	SetBacklogQuota(TopicName, BacklogQuota, BacklogQuotaType) error

	// RemoveBacklogQuota removes a backlog quota policy from a topic
	RemoveBacklogQuota(TopicName, BacklogQuotaType) error

	// GetInactiveTopicPolicies gets the inactive topic policies on a topic
	GetInactiveTopicPolicies(topic TopicName, applied bool) (InactiveTopicPolicies, error)

	// RemoveInactiveTopicPolicies removes inactive topic policies from a topic
	RemoveInactiveTopicPolicies(TopicName) error

	// SetInactiveTopicPolicies sets the inactive topic policies on a topic
	SetInactiveTopicPolicies(topic TopicName, data InactiveTopicPolicies) error

	// GetReplicationClusters get the replication clusters of a topic
	GetReplicationClusters(topic TopicName) ([]string, error)

	// SetReplicationClusters sets the replication clusters on a topic
	SetReplicationClusters(topic TopicName, data []string) error
}

type topics struct {
	pulsar            *pulsarClient
	basePath          string
	persistentPath    string
	nonPersistentPath string
	lookupPath        string
	apiVersion        APIVersion
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
		lookupPath:        "/lookup/" + c.apiProfile.Topics.String() + "/topic",
		apiVersion:        c.apiProfile.Topics,
	}
}

func (t *topics) Create(topic TopicName, partitions int) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "partitions")
	data := &partitions
	if partitions == 0 {
		endpoint = t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath())
		data = nil
	}

	return t.pulsar.restClient.Put(endpoint, data)
}

func (t *topics) Delete(topic TopicName, force bool, nonPartitioned bool) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "partitions")
	if nonPartitioned {
		endpoint = t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath())
	}
	params := map[string]string{
		"force": strconv.FormatBool(force),
	}
	return t.pulsar.restClient.DeleteWithQueryParams(endpoint, params)
}

func (t *topics) Update(topic TopicName, partitions int) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "partitions")
	return t.pulsar.restClient.Post(endpoint, partitions)
}

func (t *topics) GetMetadata(topic TopicName) (PartitionedTopicMetadata, error) {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "partitions")
	var partitionedMeta PartitionedTopicMetadata
	err := t.pulsar.restClient.Get(endpoint, &partitionedMeta)
	return partitionedMeta, err
}

func (t *topics) List(namespace NameSpaceName) ([]string, []string, error) {
	var partitionedTopics, nonPartitionedTopics []string
	partitionedTopicsChan := make(chan []string)
	nonPartitionedTopicsChan := make(chan []string)
	errChan := make(chan error)

	pp := t.pulsar.endpoint(t.apiVersion, t.persistentPath, namespace.String(), "partitioned")
	np := t.pulsar.endpoint(t.apiVersion, t.nonPersistentPath, namespace.String(), "partitioned")
	p := t.pulsar.endpoint(t.apiVersion, t.persistentPath, namespace.String())
	n := t.pulsar.endpoint(t.apiVersion, t.nonPersistentPath, namespace.String())

	go t.getTopics(pp, partitionedTopicsChan, errChan)
	go t.getTopics(np, partitionedTopicsChan, errChan)
	go t.getTopics(p, nonPartitionedTopicsChan, errChan)
	go t.getTopics(n, nonPartitionedTopicsChan, errChan)

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

func (t *topics) getTopics(endpoint string, out chan<- []string, err chan<- error) {
	var topics []string
	err <- t.pulsar.restClient.Get(endpoint, &topics)
	out <- topics
}

func (t *topics) GetInternalInfo(topic TopicName) (ManagedLedgerInfo, error) {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "internal-info")
	var info ManagedLedgerInfo
	err := t.pulsar.restClient.Get(endpoint, &info)
	return info, err
}

func (t *topics) GetPermissions(topic TopicName) (map[string][]AuthAction, error) {
	var permissions map[string][]AuthAction
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "permissions")
	err := t.pulsar.restClient.Get(endpoint, &permissions)
	return permissions, err
}

func (t *topics) GrantPermission(topic TopicName, role string, action []AuthAction) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "permissions", role)
	s := []string{}
	for _, v := range action {
		s = append(s, v.String())
	}
	return t.pulsar.restClient.Post(endpoint, s)
}

func (t *topics) RevokePermission(topic TopicName, role string) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "permissions", role)
	return t.pulsar.restClient.Delete(endpoint)
}

func (t *topics) Lookup(topic TopicName) (LookupData, error) {
	var lookup LookupData
	endpoint := fmt.Sprintf("%s/%s", t.lookupPath, topic.GetRestPath())
	err := t.pulsar.restClient.Get(endpoint, &lookup)
	return lookup, err
}

func (t *topics) GetBundleRange(topic TopicName) (string, error) {
	endpoint := fmt.Sprintf("%s/%s/%s", t.lookupPath, topic.GetRestPath(), "bundle")
	data, err := t.pulsar.restClient.GetWithQueryParams(endpoint, nil, nil, false)
	return string(data), err
}

func (t *topics) GetLastMessageID(topic TopicName) (MessageID, error) {
	var messageID MessageID
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "lastMessageId")
	err := t.pulsar.restClient.Get(endpoint, &messageID)
	return messageID, err
}

func (t *topics) GetMessageID(topic TopicName, timestamp int64) (MessageID, error) {
	var messageID MessageID
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "messageid",
		strconv.FormatInt(timestamp, 10))
	err := t.pulsar.restClient.Get(endpoint, &messageID)
	return messageID, err
}

func (t *topics) GetStats(topic TopicName) (TopicStats, error) {
	var stats TopicStats
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "stats")
	err := t.pulsar.restClient.Get(endpoint, &stats)
	return stats, err
}

func (t *topics) GetInternalStats(topic TopicName) (PersistentTopicInternalStats, error) {
	var stats PersistentTopicInternalStats
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "internalStats")
	err := t.pulsar.restClient.Get(endpoint, &stats)
	return stats, err
}

func (t *topics) GetPartitionedStats(topic TopicName, perPartition bool) (PartitionedTopicStats, error) {
	var stats PartitionedTopicStats
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "partitioned-stats")
	params := map[string]string{
		"perPartition": strconv.FormatBool(perPartition),
	}
	_, err := t.pulsar.restClient.GetWithQueryParams(endpoint, &stats, params, true)
	return stats, err
}

func (t *topics) Terminate(topic TopicName) (MessageID, error) {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "terminate")
	var messageID MessageID
	err := t.pulsar.restClient.PostWithObj(endpoint, nil, &messageID)
	return messageID, err
}

func (t *topics) Offload(topic TopicName, messageID MessageID) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "offload")
	return t.pulsar.restClient.Put(endpoint, messageID)
}

func (t *topics) OffloadStatus(topic TopicName) (OffloadProcessStatus, error) {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "offload")
	var status OffloadProcessStatus
	err := t.pulsar.restClient.Get(endpoint, &status)
	return status, err
}

func (t *topics) Unload(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "unload")
	return t.pulsar.restClient.Put(endpoint, nil)
}

func (t *topics) Compact(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "compaction")
	return t.pulsar.restClient.Put(endpoint, nil)
}

func (t *topics) CompactStatus(topic TopicName) (LongRunningProcessStatus, error) {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "compaction")
	var status LongRunningProcessStatus
	err := t.pulsar.restClient.Get(endpoint, &status)
	return status, err
}

func (t *topics) GetMessageTTL(topic TopicName) (int, error) {
	var ttl int
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "messageTTL")
	err := t.pulsar.restClient.Get(endpoint, &ttl)
	return ttl, err
}

func (t *topics) SetMessageTTL(topic TopicName, messageTTL int) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "messageTTL")
	params := make(map[string]string)
	params["messageTTL"] = strconv.Itoa(messageTTL)
	err := t.pulsar.restClient.PostWithQueryParams(endpoint, nil, params)
	return err
}

func (t *topics) RemoveMessageTTL(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "messageTTL")
	params := make(map[string]string)
	params["messageTTL"] = strconv.Itoa(0)
	err := t.pulsar.restClient.DeleteWithQueryParams(endpoint, params)
	return err
}

func (t *topics) GetMaxProducers(topic TopicName) (int, error) {
	var maxProducers int
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "maxProducers")
	err := t.pulsar.restClient.Get(endpoint, &maxProducers)
	return maxProducers, err
}

func (t *topics) SetMaxProducers(topic TopicName, maxProducers int) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "maxProducers")
	err := t.pulsar.restClient.Post(endpoint, &maxProducers)
	return err
}

func (t *topics) RemoveMaxProducers(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "maxProducers")
	err := t.pulsar.restClient.Delete(endpoint)
	return err
}

func (t *topics) GetMaxConsumers(topic TopicName) (int, error) {
	var maxConsumers int
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "maxConsumers")
	err := t.pulsar.restClient.Get(endpoint, &maxConsumers)
	return maxConsumers, err
}

func (t *topics) SetMaxConsumers(topic TopicName, maxConsumers int) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "maxConsumers")
	err := t.pulsar.restClient.Post(endpoint, &maxConsumers)
	return err
}

func (t *topics) RemoveMaxConsumers(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "maxConsumers")
	err := t.pulsar.restClient.Delete(endpoint)
	return err
}

func (t *topics) GetMaxUnackMessagesPerConsumer(topic TopicName) (int, error) {
	var maxNum int
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnConsumer")
	err := t.pulsar.restClient.Get(endpoint, &maxNum)
	return maxNum, err
}

func (t *topics) SetMaxUnackMessagesPerConsumer(topic TopicName, maxUnackedNum int) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnConsumer")
	return t.pulsar.restClient.Post(endpoint, &maxUnackedNum)
}

func (t *topics) RemoveMaxUnackMessagesPerConsumer(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnConsumer")
	return t.pulsar.restClient.Delete(endpoint)
}

func (t *topics) GetMaxUnackMessagesPerSubscription(topic TopicName) (int, error) {
	var maxNum int
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnSubscription")
	err := t.pulsar.restClient.Get(endpoint, &maxNum)
	return maxNum, err
}

func (t *topics) SetMaxUnackMessagesPerSubscription(topic TopicName, maxUnackedNum int) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnSubscription")
	return t.pulsar.restClient.Post(endpoint, &maxUnackedNum)
}

func (t *topics) RemoveMaxUnackMessagesPerSubscription(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnSubscription")
	return t.pulsar.restClient.Delete(endpoint)
}

func (t *topics) GetPersistence(topic TopicName) (*PersistenceData, error) {
	var persistenceData PersistenceData
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "persistence")
	err := t.pulsar.restClient.Get(endpoint, &persistenceData)
	return &persistenceData, err
}

func (t *topics) SetPersistence(topic TopicName, persistenceData PersistenceData) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "persistence")
	return t.pulsar.restClient.Post(endpoint, &persistenceData)
}

func (t *topics) RemovePersistence(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "persistence")
	return t.pulsar.restClient.Delete(endpoint)
}

func (t *topics) GetDelayedDelivery(topic TopicName) (*DelayedDeliveryData, error) {
	var delayedDeliveryData DelayedDeliveryData
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "delayedDelivery")
	err := t.pulsar.restClient.Get(endpoint, &delayedDeliveryData)
	return &delayedDeliveryData, err
}

func (t *topics) SetDelayedDelivery(topic TopicName, delayedDeliveryData DelayedDeliveryData) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "delayedDelivery")
	return t.pulsar.restClient.Post(endpoint, &delayedDeliveryData)
}

func (t *topics) RemoveDelayedDelivery(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "delayedDelivery")
	return t.pulsar.restClient.Delete(endpoint)
}

func (t *topics) GetDispatchRate(topic TopicName) (*DispatchRateData, error) {
	var dispatchRateData DispatchRateData
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "dispatchRate")
	err := t.pulsar.restClient.Get(endpoint, &dispatchRateData)
	return &dispatchRateData, err
}

func (t *topics) SetDispatchRate(topic TopicName, dispatchRateData DispatchRateData) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "dispatchRate")
	return t.pulsar.restClient.Post(endpoint, &dispatchRateData)
}

func (t *topics) RemoveDispatchRate(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "dispatchRate")
	return t.pulsar.restClient.Delete(endpoint)
}

func (t *topics) GetPublishRate(topic TopicName) (*PublishRateData, error) {
	var publishRateData PublishRateData
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "publishRate")
	err := t.pulsar.restClient.Get(endpoint, &publishRateData)
	return &publishRateData, err
}

func (t *topics) SetPublishRate(topic TopicName, publishRateData PublishRateData) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "publishRate")
	return t.pulsar.restClient.Post(endpoint, &publishRateData)
}

func (t *topics) RemovePublishRate(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "publishRate")
	return t.pulsar.restClient.Delete(endpoint)
}

func (t *topics) GetDeduplicationStatus(topic TopicName) (bool, error) {
	var enabled bool
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "deduplicationEnabled")
	err := t.pulsar.restClient.Get(endpoint, &enabled)
	return enabled, err
}

func (t *topics) SetDeduplicationStatus(topic TopicName, enabled bool) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "deduplicationEnabled")
	return t.pulsar.restClient.Post(endpoint, enabled)
}

func (t *topics) RemoveDeduplicationStatus(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "deduplicationEnabled")
	return t.pulsar.restClient.Delete(endpoint)
}

func (t *topics) GetRetention(topic TopicName, applied bool) (*RetentionPolicies, error) {
	var policy RetentionPolicies
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "retention")
	_, err := t.pulsar.restClient.GetWithQueryParams(endpoint, &policy, map[string]string{
		"applied": strconv.FormatBool(applied),
	}, true)
	return &policy, err
}

func (t *topics) RemoveRetention(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "retention")
	return t.pulsar.restClient.Delete(endpoint)
}

func (t *topics) SetRetention(topic TopicName, data RetentionPolicies) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "retention")
	return t.pulsar.restClient.Post(endpoint, data)
}

func (t *topics) GetCompactionThreshold(topic TopicName, applied bool) (int64, error) {
	var threshold int64
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "compactionThreshold")
	_, err := t.pulsar.restClient.GetWithQueryParams(endpoint, &threshold, map[string]string{
		"applied": strconv.FormatBool(applied),
	}, true)
	return threshold, err
}

func (t *topics) SetCompactionThreshold(topic TopicName, threshold int64) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "compactionThreshold")
	err := t.pulsar.restClient.Post(endpoint, threshold)
	return err
}

func (t *topics) RemoveCompactionThreshold(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "compactionThreshold")
	err := t.pulsar.restClient.Delete(endpoint)
	return err
}

func (t *topics) GetBacklogQuotaMap(topic TopicName, applied bool) (map[BacklogQuotaType]BacklogQuota,
	error,
) {
	var backlogQuotaMap map[BacklogQuotaType]BacklogQuota
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "backlogQuotaMap")

	queryParams := map[string]string{"applied": strconv.FormatBool(applied)}
	_, err := t.pulsar.restClient.GetWithQueryParams(endpoint, &backlogQuotaMap, queryParams, true)

	return backlogQuotaMap, err
}

func (t *topics) SetBacklogQuota(topic TopicName, backlogQuota BacklogQuota,
	backlogQuotaType BacklogQuotaType,
) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "backlogQuota")
	params := make(map[string]string)
	params["backlogQuotaType"] = string(backlogQuotaType)
	return t.pulsar.restClient.PostWithQueryParams(endpoint, &backlogQuota, params)
}

func (t *topics) RemoveBacklogQuota(topic TopicName, backlogQuotaType BacklogQuotaType) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "backlogQuota")
	return t.pulsar.restClient.DeleteWithQueryParams(endpoint, map[string]string{
		"backlogQuotaType": string(backlogQuotaType),
	})
}

func (t *topics) GetInactiveTopicPolicies(topic TopicName, applied bool) (InactiveTopicPolicies, error) {
	var out InactiveTopicPolicies
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "inactiveTopicPolicies")
	_, err := t.pulsar.restClient.GetWithQueryParams(endpoint, &out, map[string]string{
		"applied": strconv.FormatBool(applied),
	}, true)
	return out, err
}

func (t *topics) RemoveInactiveTopicPolicies(topic TopicName) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "inactiveTopicPolicies")
	return t.pulsar.restClient.Delete(endpoint)
}

func (t *topics) SetInactiveTopicPolicies(topic TopicName, data InactiveTopicPolicies) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "inactiveTopicPolicies")
	return t.pulsar.restClient.Post(endpoint, data)
}

func (t *topics) SetReplicationClusters(topic TopicName, data []string) error {
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "replication")
	return t.pulsar.restClient.Post(endpoint, data)
}

func (t *topics) GetReplicationClusters(topic TopicName) ([]string, error) {
	var data []string
	endpoint := t.pulsar.endpoint(t.apiVersion, t.basePath, topic.GetRestPath(), "replication")
	err := t.pulsar.restClient.Get(endpoint, &data)
	return data, err
}
