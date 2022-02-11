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
	"fmt"
	"strconv"

	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common"
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/utils"
)

// Topics is admin interface for topics management
type Topics interface {
	// Create a topic
	Create(utils.TopicName, int) error

	// Delete a topic
	Delete(utils.TopicName, bool, bool) error

	// Update number of partitions of a non-global partitioned topic
	// It requires partitioned-topic to be already exist and number of new partitions must be greater than existing
	// number of partitions. Decrementing number of partitions requires deletion of topic which is not supported.
	Update(utils.TopicName, int) error

	// GetMetadata returns metadata of a partitioned topic
	GetMetadata(utils.TopicName) (utils.PartitionedTopicMetadata, error)

	// List returns the list of topics under a namespace
	List(utils.NameSpaceName) ([]string, []string, error)

	// GetInternalInfo returns the internal metadata info for the topic
	GetInternalInfo(utils.TopicName) (utils.ManagedLedgerInfo, error)

	// GetPermissions returns permissions on a topic
	// Retrieve the effective permissions for a topic. These permissions are defined by the permissions set at the
	// namespace level combined (union) with any eventual specific permission set on the topic.
	GetPermissions(utils.TopicName) (map[string][]common.AuthAction, error)

	// GrantPermission grants a new permission to a client role on a single topic
	GrantPermission(utils.TopicName, string, []common.AuthAction) error

	// RevokePermission revokes permissions to a client role on a single topic. If the permission
	// was not set at the topic level, but rather at the namespace level, this operation will
	// return an error (HTTP status code 412).
	RevokePermission(utils.TopicName, string) error

	// Lookup a topic returns the broker URL that serves the topic
	Lookup(utils.TopicName) (utils.LookupData, error)

	// GetBundleRange returns a bundle range of a topic
	GetBundleRange(utils.TopicName) (string, error)

	// GetLastMessageID returns the last commit message Id of a topic
	GetLastMessageID(utils.TopicName) (utils.MessageID, error)

	// GetStats returns the stats for the topic
	// All the rates are computed over a 1 minute window and are relative the last completed 1 minute period
	GetStats(utils.TopicName) (utils.TopicStats, error)

	// GetInternalStats returns the internal stats for the topic.
	GetInternalStats(utils.TopicName) (utils.PersistentTopicInternalStats, error)

	// GetPartitionedStats returns the stats for the partitioned topic
	// All the rates are computed over a 1 minute window and are relative the last completed 1 minute period
	GetPartitionedStats(utils.TopicName, bool) (utils.PartitionedTopicStats, error)

	// Terminate the topic and prevent any more messages being published on it
	Terminate(utils.TopicName) (utils.MessageID, error)

	// Offload triggers offloading messages in topic to longterm storage
	Offload(utils.TopicName, utils.MessageID) error

	// OffloadStatus checks the status of an ongoing offloading operation for a topic
	OffloadStatus(utils.TopicName) (utils.OffloadProcessStatus, error)

	// Unload a topic
	Unload(utils.TopicName) error

	// Compact triggers compaction to run for a topic. A single topic can only have one instance of compaction
	// running at any time. Any attempt to trigger another will be met with a ConflictException.
	Compact(utils.TopicName) error

	// CompactStatus checks the status of an ongoing compaction for a topic
	CompactStatus(utils.TopicName) (utils.LongRunningProcessStatus, error)

	// GetMessageTTL Get the message TTL for a topic
	GetMessageTTL(utils.TopicName) (int, error)

	// SetMessageTTL Set the message TTL for a topic
	SetMessageTTL(utils.TopicName, int) error

	// RemoveMessageTTL Remove the message TTL for a topic
	RemoveMessageTTL(utils.TopicName) error

	// GetMaxProducers Get max number of producers for a topic
	GetMaxProducers(utils.TopicName) (int, error)

	// SetMaxProducers Set max number of producers for a topic
	SetMaxProducers(utils.TopicName, int) error

	// RemoveMaxProducers Remove max number of producers for a topic
	RemoveMaxProducers(utils.TopicName) error

	// GetMaxConsumers Get max number of consumers for a topic
	GetMaxConsumers(utils.TopicName) (int, error)

	// SetMaxConsumers Set max number of consumers for a topic
	SetMaxConsumers(utils.TopicName, int) error

	// RemoveMaxConsumers Remove max number of consumers for a topic
	RemoveMaxConsumers(utils.TopicName) error

	// GetMaxUnackMessagesPerConsumer Get max unacked messages policy on consumer for a topic
	GetMaxUnackMessagesPerConsumer(utils.TopicName) (int, error)

	// SetMaxUnackMessagesPerConsumer Set max unacked messages policy on consumer for a topic
	SetMaxUnackMessagesPerConsumer(utils.TopicName, int) error

	// RemoveMaxUnackMessagesPerConsumer Remove max unacked messages policy on consumer for a topic
	RemoveMaxUnackMessagesPerConsumer(utils.TopicName) error

	// GetMaxUnackMessagesPerSubscription Get max unacked messages policy on subscription for a topic
	GetMaxUnackMessagesPerSubscription(utils.TopicName) (int, error)

	// SetMaxUnackMessagesPerSubscription Set max unacked messages policy on subscription for a topic
	SetMaxUnackMessagesPerSubscription(utils.TopicName, int) error

	// RemoveMaxUnackMessagesPerSubscription Remove max unacked messages policy on subscription for a topic
	RemoveMaxUnackMessagesPerSubscription(utils.TopicName) error

	// GetPersistence Get the persistence policies for a topic
	GetPersistence(utils.TopicName) (*utils.PersistenceData, error)

	// SetPersistence Set the persistence policies for a topic
	SetPersistence(utils.TopicName, utils.PersistenceData) error

	// RemovePersistence Remove the persistence policies for a topic
	RemovePersistence(utils.TopicName) error

	// GetDelayedDelivery Get the delayed delivery policy for a topic
	GetDelayedDelivery(utils.TopicName) (*utils.DelayedDeliveryData, error)

	// SetDelayedDelivery Set the delayed delivery policy on a topic
	SetDelayedDelivery(utils.TopicName, utils.DelayedDeliveryData) error

	// RemoveDelayedDelivery Remove the delayed delivery policy on a topic
	RemoveDelayedDelivery(utils.TopicName) error

	// GetDispatchRate Get message dispatch rate for a topic
	GetDispatchRate(utils.TopicName) (*utils.DispatchRateData, error)

	// SetDispatchRate Set message dispatch rate for a topic
	SetDispatchRate(utils.TopicName, utils.DispatchRateData) error

	// RemoveDispatchRate Remove message dispatch rate for a topic
	RemoveDispatchRate(utils.TopicName) error

	// GetPublishRate Get message publish rate for a topic
	GetPublishRate(utils.TopicName) (*utils.PublishRateData, error)

	// SetPublishRate Set message publish rate for a topic
	SetPublishRate(utils.TopicName, utils.PublishRateData) error

	// RemovePublishRate Remove message publish rate for a topic
	RemovePublishRate(utils.TopicName) error

	// GetDeduplicationStatus Get the deduplication policy for a topic
	GetDeduplicationStatus(utils.TopicName) (bool, error)

	// SetDeduplicationStatus Set the deduplication policy for a topic
	SetDeduplicationStatus(utils.TopicName, bool) error

	// RemoveDeduplicationStatus Remove the deduplication policy for a topic
	RemoveDeduplicationStatus(utils.TopicName) error

	// GetRetention returns the retention configuration for a topic
	GetRetention(utils.TopicName, bool) (*utils.RetentionPolicies, error)

	// RemoveRetention removes the retention configuration on a topic
	RemoveRetention(utils.TopicName) error

	// SetRetention sets the retention policy for a topic
	SetRetention(utils.TopicName, utils.RetentionPolicies) error

	// Get the compaction threshold for a topic
	GetCompactionThreshold(topic utils.TopicName, applied bool) (int64, error)

	// Set the compaction threshold for a topic
	SetCompactionThreshold(topic utils.TopicName, threshold int64) error

	// Remove compaction threshold for a topic
	RemoveCompactionThreshold(utils.TopicName) error

	// GetBacklogQuotaMap returns backlog quota map for a topic
	GetBacklogQuotaMap(topic utils.TopicName, applied bool) (map[utils.BacklogQuotaType]utils.BacklogQuota, error)

	// SetBacklogQuota sets a backlog quota for a topic
	SetBacklogQuota(utils.TopicName, utils.BacklogQuota, utils.BacklogQuotaType) error

	// RemoveBacklogQuota removes a backlog quota policy from a topic
	RemoveBacklogQuota(utils.TopicName, utils.BacklogQuotaType) error

	// GetInactiveTopicPolicies gets the inactive topic policies on a topic
	GetInactiveTopicPolicies(topic utils.TopicName, applied bool) (utils.InactiveTopicPolicies, error)

	// RemoveInactiveTopicPolicies removes inactive topic policies from a topic
	RemoveInactiveTopicPolicies(utils.TopicName) error

	// SetInactiveTopicPolicies sets the inactive topic policies on a topic
	SetInactiveTopicPolicies(topic utils.TopicName, data utils.InactiveTopicPolicies) error
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
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "partitions")
	data := &partitions
	if partitions == 0 {
		endpoint = t.pulsar.endpoint(t.basePath, topic.GetRestPath())
		data = nil
	}

	return t.pulsar.Client.Put(endpoint, data)
}

func (t *topics) Delete(topic utils.TopicName, force bool, nonPartitioned bool) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "partitions")
	if nonPartitioned {
		endpoint = t.pulsar.endpoint(t.basePath, topic.GetRestPath())
	}
	params := map[string]string{
		"force": strconv.FormatBool(force),
	}
	return t.pulsar.Client.DeleteWithQueryParams(endpoint, params)
}

func (t *topics) Update(topic utils.TopicName, partitions int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "partitions")
	return t.pulsar.Client.Post(endpoint, partitions)
}

func (t *topics) GetMetadata(topic utils.TopicName) (utils.PartitionedTopicMetadata, error) {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "partitions")
	var partitionedMeta utils.PartitionedTopicMetadata
	err := t.pulsar.Client.Get(endpoint, &partitionedMeta)
	return partitionedMeta, err
}

func (t *topics) List(namespace utils.NameSpaceName) ([]string, []string, error) {
	var partitionedTopics, nonPartitionedTopics []string
	partitionedTopicsChan := make(chan []string)
	nonPartitionedTopicsChan := make(chan []string)
	errChan := make(chan error)

	pp := t.pulsar.endpoint(t.persistentPath, namespace.String(), "partitioned")
	np := t.pulsar.endpoint(t.nonPersistentPath, namespace.String(), "partitioned")
	p := t.pulsar.endpoint(t.persistentPath, namespace.String())
	n := t.pulsar.endpoint(t.nonPersistentPath, namespace.String())

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
	err <- t.pulsar.Client.Get(endpoint, &topics)
	out <- topics
}

func (t *topics) GetInternalInfo(topic utils.TopicName) (utils.ManagedLedgerInfo, error) {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "internal-info")
	var info utils.ManagedLedgerInfo
	err := t.pulsar.Client.Get(endpoint, &info)
	return info, err
}

func (t *topics) GetPermissions(topic utils.TopicName) (map[string][]common.AuthAction, error) {
	var permissions map[string][]common.AuthAction
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "permissions")
	err := t.pulsar.Client.Get(endpoint, &permissions)
	return permissions, err
}

func (t *topics) GrantPermission(topic utils.TopicName, role string, action []common.AuthAction) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "permissions", role)
	s := []string{}
	for _, v := range action {
		s = append(s, v.String())
	}
	return t.pulsar.Client.Post(endpoint, s)
}

func (t *topics) RevokePermission(topic utils.TopicName, role string) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "permissions", role)
	return t.pulsar.Client.Delete(endpoint)
}

func (t *topics) Lookup(topic utils.TopicName) (utils.LookupData, error) {
	var lookup utils.LookupData
	endpoint := fmt.Sprintf("%s/%s", t.lookupPath, topic.GetRestPath())
	err := t.pulsar.Client.Get(endpoint, &lookup)
	return lookup, err
}

func (t *topics) GetBundleRange(topic utils.TopicName) (string, error) {
	endpoint := fmt.Sprintf("%s/%s/%s", t.lookupPath, topic.GetRestPath(), "bundle")
	data, err := t.pulsar.Client.GetWithQueryParams(endpoint, nil, nil, false)
	return string(data), err
}

func (t *topics) GetLastMessageID(topic utils.TopicName) (utils.MessageID, error) {
	var messageID utils.MessageID
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "lastMessageId")
	err := t.pulsar.Client.Get(endpoint, &messageID)
	return messageID, err
}

func (t *topics) GetStats(topic utils.TopicName) (utils.TopicStats, error) {
	var stats utils.TopicStats
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "stats")
	err := t.pulsar.Client.Get(endpoint, &stats)
	return stats, err
}

func (t *topics) GetInternalStats(topic utils.TopicName) (utils.PersistentTopicInternalStats, error) {
	var stats utils.PersistentTopicInternalStats
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "internalStats")
	err := t.pulsar.Client.Get(endpoint, &stats)
	return stats, err
}

func (t *topics) GetPartitionedStats(topic utils.TopicName, perPartition bool) (utils.PartitionedTopicStats, error) {
	var stats utils.PartitionedTopicStats
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "partitioned-stats")
	params := map[string]string{
		"perPartition": strconv.FormatBool(perPartition),
	}
	_, err := t.pulsar.Client.GetWithQueryParams(endpoint, &stats, params, true)
	return stats, err
}

func (t *topics) Terminate(topic utils.TopicName) (utils.MessageID, error) {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "terminate")
	var messageID utils.MessageID
	err := t.pulsar.Client.PostWithObj(endpoint, "", &messageID)
	return messageID, err
}

func (t *topics) Offload(topic utils.TopicName, messageID utils.MessageID) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "offload")
	return t.pulsar.Client.Put(endpoint, messageID)
}

func (t *topics) OffloadStatus(topic utils.TopicName) (utils.OffloadProcessStatus, error) {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "offload")
	var status utils.OffloadProcessStatus
	err := t.pulsar.Client.Get(endpoint, &status)
	return status, err
}

func (t *topics) Unload(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "unload")
	return t.pulsar.Client.Put(endpoint, "")
}

func (t *topics) Compact(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "compaction")
	return t.pulsar.Client.Put(endpoint, "")
}

func (t *topics) CompactStatus(topic utils.TopicName) (utils.LongRunningProcessStatus, error) {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "compaction")
	var status utils.LongRunningProcessStatus
	err := t.pulsar.Client.Get(endpoint, &status)
	return status, err
}

func (t *topics) GetMessageTTL(topic utils.TopicName) (int, error) {
	var ttl int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "messageTTL")
	err := t.pulsar.Client.Get(endpoint, &ttl)
	return ttl, err
}

func (t *topics) SetMessageTTL(topic utils.TopicName, messageTTL int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "messageTTL")
	var params = make(map[string]string)
	params["messageTTL"] = strconv.Itoa(messageTTL)
	err := t.pulsar.Client.PostWithQueryParams(endpoint, nil, params)
	return err
}

func (t *topics) RemoveMessageTTL(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "messageTTL")
	var params = make(map[string]string)
	params["messageTTL"] = strconv.Itoa(0)
	err := t.pulsar.Client.DeleteWithQueryParams(endpoint, params)
	return err
}

func (t *topics) GetMaxProducers(topic utils.TopicName) (int, error) {
	var maxProducers int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxProducers")
	err := t.pulsar.Client.Get(endpoint, &maxProducers)
	return maxProducers, err
}

func (t *topics) SetMaxProducers(topic utils.TopicName, maxProducers int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxProducers")
	err := t.pulsar.Client.Post(endpoint, &maxProducers)
	return err
}

func (t *topics) RemoveMaxProducers(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxProducers")
	err := t.pulsar.Client.Delete(endpoint)
	return err
}

func (t *topics) GetMaxConsumers(topic utils.TopicName) (int, error) {
	var maxConsumers int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxConsumers")
	err := t.pulsar.Client.Get(endpoint, &maxConsumers)
	return maxConsumers, err
}

func (t *topics) SetMaxConsumers(topic utils.TopicName, maxConsumers int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxConsumers")
	err := t.pulsar.Client.Post(endpoint, &maxConsumers)
	return err
}

func (t *topics) RemoveMaxConsumers(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxConsumers")
	err := t.pulsar.Client.Delete(endpoint)
	return err
}

func (t *topics) GetMaxUnackMessagesPerConsumer(topic utils.TopicName) (int, error) {
	var maxNum int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnConsumer")
	err := t.pulsar.Client.Get(endpoint, &maxNum)
	return maxNum, err
}

func (t *topics) SetMaxUnackMessagesPerConsumer(topic utils.TopicName, maxUnackedNum int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnConsumer")
	return t.pulsar.Client.Post(endpoint, &maxUnackedNum)
}

func (t *topics) RemoveMaxUnackMessagesPerConsumer(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnConsumer")
	return t.pulsar.Client.Delete(endpoint)
}

func (t *topics) GetMaxUnackMessagesPerSubscription(topic utils.TopicName) (int, error) {
	var maxNum int
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnSubscription")
	err := t.pulsar.Client.Get(endpoint, &maxNum)
	return maxNum, err
}

func (t *topics) SetMaxUnackMessagesPerSubscription(topic utils.TopicName, maxUnackedNum int) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnSubscription")
	return t.pulsar.Client.Post(endpoint, &maxUnackedNum)
}

func (t *topics) RemoveMaxUnackMessagesPerSubscription(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "maxUnackedMessagesOnSubscription")
	return t.pulsar.Client.Delete(endpoint)
}

func (t *topics) GetPersistence(topic utils.TopicName) (*utils.PersistenceData, error) {
	var persistenceData utils.PersistenceData
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "persistence")
	err := t.pulsar.Client.Get(endpoint, &persistenceData)
	return &persistenceData, err
}

func (t *topics) SetPersistence(topic utils.TopicName, persistenceData utils.PersistenceData) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "persistence")
	return t.pulsar.Client.Post(endpoint, &persistenceData)
}

func (t *topics) RemovePersistence(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "persistence")
	return t.pulsar.Client.Delete(endpoint)
}

func (t *topics) GetDelayedDelivery(topic utils.TopicName) (*utils.DelayedDeliveryData, error) {
	var delayedDeliveryData utils.DelayedDeliveryData
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "delayedDelivery")
	err := t.pulsar.Client.Get(endpoint, &delayedDeliveryData)
	return &delayedDeliveryData, err
}

func (t *topics) SetDelayedDelivery(topic utils.TopicName, delayedDeliveryData utils.DelayedDeliveryData) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "delayedDelivery")
	return t.pulsar.Client.Post(endpoint, &delayedDeliveryData)
}

func (t *topics) RemoveDelayedDelivery(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "delayedDelivery")
	return t.pulsar.Client.Delete(endpoint)
}

func (t *topics) GetDispatchRate(topic utils.TopicName) (*utils.DispatchRateData, error) {
	var dispatchRateData utils.DispatchRateData
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "dispatchRate")
	err := t.pulsar.Client.Get(endpoint, &dispatchRateData)
	return &dispatchRateData, err
}

func (t *topics) SetDispatchRate(topic utils.TopicName, dispatchRateData utils.DispatchRateData) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "dispatchRate")
	return t.pulsar.Client.Post(endpoint, &dispatchRateData)
}

func (t *topics) RemoveDispatchRate(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "dispatchRate")
	return t.pulsar.Client.Delete(endpoint)
}

func (t *topics) GetPublishRate(topic utils.TopicName) (*utils.PublishRateData, error) {
	var publishRateData utils.PublishRateData
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "publishRate")
	err := t.pulsar.Client.Get(endpoint, &publishRateData)
	return &publishRateData, err
}

func (t *topics) SetPublishRate(topic utils.TopicName, publishRateData utils.PublishRateData) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "publishRate")
	return t.pulsar.Client.Post(endpoint, &publishRateData)
}

func (t *topics) RemovePublishRate(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "publishRate")
	return t.pulsar.Client.Delete(endpoint)
}
func (t *topics) GetDeduplicationStatus(topic utils.TopicName) (bool, error) {
	var enabled bool
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "deduplicationEnabled")
	err := t.pulsar.Client.Get(endpoint, &enabled)
	return enabled, err
}

func (t *topics) SetDeduplicationStatus(topic utils.TopicName, enabled bool) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "deduplicationEnabled")
	return t.pulsar.Client.Post(endpoint, enabled)
}
func (t *topics) RemoveDeduplicationStatus(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "deduplicationEnabled")
	return t.pulsar.Client.Delete(endpoint)
}

func (t *topics) GetRetention(topic utils.TopicName, applied bool) (*utils.RetentionPolicies, error) {
	var policy utils.RetentionPolicies
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "retention")
	_, err := t.pulsar.Client.GetWithQueryParams(endpoint, &policy, map[string]string{
		"applied": strconv.FormatBool(applied),
	}, true)
	return &policy, err
}

func (t *topics) RemoveRetention(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "retention")
	return t.pulsar.Client.Delete(endpoint)
}

func (t *topics) SetRetention(topic utils.TopicName, data utils.RetentionPolicies) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "retention")
	return t.pulsar.Client.Post(endpoint, data)
}

func (t *topics) GetCompactionThreshold(topic utils.TopicName, applied bool) (int64, error) {
	var threshold int64
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "compactionThreshold")
	_, err := t.pulsar.Client.GetWithQueryParams(endpoint, &threshold, map[string]string{
		"applied": strconv.FormatBool(applied),
	}, true)
	return threshold, err
}

func (t *topics) SetCompactionThreshold(topic utils.TopicName, threshold int64) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "compactionThreshold")
	err := t.pulsar.Client.Post(endpoint, threshold)
	return err
}

func (t *topics) RemoveCompactionThreshold(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "compactionThreshold")
	err := t.pulsar.Client.Delete(endpoint)
	return err
}

func (t *topics) GetBacklogQuotaMap(topic utils.TopicName, applied bool) (map[utils.BacklogQuotaType]utils.BacklogQuota,
	error) {
	var backlogQuotaMap map[utils.BacklogQuotaType]utils.BacklogQuota
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "backlogQuotaMap")

	queryParams := map[string]string{"applied": strconv.FormatBool(applied)}
	_, err := t.pulsar.Client.GetWithQueryParams(endpoint, &backlogQuotaMap, queryParams, true)

	return backlogQuotaMap, err
}

func (t *topics) SetBacklogQuota(topic utils.TopicName, backlogQuota utils.BacklogQuota,
	backlogQuotaType utils.BacklogQuotaType) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "backlogQuota")
	params := make(map[string]string)
	params["backlogQuotaType"] = string(backlogQuotaType)
	return t.pulsar.Client.PostWithQueryParams(endpoint, &backlogQuota, params)
}

func (t *topics) RemoveBacklogQuota(topic utils.TopicName, backlogQuotaType utils.BacklogQuotaType) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "backlogQuota")
	return t.pulsar.Client.DeleteWithQueryParams(endpoint, map[string]string{
		"backlogQuotaType": string(backlogQuotaType),
	})
}

func (t *topics) GetInactiveTopicPolicies(topic utils.TopicName, applied bool) (utils.InactiveTopicPolicies, error) {
	var out utils.InactiveTopicPolicies
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "inactiveTopicPolicies")
	_, err := t.pulsar.Client.GetWithQueryParams(endpoint, &out, map[string]string{
		"applied": strconv.FormatBool(applied),
	}, true)
	return out, err
}

func (t *topics) RemoveInactiveTopicPolicies(topic utils.TopicName) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "inactiveTopicPolicies")
	return t.pulsar.Client.Delete(endpoint)
}

func (t *topics) SetInactiveTopicPolicies(topic utils.TopicName, data utils.InactiveTopicPolicies) error {
	endpoint := t.pulsar.endpoint(t.basePath, topic.GetRestPath(), "inactiveTopicPolicies")
	return t.pulsar.Client.Post(endpoint, data)
}
