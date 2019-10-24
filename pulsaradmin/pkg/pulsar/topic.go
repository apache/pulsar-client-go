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
}

type topics struct {
	client            *client
	basePath          string
	persistentPath    string
	nonPersistentPath string
	lookupPath        string
}

// Topics is used to access the topics endpoints
func (c *client) Topics() Topics {
	return &topics{
		client:            c,
		basePath:          "",
		persistentPath:    "/persistent",
		nonPersistentPath: "/non-persistent",
		lookupPath:        "/lookup/v2/topic",
	}
}

func (t *topics) Create(topic TopicName, partitions int) error {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "partitions")
	if partitions == 0 {
		endpoint = t.client.endpoint(t.basePath, topic.GetRestPath())
	}
	return t.client.put(endpoint, partitions)
}

func (t *topics) Delete(topic TopicName, force bool, nonPartitioned bool) error {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "partitions")
	if nonPartitioned {
		endpoint = t.client.endpoint(t.basePath, topic.GetRestPath())
	}
	params := map[string]string{
		"force": strconv.FormatBool(force),
	}
	return t.client.deleteWithQueryParams(endpoint, nil, params)
}

func (t *topics) Update(topic TopicName, partitions int) error {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "partitions")
	return t.client.post(endpoint, partitions)
}

func (t *topics) GetMetadata(topic TopicName) (PartitionedTopicMetadata, error) {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "partitions")
	var partitionedMeta PartitionedTopicMetadata
	err := t.client.get(endpoint, &partitionedMeta)
	return partitionedMeta, err
}

func (t *topics) List(namespace NameSpaceName) ([]string, []string, error) {
	var partitionedTopics, nonPartitionedTopics []string
	partitionedTopicsChan := make(chan []string)
	nonPartitionedTopicsChan := make(chan []string)
	errChan := make(chan error)

	pp := t.client.endpoint(t.persistentPath, namespace.String(), "partitioned")
	np := t.client.endpoint(t.nonPersistentPath, namespace.String(), "partitioned")
	p := t.client.endpoint(t.persistentPath, namespace.String())
	n := t.client.endpoint(t.nonPersistentPath, namespace.String())

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
	err <- t.client.get(endpoint, &topics)
	out <- topics
}

func (t *topics) GetInternalInfo(topic TopicName) (ManagedLedgerInfo, error) {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "internal-info")
	var info ManagedLedgerInfo
	err := t.client.get(endpoint, &info)
	return info, err
}

func (t *topics) GetPermissions(topic TopicName) (map[string][]AuthAction, error) {
	var permissions map[string][]AuthAction
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "permissions")
	err := t.client.get(endpoint, &permissions)
	return permissions, err
}

func (t *topics) GrantPermission(topic TopicName, role string, action []AuthAction) error {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "permissions", role)
	s := []string{}
	for _, v := range action {
		s = append(s, v.String())
	}
	return t.client.post(endpoint, s)
}

func (t *topics) RevokePermission(topic TopicName, role string) error {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "permissions", role)
	return t.client.delete(endpoint)
}

func (t *topics) Lookup(topic TopicName) (LookupData, error) {
	var lookup LookupData
	endpoint := fmt.Sprintf("%s/%s", t.lookupPath, topic.GetRestPath())
	err := t.client.get(endpoint, &lookup)
	return lookup, err
}

func (t *topics) GetBundleRange(topic TopicName) (string, error) {
	endpoint := fmt.Sprintf("%s/%s/%s", t.lookupPath, topic.GetRestPath(), "bundle")
	data, err := t.client.getWithQueryParams(endpoint, nil, nil, false)
	return string(data), err
}

func (t *topics) GetLastMessageID(topic TopicName) (MessageID, error) {
	var messageID MessageID
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "lastMessageId")
	err := t.client.get(endpoint, &messageID)
	return messageID, err
}

func (t *topics) GetStats(topic TopicName) (TopicStats, error) {
	var stats TopicStats
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "stats")
	err := t.client.get(endpoint, &stats)
	return stats, err
}

func (t *topics) GetInternalStats(topic TopicName) (PersistentTopicInternalStats, error) {
	var stats PersistentTopicInternalStats
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "internalStats")
	err := t.client.get(endpoint, &stats)
	return stats, err
}

func (t *topics) GetPartitionedStats(topic TopicName, perPartition bool) (PartitionedTopicStats, error) {
	var stats PartitionedTopicStats
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "partitioned-stats")
	params := map[string]string{
		"perPartition": strconv.FormatBool(perPartition),
	}
	_, err := t.client.getWithQueryParams(endpoint, &stats, params, true)
	return stats, err
}

func (t *topics) Terminate(topic TopicName) (MessageID, error) {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "terminate")
	var messageID MessageID
	err := t.client.postWithObj(endpoint, "", &messageID)
	return messageID, err
}

func (t *topics) Offload(topic TopicName, messageID MessageID) error {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "offload")
	return t.client.put(endpoint, messageID)
}

func (t *topics) OffloadStatus(topic TopicName) (OffloadProcessStatus, error) {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "offload")
	var status OffloadProcessStatus
	err := t.client.get(endpoint, &status)
	return status, err
}

func (t *topics) Unload(topic TopicName) error {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "unload")
	return t.client.put(endpoint, "")
}

func (t *topics) Compact(topic TopicName) error {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "compaction")
	return t.client.put(endpoint, "")
}

func (t *topics) CompactStatus(topic TopicName) (LongRunningProcessStatus, error) {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "compaction")
	var status LongRunningProcessStatus
	err := t.client.get(endpoint, &status)
	return status, err
}
