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

type Topics interface {
	Create(TopicName, int) error
	Delete(TopicName, bool, bool) error
	Update(TopicName, int) error
	GetMetadata(TopicName) (PartitionedTopicMetadata, error)
	List(NameSpaceName) ([]string, []string, error)
	GetPermissions(TopicName) (map[string][]AuthAction, error)
	GrantPermission(TopicName, string, []AuthAction) error
	RevokePermission(TopicName, string) error
	Lookup(TopicName) (LookupData, error)
	GetBundleRange(TopicName) (string, error)
	GetLastMessageId(TopicName) (MessageId, error)
	GetStats(TopicName) (TopicStats, error)
	GetInternalStats(TopicName) (PersistentTopicInternalStats, error)
	GetPartitionedStats(TopicName, bool) (PartitionedTopicStats, error)
}

type topics struct {
	client            *client
	basePath          string
	persistentPath    string
	nonPersistentPath string
	lookupPath        string
}

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
	return t.client.put(endpoint, partitions, nil)
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
	return t.client.post(endpoint, partitions, nil)
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

func (t *topics) GetPermissions(topic TopicName) (map[string][]AuthAction, error) {
	var permissions map[string][]AuthAction
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "permissions")
	err := t.client.get(endpoint, &permissions)
	return permissions, err
}

func (t *topics) GrantPermission(topic TopicName, role string, action []AuthAction) error {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "permissions", role)
	var s []string
	for _, v := range action {
		s = append(s, v.String())
	}
	return t.client.post(endpoint, s, nil)
}

func (t *topics) RevokePermission(topic TopicName, role string) error {
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "permissions", role)
	return t.client.delete(endpoint, nil)
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

func (t *topics) GetLastMessageId(topic TopicName) (MessageId, error) {
	var messageId MessageId
	endpoint := t.client.endpoint(t.basePath, topic.GetRestPath(), "lastMessageId")
	err := t.client.get(endpoint, &messageId)
	return messageId, err
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
