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

package padmin

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type NonPersistentTopics interface {
	WithOptions(opts ...Option) NonPersistentTopics
	Topics
	TopicBacklog
	TopicMessageTTL
	TopicCompaction
}

// GetTopicBacklogQuota Get backlog quota map on a topic.
func (n *NonPersistentTopicsImpl) GetTopicBacklogQuota(tenant, namespace, topic string) (*BacklogQuotaResp, error) {
	resp, err := n.cli.Get(fmt.Sprintf(UrlNonPersistentTopicGetBacklogQuotaMapFormat, tenant, namespace, topic))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body = new(BacklogQuotaResp)
	if err := EasyReader(resp, body); err != nil {
		return nil, err
	}
	return body, nil
}

// SetTopicBacklogQuota Set a backlog quota for a topic.
func (n *NonPersistentTopicsImpl) SetTopicBacklogQuota(tenant, namespace, topic string, cfg *BacklogQuota) error {
	url := fmt.Sprintf(UrlNonPersistentTopicOperateBacklogQuotaFormat, tenant, namespace, topic)
	resp, err := n.cli.Post(url, cfg)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

// RemoveTopicBacklogQuota Remove a backlog quota policy from a topic.
func (n *NonPersistentTopicsImpl) RemoveTopicBacklogQuota(tenant, namespace, topic string, opts ...Option) error {
	if len(opts) != 0 {
		n.WithOptions(opts...)
	}
	url := fmt.Sprintf(UrlNonPersistentTopicOperateBacklogQuotaFormat, tenant, namespace, topic)
	if n.options.backlogQuotaType != "" {
		url = fmt.Sprintf("%s?backlogQuotaType=", n.options.backlogQuotaType)
	}
	resp, err := n.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

// EstimatedOfflineTopicBacklog Get estimated backlog for offline topic.
func (n *NonPersistentTopicsImpl) EstimatedOfflineTopicBacklog(tenant, namespace, topic string) (*OfflineTopicStats, error) {
	resp, err := n.cli.Get(fmt.Sprintf(UrlNonPersistentTopicEstimatedOfflineBacklogFormat, tenant, namespace, topic))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body = new(OfflineTopicStats)
	if err := EasyReader(resp, body); err != nil {
		return nil, err
	}
	return body, nil

}

// CalculateBacklogSizeByMessageID Calculate backlog size by a message ID (in bytes).
func (n *NonPersistentTopicsImpl) CalculateBacklogSizeByMessageID(tenant, namespace, topic string) error {
	resp, err := n.cli.Get(fmt.Sprintf(UrlNonPersistentTopicCalculateBacklogSizeByMessageIDFormat, tenant, namespace, topic))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return HttpCheck(resp)
}

type NonPersistentTopicsImpl struct {
	cli HttpClient
	options
}

func (n *NonPersistentTopicsImpl) WithOptions(opts ...Option) NonPersistentTopics {
	for _, opt := range opts {
		opt(&n.options)
	}
	return n
}

func newNonPersistentTopics(cli HttpClient) *NonPersistentTopicsImpl {
	return &NonPersistentTopicsImpl{cli: cli}
}

func (n *NonPersistentTopicsImpl) CreateNonPartitioned(tenant, namespace, topic string) error {
	path := fmt.Sprintf(UrlNonPersistentTopicFormat, tenant, namespace, topic)
	resp, err := n.cli.Put(path, nil)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NonPersistentTopicsImpl) DeleteNonPartitioned(tenant, namespace, topic string) error {
	path := fmt.Sprintf(UrlNonPersistentTopicFormat, tenant, namespace, topic)
	resp, err := n.cli.Delete(path)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NonPersistentTopicsImpl) ListNonPartitioned(tenant, namespace string) ([]string, error) {
	path := fmt.Sprintf(UrlNonPersistentNamespaceFormat, tenant, namespace)
	resp, err := n.cli.Get(path)
	if err != nil {
		return nil, err
	}
	data, err := HttpCheckReadBytes(resp)
	if err != nil {
		return nil, err
	}
	topics := make([]string, 0)
	err = json.Unmarshal(data, &topics)
	if err != nil {
		return nil, err
	}
	return topics, nil
}

func (n *NonPersistentTopicsImpl) CreatePartitioned(tenant, namespace, topic string, numPartitions int) error {
	path := fmt.Sprintf(UrlNonPersistentPartitionedTopicFormat, tenant, namespace, topic)
	resp, err := n.cli.Put(path, numPartitions)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NonPersistentTopicsImpl) ListPartitioned(tenant, namespace string) ([]string, error) {
	path := fmt.Sprintf(UrlNonPersistentPartitionedNamespaceFormat, tenant, namespace)
	resp, err := n.cli.Get(path)
	if err != nil {
		return nil, err
	}
	var topics []string
	if err := EasyReader(resp, &topics); err != nil {
		return nil, err
	}
	return topics, nil
}

func (n *NonPersistentTopicsImpl) DeletePartitioned(tenant, namespace, topic string) error {
	path := fmt.Sprintf(UrlNonPersistentPartitionedTopicFormat, tenant, namespace, topic)
	resp, err := n.cli.Delete(path)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NonPersistentTopicsImpl) ListNamespaceTopics(tenant, namespace string) ([]string, error) {
	url := fmt.Sprintf(UrlNonPersistentNamespaceFormat, tenant, namespace)
	resp, err := n.cli.Get(url)
	if err != nil {
		return nil, err
	}
	var topics []string
	if err := EasyReader(resp, &topics); err != nil {
		return nil, err
	}
	return topics, nil
}

func (n *NonPersistentTopicsImpl) GetPartitionedMetadata(tenant, namespace, topic string) (*PartitionedMetadata, error) {
	url := fmt.Sprintf(UrlNonPersistentPartitionedTopicFormat, tenant, namespace, topic)
	resp, err := n.cli.Get(url)
	if err != nil {
		return nil, err
	}
	var metadata = new(PartitionedMetadata)
	if err := EasyReader(resp, metadata); err != nil {
		return nil, err
	}
	return metadata, nil
}

func (n *NonPersistentTopicsImpl) GetTopicRetention(tenant, namespace, topic string) (*RetentionConfiguration, error) {
	url := fmt.Sprintf(UrlNonPersistentPartitionedRetentionFormat, tenant, namespace, topic)
	resp, err := n.cli.Get(url)
	if err != nil {
		return nil, err
	}
	var retention = new(RetentionConfiguration)
	if err := EasyReader(resp, retention); err != nil {
		return nil, err
	}
	return retention, nil
}

func (n *NonPersistentTopicsImpl) SetTopicRetention(tenant, namespace, topic string, cfg *RetentionConfiguration) error {
	if cfg == nil {
		return fmt.Errorf("config empty")
	}
	url := fmt.Sprintf(UrlNonPersistentPartitionedRetentionFormat, tenant, namespace, topic)
	resp, err := n.cli.Post(url, cfg)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NonPersistentTopicsImpl) RemoveTopicRetention(tenant, namespace, topic string) error {
	url := fmt.Sprintf(UrlNonPersistentPartitionedRetentionFormat, tenant, namespace, topic)
	resp, err := n.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NonPersistentTopicsImpl) GetTopicMessageTTL(tenant, namespace, topic string) (int64, error) {
	url := fmt.Sprintf(UrlNonPersistentTopicMessageTTLFormat, tenant, namespace, topic)
	resp, err := n.cli.Get(url)
	if err != nil {
		return 0, err
	}
	res, err := ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return 0, nil
	}
	i, err := strconv.ParseInt(res, 10, 64)
	if err != nil {
		return 0, err
	}
	return i, nil
}

func (n *NonPersistentTopicsImpl) SetTopicMessageTTL(tenant, namespace, topic string, seconds int64) error {
	url := fmt.Sprintf(UrlNonPersistentTopicMessageTTLFormat, tenant, namespace, topic)
	resp, err := n.cli.Post(url, seconds)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NonPersistentTopicsImpl) RemoveTopicMessageTTL(tenant, namespace, topic string) error {
	url := fmt.Sprintf(UrlNonPersistentTopicMessageTTLFormat, tenant, namespace, topic)
	resp, err := n.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NonPersistentTopicsImpl) GetTopicCompactionThreshold(tenant, namespace, topic string) (int64, error) {
	url := fmt.Sprintf(UrlNonPersistentTopicCompactionThresholdFormat, tenant, namespace, topic)
	resp, err := n.cli.Get(url)
	if err != nil {
		return 0, err
	}
	res, err := ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return 0, nil
	}
	i, err := strconv.ParseInt(res, 10, 64)
	if err != nil {
		return 0, err
	}
	return i, nil
}

func (n *NonPersistentTopicsImpl) SetTopicCompactionThreshold(tenant, namespace, topic string, threshold int64) error {
	url := fmt.Sprintf(UrlPersistentTopicCompactionThresholdFormat, tenant, namespace, topic)
	resp, err := n.cli.Post(url, threshold)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NonPersistentTopicsImpl) RemoveTopicCompactionThreshold(tenant, namespace, topic string) error {
	url := fmt.Sprintf(UrlPersistentTopicCompactionThresholdFormat, tenant, namespace, topic)
	resp, err := n.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NonPersistentTopicsImpl) GetTopicCompactionStatus(tenant, namespace, topic string) (*LongRunningProcessStatus, error) {
	url := fmt.Sprintf(UrlNonPersistentTopicCompactionFormat, tenant, namespace, topic)
	resp, err := n.cli.Get(url)
	if err != nil {
		return nil, err
	}
	var body = new(LongRunningProcessStatus)
	if err := EasyReader(resp, body); err != nil {
		return nil, err
	}
	return body, nil
}

func (n *NonPersistentTopicsImpl) TriggerTopicCompaction(tenant, namespace, topic string) error {
	url := fmt.Sprintf(UrlNonPersistentTopicCompactionFormat, tenant, namespace, topic)
	resp, err := n.cli.Put(url, nil)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}
