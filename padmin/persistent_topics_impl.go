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

type PersistentTopics interface {
	WithOptions(opts ...Option) PersistentTopics
	Topics
	TopicBacklog
	TopicMessageTTL
	TopicCompaction
}

type PersistentTopicsImpl struct {
	cli HttpClient
	options
}

func (p *PersistentTopicsImpl) WithOptions(opts ...Option) PersistentTopics {
	for _, opt := range opts {
		opt(&p.options)
	}
	return p
}

// GetTopicBacklogQuota Get backlog quota map on a topic.
func (p *PersistentTopicsImpl) GetTopicBacklogQuota(tenant, namespace, topic string) (*BacklogQuotaResp, error) {
	resp, err := p.cli.Get(fmt.Sprintf(UrlPersistentTopicGetBacklogQuotaMapFormat, tenant, namespace, topic))
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
func (p *PersistentTopicsImpl) SetTopicBacklogQuota(tenant, namespace, topic string, cfg *BacklogQuota) error {
	url := fmt.Sprintf(UrlPersistentTopicOperateBacklogQuotaFormat, tenant, namespace, topic)
	resp, err := p.cli.Post(url, cfg)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

// RemoveTopicBacklogQuota Remove a backlog quota policy from a topic.
func (p *PersistentTopicsImpl) RemoveTopicBacklogQuota(tenant, namespace, topic string, opts ...Option) error {
	url := fmt.Sprintf(UrlPersistentTopicOperateBacklogQuotaFormat, tenant, namespace, topic)
	if p.options.backlogQuotaType != "" {
		url = fmt.Sprintf("%s?backlogQuotaType=", p.options.backlogQuotaType)
	}
	resp, err := p.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

// EstimatedOfflineTopicBacklog Get estimated backlog for offline topic.
func (p *PersistentTopicsImpl) EstimatedOfflineTopicBacklog(tenant, namespace, topic string) (*OfflineTopicStats, error) {
	resp, err := p.cli.Get(fmt.Sprintf(UrlPersistentTopicEstimatedOfflineBacklogFormat, tenant, namespace, topic))
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
func (p *PersistentTopicsImpl) CalculateBacklogSizeByMessageID(tenant, namespace, topic string) error {
	resp, err := p.cli.Get(fmt.Sprintf(UrlPersistentTopicCalculateBacklogSizeByMessageIDFormat, tenant, namespace, topic))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return HttpCheck(resp)
}

func newPersistentTopics(cli HttpClient) *PersistentTopicsImpl {
	return &PersistentTopicsImpl{cli: cli}
}

func (p *PersistentTopicsImpl) CreateNonPartitioned(tenant, namespace, topic string) error {
	path := fmt.Sprintf(UrlPersistentTopicFormat, tenant, namespace, topic)
	resp, err := p.cli.Put(path, nil)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (p *PersistentTopicsImpl) DeleteNonPartitioned(tenant, namespace, topic string) error {
	path := fmt.Sprintf(UrlPersistentTopicFormat, tenant, namespace, topic)
	resp, err := p.cli.Delete(path)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (p *PersistentTopicsImpl) ListNonPartitioned(tenant, namespace string) ([]string, error) {
	path := fmt.Sprintf(UrlPersistentNamespaceFormat, tenant, namespace)
	resp, err := p.cli.Get(path)
	if err != nil {
		return nil, err
	}
	data, err := HttpCheckReadBytes(resp)
	if err != nil {
		return nil, err
	}
	var topics []string
	if err := json.Unmarshal(data, &topics); err != nil {
		return nil, err
	}
	return topics, nil
}

func (p *PersistentTopicsImpl) CreatePartitioned(tenant, namespace, topic string, numPartitions int) error {
	path := fmt.Sprintf(UrlPersistentPartitionedTopicFormat, tenant, namespace, topic)
	resp, err := p.cli.Put(path, numPartitions)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (p *PersistentTopicsImpl) DeletePartitioned(tenant, namespace, topic string) error {
	path := fmt.Sprintf(UrlPersistentPartitionedTopicFormat, tenant, namespace, topic)
	resp, err := p.cli.Delete(path)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

// ListPartitioned Get the list of partitioned topics under a namespace.
func (p *PersistentTopicsImpl) ListPartitioned(tenant, namespace string) ([]string, error) {
	path := fmt.Sprintf(UrlPersistentPartitionedNamespaceFormat, tenant, namespace)
	resp, err := p.cli.Get(path)
	if err != nil {
		return nil, err
	}
	data, err := HttpCheckReadBytes(resp)
	if err != nil {
		return nil, err
	}
	var topics []string
	if err := json.Unmarshal(data, &topics); err != nil {
		return nil, err
	}
	return topics, nil
}

// ListNamespaceTopics Get the list of topics under a namespace.
func (p *PersistentTopicsImpl) ListNamespaceTopics(tenant, namespace string) ([]string, error) {
	url := fmt.Sprintf(UrlPersistentNamespaceFormat, tenant, namespace)
	resp, err := p.cli.Get(url)
	if err != nil {
		return nil, err
	}
	var topics []string
	if err := EasyReader(resp, &topics); err != nil {
		return nil, err
	}
	return topics, nil
}

func (p *PersistentTopicsImpl) GetPartitionedMetadata(tenant, namespace, topic string) (*PartitionedMetadata, error) {
	url := fmt.Sprintf(UrlPersistentPartitionedTopicFormat, tenant, namespace, topic)
	resp, err := p.cli.Get(url)
	if err != nil {
		return nil, err
	}
	var metadata = new(PartitionedMetadata)
	if err := EasyReader(resp, metadata); err != nil {
		return nil, err
	}
	return metadata, nil
}

func (p *PersistentTopicsImpl) GetTopicRetention(tenant, namespace, topic string) (*RetentionConfiguration, error) {
	url := fmt.Sprintf(UrlPersistentPartitionedRetentionFormat, tenant, namespace, topic)
	resp, err := p.cli.Get(url)
	if err != nil {
		return nil, err
	}
	var retention = new(RetentionConfiguration)
	if err := EasyReader(resp, retention); err != nil {
		return nil, err
	}
	return retention, nil
}

func (p *PersistentTopicsImpl) SetTopicRetention(tenant, namespace, topic string, cfg *RetentionConfiguration) error {
	if cfg == nil {
		return fmt.Errorf("config empty")
	}
	url := fmt.Sprintf(UrlPersistentPartitionedRetentionFormat, tenant, namespace, topic)
	resp, err := p.cli.Post(url, cfg)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (p *PersistentTopicsImpl) RemoveTopicRetention(tenant, namespace, topic string) error {
	url := fmt.Sprintf(UrlPersistentPartitionedRetentionFormat, tenant, namespace, topic)
	resp, err := p.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (p *PersistentTopicsImpl) GetTopicMessageTTL(tenant, namespace, topic string) (int64, error) {
	url := fmt.Sprintf(UrlPersistentTopicMessageTTLFormat, tenant, namespace, topic)
	resp, err := p.cli.Get(url)
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

func (p *PersistentTopicsImpl) SetTopicMessageTTL(tenant, namespace, topic string, seconds int64) error {
	url := fmt.Sprintf(UrlPersistentTopicMessageTTLFormat, tenant, namespace, topic)
	resp, err := p.cli.Post(url, seconds)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (p *PersistentTopicsImpl) RemoveTopicMessageTTL(tenant, namespace, topic string) error {
	url := fmt.Sprintf(UrlPersistentTopicMessageTTLFormat, tenant, namespace, topic)
	resp, err := p.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (p *PersistentTopicsImpl) GetTopicCompactionThreshold(tenant, namespace, topic string) (int64, error) {
	url := fmt.Sprintf(UrlPersistentTopicCompactionThresholdFormat, tenant, namespace, topic)
	resp, err := p.cli.Get(url)
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

func (p *PersistentTopicsImpl) SetTopicCompactionThreshold(tenant, namespace, topic string, threshold int64) error {
	url := fmt.Sprintf(UrlPersistentTopicCompactionThresholdFormat, tenant, namespace, topic)
	resp, err := p.cli.Post(url, threshold)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (p *PersistentTopicsImpl) RemoveTopicCompactionThreshold(tenant, namespace, topic string) error {
	url := fmt.Sprintf(UrlPersistentTopicCompactionThresholdFormat, tenant, namespace, topic)
	resp, err := p.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (p *PersistentTopicsImpl) GetTopicCompactionStatus(tenant, namespace, topic string) (*LongRunningProcessStatus, error) {
	url := fmt.Sprintf(UrlPersistentTopicCompactionFormat, tenant, namespace, topic)
	resp, err := p.cli.Get(url)
	if err != nil {
		return nil, err
	}
	var body = new(LongRunningProcessStatus)
	if err := EasyReader(resp, body); err != nil {
		return nil, err
	}
	return body, nil
}

func (p *PersistentTopicsImpl) TriggerTopicCompaction(tenant, namespace, topic string) error {
	url := fmt.Sprintf(UrlPersistentTopicCompactionFormat, tenant, namespace, topic)
	resp, err := p.cli.Put(url, nil)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}
