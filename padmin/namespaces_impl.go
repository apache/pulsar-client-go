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
	"io"
	"strconv"
)

type Namespaces interface {
	Create(tenant, namespace string) error
	Delete(tenant, namespace string) error
	List(tenant string) ([]string, error)
	NamespaceRetention
	NamespaceBacklog
	NamespaceMessageTTL
	NamespaceCompaction
}

type NamespacesImpl struct {
	cli HttpClient
	options
}

func newNamespaces(cli HttpClient) Namespaces {
	return &NamespacesImpl{cli: cli}
}

func (n *NamespacesImpl) WithOptions(opts ...Option) Namespaces {
	for _, opt := range opts {
		opt(&n.options)
	}
	return n
}

// GetNamespaceBacklogQuota Get backlog quota map on a namespace.
// required params: tenant, namespace
func (n *NamespacesImpl) GetNamespaceBacklogQuota(tenant, namespace string) (*BacklogQuotaResp, error) {
	resp, err := n.cli.Get(fmt.Sprintf(UrlNamespaceGetBacklogQuotaMapFormat, tenant, namespace))
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

// SetNamespaceBacklogQuota  Set a backlog quota for all the topics on a namespace.
// required params: tenant, namespace
func (n *NamespacesImpl) SetNamespaceBacklogQuota(tenant, namespace string, cfg *BacklogQuota) error {
	url := fmt.Sprintf(UrlNamespaceOperateBacklogQuotaFormat, tenant, namespace)
	resp, err := n.cli.Post(url, cfg)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

// RemoveNamespaceBacklogQuota Remove a backlog quota policy from a namespace.
// required params: tenant, namespace
func (n *NamespacesImpl) RemoveNamespaceBacklogQuota(tenant, namespace string, opts ...Option) error {
	if len(opts) != 0 {
		n.WithOptions(opts...)
	}
	url := fmt.Sprintf(UrlNamespaceOperateBacklogQuotaFormat, tenant, namespace)
	if n.options.backlogQuotaType != "" {
		url = fmt.Sprintf("%s?backlogQuotaType=", n.options.backlogQuotaType)
	}
	resp, err := n.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

// ClearNamespaceAllTopicsBacklog Clear backlog for all topics on a namespace.
// required params: tenant, namespace
func (n *NamespacesImpl) ClearNamespaceAllTopicsBacklog(tenant, namespace string) error {
	url := fmt.Sprintf(UrlNamespaceClearAllTopicsBacklogFormat, tenant, namespace)
	resp, err := n.cli.Post(url, nil)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

// ClearNamespaceSubscriptionBacklog Clear backlog for a given subscription on all topics on a namespace.
// required params: tenant, namespace, subscriptionName
func (n *NamespacesImpl) ClearNamespaceSubscriptionBacklog(tenant, namespace, subscription string) error {
	url := fmt.Sprintf(UrlNamespaceClearSubscriptionBacklogFormat, tenant, namespace, subscription)
	resp, err := n.cli.Post(url, nil)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

// ClearNamespaceAllTopicsBacklogForBundle Clear backlog for all topics on a namespace bundle.
// required params: tenant, namespace, bundleName
func (n *NamespacesImpl) ClearNamespaceAllTopicsBacklogForBundle(tenant, namespace, bundle string) error {
	url := fmt.Sprintf(UrlNamespaceClearAllTopicsBacklogForBundleFormat, tenant, namespace, bundle)
	resp, err := n.cli.Post(url, nil)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

// ClearNamespaceSubscriptionBacklogForBundle Clear backlog for a given subscription on all topics on a namespace bundle.
// required params: tenant, namespace, subscriptionName, bundleName
func (n *NamespacesImpl) ClearNamespaceSubscriptionBacklogForBundle(tenant, namespace, subscription, bundle string) error {
	url := fmt.Sprintf(UrlNamespaceClearSubscriptionBacklogForBundleFormat, tenant, namespace, subscription, bundle)
	resp, err := n.cli.Post(url, nil)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NamespacesImpl) Create(tenant, namespace string) error {
	resp, err := n.cli.Put(fmt.Sprintf(UrlNamespacesFormat, tenant, namespace), nil)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NamespacesImpl) Delete(tenant, namespace string) error {
	resp, err := n.cli.Delete(fmt.Sprintf(UrlNamespacesFormat, tenant, namespace))
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NamespacesImpl) List(tenant string) ([]string, error) {
	resp, err := n.cli.Get(fmt.Sprintf(UrlNamespacesFormat, tenant, ""))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (n *NamespacesImpl) GetNamespaceRetention(tenant, namespace string) (*RetentionConfiguration, error) {
	url := fmt.Sprintf(UrlNamespaceRetentionFormat, tenant, namespace)
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

func (n *NamespacesImpl) SetNamespaceRetention(tenant, namespace string, cfg *RetentionConfiguration) error {
	if cfg == nil {
		return fmt.Errorf("config empty")
	}
	url := fmt.Sprintf(UrlNamespaceRetentionFormat, tenant, namespace)
	resp, err := n.cli.Post(url, cfg)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NamespacesImpl) RemoveNamespaceRetention(tenant, namespace string) error {
	url := fmt.Sprintf(UrlNamespaceRetentionFormat, tenant, namespace)
	resp, err := n.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NamespacesImpl) GetNamespaceMessageTTL(tenant, namespace string) (int64, error) {
	url := fmt.Sprintf(UrlNamespaceMessageTTLFormat, tenant, namespace)
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

func (n *NamespacesImpl) SetNamespaceMessageTTL(tenant, namespace string, seconds int64) error {
	url := fmt.Sprintf(UrlNamespaceMessageTTLFormat, tenant, namespace)
	resp, err := n.cli.Post(url, seconds)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NamespacesImpl) RemoveNamespaceMessageTTL(tenant, namespace string) error {
	url := fmt.Sprintf(UrlNamespaceMessageTTLFormat, tenant, namespace)
	resp, err := n.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NamespacesImpl) GetMaximumUnCompactedBytes(tenant, namespace string) (int64, error) {
	url := fmt.Sprintf(UrlNamespaceCompactionThresholdFormat, tenant, namespace)
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

func (n *NamespacesImpl) SetMaximumUnCompactedBytes(tenant, namespace string, threshold int64) error {
	url := fmt.Sprintf(UrlNamespaceCompactionThresholdFormat, tenant, namespace)
	resp, err := n.cli.Put(url, threshold)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}

func (n *NamespacesImpl) RemoveMaximumUnCompactedBytes(tenant, namespace string) error {
	url := fmt.Sprintf(UrlNamespaceCompactionThresholdFormat, tenant, namespace)
	resp, err := n.cli.Delete(url)
	if err != nil {
		return err
	}
	return HttpCheck(resp)
}
