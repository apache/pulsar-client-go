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
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/utils"
)

type ResourceQuotas interface {
	// Get default resource quota for new resource bundles.
	GetDefaultResourceQuota() (*utils.ResourceQuota, error)

	// Set default resource quota for new namespace bundles.
	SetDefaultResourceQuota(quota utils.ResourceQuota) error

	// Get resource quota of a namespace bundle.
	GetNamespaceBundleResourceQuota(namespace, bundle string) (*utils.ResourceQuota, error)

	// Set resource quota for a namespace bundle.
	SetNamespaceBundleResourceQuota(namespace, bundle string, quota utils.ResourceQuota) error

	// Reset resource quota for a namespace bundle to default value.
	ResetNamespaceBundleResourceQuota(namespace, bundle string) error
}

type resource struct {
	client   *client
	basePath string
}

func (c *client) ResourceQuotas() ResourceQuotas {
	return &resource{
		client:   c,
		basePath: "/resource-quotas",
	}
}

func (r *resource) GetDefaultResourceQuota() (*utils.ResourceQuota, error) {
	endpoint := r.client.endpoint(r.basePath)
	var quota utils.ResourceQuota
	err := r.client.get(endpoint, &quota)
	if err != nil {
		return nil, err
	}
	return &quota, nil
}

func (r *resource) SetDefaultResourceQuota(quota utils.ResourceQuota) error {
	endpoint := r.client.endpoint(r.basePath)
	return r.client.post(endpoint, &quota)
}

func (r *resource) GetNamespaceBundleResourceQuota(namespace, bundle string) (*utils.ResourceQuota, error) {
	endpoint := r.client.endpoint(r.basePath, namespace, bundle)
	var quota utils.ResourceQuota
	err := r.client.get(endpoint, &quota)
	if err != nil {
		return nil, err
	}
	return &quota, nil
}

func (r *resource) SetNamespaceBundleResourceQuota(namespace, bundle string, quota utils.ResourceQuota) error {
	endpoint := r.client.endpoint(r.basePath, namespace, bundle)
	return r.client.post(endpoint, &quota)
}

func (r *resource) ResetNamespaceBundleResourceQuota(namespace, bundle string) error {
	endpoint := r.client.endpoint(r.basePath, namespace, bundle)
	return r.client.delete(endpoint)
}
