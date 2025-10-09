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

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

type ResourceQuotas interface {
	// GetDefaultResourceQuota returns default resource quota for new resource bundles.
	GetDefaultResourceQuota() (*utils.ResourceQuota, error)

	// GetDefaultResourceQuotaWithContext returns default resource quota for new resource bundles.
	GetDefaultResourceQuotaWithContext(ctx context.Context) (*utils.ResourceQuota, error)

	// SetDefaultResourceQuota sets default resource quota for new namespace bundles.
	SetDefaultResourceQuota(quota utils.ResourceQuota) error

	// SetDefaultResourceQuotaWithContext sets default resource quota for new namespace bundles.
	SetDefaultResourceQuotaWithContext(ctx context.Context, quota utils.ResourceQuota) error

	// GetNamespaceBundleResourceQuota returns resource quota of a namespace bundle.
	GetNamespaceBundleResourceQuota(namespace, bundle string) (*utils.ResourceQuota, error)

	// GetNamespaceBundleResourceQuotaWithContext returns resource quota of a namespace bundle.
	GetNamespaceBundleResourceQuotaWithContext(ctx context.Context, namespace, bundle string) (*utils.ResourceQuota, error)

	// SetNamespaceBundleResourceQuota sets resource quota for a namespace bundle.
	SetNamespaceBundleResourceQuota(namespace, bundle string, quota utils.ResourceQuota) error

	// SetNamespaceBundleResourceQuotaWithContext sets resource quota for a namespace bundle.
	SetNamespaceBundleResourceQuotaWithContext(
		ctx context.Context,
		namespace,
		bundle string,
		quota utils.ResourceQuota,
	) error

	// ResetNamespaceBundleResourceQuota resets resource quota for a namespace bundle to default value.
	ResetNamespaceBundleResourceQuota(namespace, bundle string) error

	// ResetNamespaceBundleResourceQuotaWithContext resets resource quota for a namespace bundle to default value.
	ResetNamespaceBundleResourceQuotaWithContext(ctx context.Context, namespace, bundle string) error
}

type resource struct {
	pulsar   *pulsarClient
	basePath string
}

func (c *pulsarClient) ResourceQuotas() ResourceQuotas {
	return &resource{
		pulsar:   c,
		basePath: "/resource-quotas",
	}
}

func (r *resource) GetDefaultResourceQuota() (*utils.ResourceQuota, error) {
	return r.GetDefaultResourceQuotaWithContext(context.Background())
}

func (r *resource) GetDefaultResourceQuotaWithContext(ctx context.Context) (*utils.ResourceQuota, error) {
	endpoint := r.pulsar.endpoint(r.basePath)
	var quota utils.ResourceQuota
	err := r.pulsar.Client.GetWithContext(ctx, endpoint, &quota)
	if err != nil {
		return nil, err
	}
	return &quota, nil
}

func (r *resource) SetDefaultResourceQuota(quota utils.ResourceQuota) error {
	return r.SetDefaultResourceQuotaWithContext(context.Background(), quota)
}

func (r *resource) SetDefaultResourceQuotaWithContext(ctx context.Context, quota utils.ResourceQuota) error {
	endpoint := r.pulsar.endpoint(r.basePath)
	return r.pulsar.Client.PostWithContext(ctx, endpoint, &quota)
}

func (r *resource) GetNamespaceBundleResourceQuota(namespace, bundle string) (*utils.ResourceQuota, error) {
	return r.GetNamespaceBundleResourceQuotaWithContext(context.Background(), namespace, bundle)
}

func (r *resource) GetNamespaceBundleResourceQuotaWithContext(
	ctx context.Context,
	namespace,
	bundle string,
) (*utils.ResourceQuota, error) {
	endpoint := r.pulsar.endpoint(r.basePath, namespace, bundle)
	var quota utils.ResourceQuota
	err := r.pulsar.Client.GetWithContext(ctx, endpoint, &quota)
	if err != nil {
		return nil, err
	}
	return &quota, nil
}

func (r *resource) SetNamespaceBundleResourceQuota(namespace, bundle string, quota utils.ResourceQuota) error {
	return r.SetNamespaceBundleResourceQuotaWithContext(context.Background(), namespace, bundle, quota)
}

func (r *resource) SetNamespaceBundleResourceQuotaWithContext(
	ctx context.Context,
	namespace,
	bundle string,
	quota utils.ResourceQuota,
) error {
	endpoint := r.pulsar.endpoint(r.basePath, namespace, bundle)
	return r.pulsar.Client.PostWithContext(ctx, endpoint, &quota)
}

func (r *resource) ResetNamespaceBundleResourceQuota(namespace, bundle string) error {
	return r.ResetNamespaceBundleResourceQuotaWithContext(context.Background(), namespace, bundle)
}

func (r *resource) ResetNamespaceBundleResourceQuotaWithContext(ctx context.Context, namespace, bundle string) error {
	endpoint := r.pulsar.endpoint(r.basePath, namespace, bundle)
	return r.pulsar.Client.DeleteWithContext(ctx, endpoint)
}
