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

// Tenants is admin interface for tenants management
type Tenants interface {
	// Create creates a new tenant
	Create(utils.TenantData) error

	// CreateWithContext creates a new tenant
	CreateWithContext(context.Context, utils.TenantData) error

	// Delete deletes an existing tenant
	Delete(string) error

	// DeleteWithContext deletes an existing tenant
	DeleteWithContext(context.Context, string) error

	// Update updates the admins for a tenant
	Update(utils.TenantData) error

	// UpdateWithContext updates the admins for a tenant
	UpdateWithContext(context.Context, utils.TenantData) error

	// List returns the list of tenants
	List() ([]string, error)

	// ListWithContext returns the list of tenants
	ListWithContext(context.Context) ([]string, error)

	// Get returns the config of the tenant.
	Get(string) (utils.TenantData, error)

	// GetWithContext returns the config of the tenant.
	GetWithContext(context.Context, string) (utils.TenantData, error)
}

type tenants struct {
	pulsar   *pulsarClient
	basePath string
}

// Tenants is used to access the tenants endpoints
func (c *pulsarClient) Tenants() Tenants {
	return &tenants{
		pulsar:   c,
		basePath: "/tenants",
	}
}

func (c *tenants) Create(data utils.TenantData) error {
	return c.CreateWithContext(context.Background(), data)
}

func (c *tenants) CreateWithContext(ctx context.Context, data utils.TenantData) error {
	endpoint := c.pulsar.endpoint(c.basePath, data.Name)
	return c.pulsar.Client.Put(ctx, endpoint, &data)
}

func (c *tenants) Delete(name string) error {
	return c.DeleteWithContext(context.Background(), name)
}

func (c *tenants) DeleteWithContext(ctx context.Context, name string) error {
	endpoint := c.pulsar.endpoint(c.basePath, name)
	return c.pulsar.Client.Delete(ctx, endpoint)
}

func (c *tenants) Update(data utils.TenantData) error {
	return c.UpdateWithContext(context.Background(), data)
}

func (c *tenants) UpdateWithContext(ctx context.Context, data utils.TenantData) error {
	endpoint := c.pulsar.endpoint(c.basePath, data.Name)
	return c.pulsar.Client.Post(ctx, endpoint, &data)
}

func (c *tenants) List() ([]string, error) {
	return c.ListWithContext(context.Background())
}

func (c *tenants) ListWithContext(ctx context.Context) ([]string, error) {
	var tenantList []string
	endpoint := c.pulsar.endpoint(c.basePath, "")
	err := c.pulsar.Client.Get(ctx, endpoint, &tenantList)
	return tenantList, err
}

func (c *tenants) Get(name string) (utils.TenantData, error) {
	return c.GetWithContext(context.Background(), name)
}

func (c *tenants) GetWithContext(ctx context.Context, name string) (utils.TenantData, error) {
	var data utils.TenantData
	endpoint := c.pulsar.endpoint(c.basePath, name)
	err := c.pulsar.Client.Get(ctx, endpoint, &data)
	return data, err
}
