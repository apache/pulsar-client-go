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

type Tenants interface {
	Create(TenantData) error
	Delete(string) error
	Update(TenantData) error
	List() ([]string, error)
	Get(string) (TenantData, error)
}

type tenants struct {
	client   *client
	basePath string
}

func (c *client) Tenants() Tenants {
	return &tenants{
		client:   c,
		basePath: "/tenants",
	}
}

func (c *tenants) Create(data TenantData) error {
	endpoint := c.client.endpoint(c.basePath, data.Name)
	return c.client.put(endpoint, &data)
}

func (c *tenants) Delete(name string) error {
	endpoint := c.client.endpoint(c.basePath, name)
	return c.client.delete(endpoint)
}

func (c *tenants) Update(data TenantData) error {
	endpoint := c.client.endpoint(c.basePath, data.Name)
	return c.client.post(endpoint, &data)
}

func (c *tenants) List() ([]string, error) {
	var tenantList []string
	endpoint := c.client.endpoint(c.basePath, "")
	err := c.client.get(endpoint, &tenantList)
	return tenantList, err
}

func (c *tenants) Get(name string) (TenantData, error) {
	var data TenantData
	endpoint := c.client.endpoint(c.basePath, name)
	err := c.client.get(endpoint, &data)
	return data, err
}
