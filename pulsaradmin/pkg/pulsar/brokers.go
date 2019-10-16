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
	"net/url"
	"strings"
)

type Brokers interface {
	// Get the list of active brokers in the cluster.
	GetActiveBrokers(cluster string) ([]string, error)

	// Get list of updatable configuration name
	GetDynamicConfigurationNames() ([]string, error)

	// Get the map of owned namespaces and their status from a single broker in the cluster
	GetOwnedNamespaces(cluster, brokerURL string) (map[string]NamespaceOwnershipStatus, error)

	// It updates dynamic configuration value in to Zk that triggers watch on
	// brokers and all brokers can update {@link ServiceConfiguration} value locally
	UpdateDynamicConfiguration(configName, configValue string) error

	// It deletes dynamic configuration value in to Zk. It will not impact current value
	// in broker but next time when broker restarts, it applies value from configuration file only.
	DeleteDynamicConfiguration(configName string) error

	// Get values of runtime configuration
	GetRuntimeConfigurations() (map[string]string, error)

	// Get the internal configuration data
	GetInternalConfigurationData() (*InternalConfigurationData, error)

	// Get values of all overridden dynamic-configs
	GetAllDynamicConfigurations() (map[string]string, error)

	// Run a health check on the broker
	HealthCheck() error
}

type broker struct {
	client   *client
	basePath string
}

func (c *client) Brokers() Brokers {
	return &broker{
		client:   c,
		basePath: "/brokers",
	}
}

func (b *broker) GetActiveBrokers(cluster string) ([]string, error) {
	endpoint := b.client.endpoint(b.basePath, cluster)
	var res []string
	err := b.client.get(endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *broker) GetDynamicConfigurationNames() ([]string, error) {
	endpoint := b.client.endpoint(b.basePath, "/configuration/")
	var res []string
	err := b.client.get(endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *broker) GetOwnedNamespaces(cluster, brokerURL string) (map[string]NamespaceOwnershipStatus, error) {
	endpoint := b.client.endpoint(b.basePath, cluster, brokerURL, "ownedNamespaces")
	var res map[string]NamespaceOwnershipStatus
	err := b.client.get(endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *broker) UpdateDynamicConfiguration(configName, configValue string) error {
	value := url.QueryEscape(configValue)
	endpoint := b.client.endpoint(b.basePath, "/configuration/", configName, value)
	return b.client.post(endpoint, nil)
}

func (b *broker) DeleteDynamicConfiguration(configName string) error {
	endpoint := b.client.endpoint(b.basePath, "/configuration/", configName)
	return b.client.delete(endpoint)
}

func (b *broker) GetRuntimeConfigurations() (map[string]string, error) {
	endpoint := b.client.endpoint(b.basePath, "/configuration/", "runtime")
	var res map[string]string
	err := b.client.get(endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *broker) GetInternalConfigurationData() (*InternalConfigurationData, error) {
	endpoint := b.client.endpoint(b.basePath, "/internal-configuration")
	var res InternalConfigurationData
	err := b.client.get(endpoint, &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (b *broker) GetAllDynamicConfigurations() (map[string]string, error) {
	endpoint := b.client.endpoint(b.basePath, "/configuration/", "values")
	var res map[string]string
	err := b.client.get(endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *broker) HealthCheck() error {
	endpoint := b.client.endpoint(b.basePath, "/health")

	buf, err := b.client.getWithQueryParams(endpoint, nil, nil, false)
	if err != nil {
		return err
	}

	if !strings.EqualFold(string(buf), "ok") {
		return fmt.Errorf("health check returned unexpected result: %s", string(buf))
	}
	return nil
}
