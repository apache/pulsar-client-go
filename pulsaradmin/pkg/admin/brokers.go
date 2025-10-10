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
	"fmt"
	"strings"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

// Brokers is admin interface for brokers management
type Brokers interface {

	// GetListActiveBrokers returns the list of active brokers in the local cluster.
	GetListActiveBrokers() ([]string, error)

	// GetListActiveBrokersWithContext returns the list of active brokers in the local cluster.
	GetListActiveBrokersWithContext(context.Context) ([]string, error)

	// GetActiveBrokers returns the list of active brokers in the cluster.
	GetActiveBrokers(cluster string) ([]string, error)

	// GetActiveBrokersWithContext returns the list of active brokers in the cluster.
	GetActiveBrokersWithContext(ctx context.Context, cluster string) ([]string, error)

	// GetDynamicConfigurationNames returns list of updatable configuration name
	GetDynamicConfigurationNames() ([]string, error)

	// GetDynamicConfigurationNamesWithContext returns list of updatable configuration name
	GetDynamicConfigurationNamesWithContext(context.Context) ([]string, error)

	// GetOwnedNamespaces returns the map of owned namespaces and their status from a single broker in the cluster
	GetOwnedNamespaces(cluster, brokerURL string) (map[string]utils.NamespaceOwnershipStatus, error)

	// GetOwnedNamespaces returns the map of owned namespaces and their status from a single broker in the cluster
	GetOwnedNamespacesWithContext(
		ctx context.Context,
		cluster,
		brokerURL string,
	) (map[string]utils.NamespaceOwnershipStatus, error)

	// UpdateDynamicConfiguration updates dynamic configuration value in to Zk that triggers watch on
	// brokers and all brokers can update {@link ServiceConfiguration} value locally
	UpdateDynamicConfiguration(configName, configValue string) error

	// UpdateDynamicConfigurationWithContext updates dynamic configuration value in to Zk that triggers watch on
	// brokers and all brokers can update {@link ServiceConfiguration} value locally
	UpdateDynamicConfigurationWithContext(ctx context.Context, configName, configValue string) error

	// DeleteDynamicConfiguration deletes dynamic configuration value in to Zk. It will not impact current value
	// in broker but next time when broker restarts, it applies value from configuration file only.
	DeleteDynamicConfiguration(configName string) error

	// DeleteDynamicConfigurationWithContext deletes dynamic configuration value in to Zk. It will not impact current value
	// in broker but next time when broker restarts, it applies value from configuration file only.
	DeleteDynamicConfigurationWithContext(ctx context.Context, configName string) error

	// GetRuntimeConfigurations returns values of runtime configuration
	GetRuntimeConfigurations() (map[string]string, error)

	// GetRuntimeConfigurationsWithContext returns values of runtime configuration
	GetRuntimeConfigurationsWithContext(context.Context) (map[string]string, error)

	// GetInternalConfigurationData returns the internal configuration data
	GetInternalConfigurationData() (*utils.InternalConfigurationData, error)

	// GetInternalConfigurationDataWithContext returns the internal configuration data
	GetInternalConfigurationDataWithContext(context.Context) (*utils.InternalConfigurationData, error)

	// GetAllDynamicConfigurations returns values of all overridden dynamic-configs
	GetAllDynamicConfigurations() (map[string]string, error)

	// GetAllDynamicConfigurationsWithContext returns values of all overridden dynamic-configs
	GetAllDynamicConfigurationsWithContext(context.Context) (map[string]string, error)

	// Deprecated: Use HealthCheckWithTopicVersion instead
	HealthCheck() error

	// Deprecated: Use HealthCheckWithTopicVersionWithContext instead
	HealthCheckWithContext(context.Context) error

	// HealthCheckWithTopicVersion runs a health check on the broker
	HealthCheckWithTopicVersion(utils.TopicVersion) error

	// HealthCheckWithTopicVersionWithContext runs a health check on the broker
	HealthCheckWithTopicVersionWithContext(context.Context, utils.TopicVersion) error

	// GetLeaderBroker get the information of the leader broker.
	GetLeaderBroker() (utils.BrokerInfo, error)

	// GetLeaderBrokerWithContext returns the information of the leader broker.
	GetLeaderBrokerWithContext(context.Context) (utils.BrokerInfo, error)
}

type broker struct {
	pulsar   *pulsarClient
	basePath string
}

// Brokers is used to access the brokers endpoints
func (c *pulsarClient) Brokers() Brokers {
	return &broker{
		pulsar:   c,
		basePath: "/brokers",
	}
}

func (b *broker) GetActiveBrokers(cluster string) ([]string, error) {
	return b.GetActiveBrokersWithContext(context.Background(), cluster)
}

func (b *broker) GetActiveBrokersWithContext(ctx context.Context, cluster string) ([]string, error) {
	endpoint := b.pulsar.endpoint(b.basePath, cluster)
	var res []string
	err := b.pulsar.Client.GetWithContext(ctx, endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *broker) GetListActiveBrokers() ([]string, error) {
	return b.GetListActiveBrokersWithContext(context.Background())
}

func (b *broker) GetListActiveBrokersWithContext(ctx context.Context) ([]string, error) {
	endpoint := b.pulsar.endpoint(b.basePath)
	var res []string
	err := b.pulsar.Client.GetWithContext(ctx, endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *broker) GetDynamicConfigurationNames() ([]string, error) {
	return b.GetDynamicConfigurationNamesWithContext(context.Background())
}

func (b *broker) GetDynamicConfigurationNamesWithContext(ctx context.Context) ([]string, error) {
	endpoint := b.pulsar.endpoint(b.basePath, "/configuration/")
	var res []string
	err := b.pulsar.Client.GetWithContext(ctx, endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *broker) GetOwnedNamespaces(cluster, brokerURL string) (map[string]utils.NamespaceOwnershipStatus, error) {
	return b.GetOwnedNamespacesWithContext(context.Background(), cluster, brokerURL)
}

func (b *broker) GetOwnedNamespacesWithContext(
	ctx context.Context,
	cluster,
	brokerURL string,
) (map[string]utils.NamespaceOwnershipStatus, error) {
	endpoint := b.pulsar.endpoint(b.basePath, cluster, brokerURL, "ownedNamespaces")
	var res map[string]utils.NamespaceOwnershipStatus
	err := b.pulsar.Client.GetWithContext(ctx, endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *broker) UpdateDynamicConfiguration(configName, configValue string) error {
	return b.UpdateDynamicConfigurationWithContext(context.Background(), configName, configValue)
}

func (b *broker) UpdateDynamicConfigurationWithContext(ctx context.Context, configName, configValue string) error {
	value := fmt.Sprintf("/configuration/%s/%s", configName, configValue)
	endpoint := b.pulsar.endpointWithFullPath(b.basePath, value)
	return b.pulsar.Client.PostWithContext(ctx, endpoint, nil)
}

func (b *broker) DeleteDynamicConfiguration(configName string) error {
	return b.DeleteDynamicConfigurationWithContext(context.Background(), configName)
}

func (b *broker) DeleteDynamicConfigurationWithContext(ctx context.Context, configName string) error {
	endpoint := b.pulsar.endpoint(b.basePath, "/configuration/", configName)
	return b.pulsar.Client.DeleteWithContext(ctx, endpoint)
}

func (b *broker) GetRuntimeConfigurations() (map[string]string, error) {
	return b.GetRuntimeConfigurationsWithContext(context.Background())
}

func (b *broker) GetRuntimeConfigurationsWithContext(ctx context.Context) (map[string]string, error) {
	endpoint := b.pulsar.endpoint(b.basePath, "/configuration/", "runtime")
	var res map[string]string
	err := b.pulsar.Client.GetWithContext(ctx, endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *broker) GetInternalConfigurationData() (*utils.InternalConfigurationData, error) {
	return b.GetInternalConfigurationDataWithContext(context.Background())
}

func (b *broker) GetInternalConfigurationDataWithContext(
	ctx context.Context,
) (*utils.InternalConfigurationData, error) {
	endpoint := b.pulsar.endpoint(b.basePath, "/internal-configuration")
	var res utils.InternalConfigurationData
	err := b.pulsar.Client.GetWithContext(ctx, endpoint, &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (b *broker) GetAllDynamicConfigurations() (map[string]string, error) {
	return b.GetAllDynamicConfigurationsWithContext(context.Background())
}

func (b *broker) GetAllDynamicConfigurationsWithContext(ctx context.Context) (map[string]string, error) {
	endpoint := b.pulsar.endpoint(b.basePath, "/configuration/", "values")
	var res map[string]string
	err := b.pulsar.Client.GetWithContext(ctx, endpoint, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (b *broker) HealthCheck() error {
	return b.HealthCheckWithContext(context.Background())
}

func (b *broker) HealthCheckWithContext(ctx context.Context) error {
	return b.HealthCheckWithTopicVersionWithContext(ctx, utils.TopicVersionV1)
}

func (b *broker) HealthCheckWithTopicVersion(topicVersion utils.TopicVersion) error {
	return b.HealthCheckWithTopicVersionWithContext(context.Background(), topicVersion)
}

func (b *broker) HealthCheckWithTopicVersionWithContext(ctx context.Context, topicVersion utils.TopicVersion) error {
	endpoint := b.pulsar.endpoint(b.basePath, "/health")

	buf, err := b.pulsar.Client.GetWithQueryParamsWithContext(ctx, endpoint, nil, map[string]string{
		"topicVersion": topicVersion.String(),
	}, false)
	if err != nil {
		return err
	}

	if !strings.EqualFold(string(buf), "ok") {
		return fmt.Errorf("health check returned unexpected result: %s", string(buf))
	}
	return nil
}

func (b *broker) GetLeaderBroker() (utils.BrokerInfo, error) {
	return b.GetLeaderBrokerWithContext(context.Background())
}

func (b *broker) GetLeaderBrokerWithContext(ctx context.Context) (utils.BrokerInfo, error) {
	endpoint := b.pulsar.endpoint(b.basePath, "/leaderBroker")
	var brokerInfo utils.BrokerInfo
	err := b.pulsar.Client.GetWithContext(ctx, endpoint, &brokerInfo)
	if err != nil {
		return brokerInfo, err
	}
	return brokerInfo, nil
}
