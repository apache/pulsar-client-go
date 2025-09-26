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

// Clusters is admin interface for clusters management
type Clusters interface {
	// List returns the list of clusters
	List() ([]string, error)

	// ListWithContext returns the list of clusters
	ListWithContext(context.Context) ([]string, error)

	// Get the configuration data for the specified cluster
	Get(string) (utils.ClusterData, error)

	// GetWithContext returns the configuration data for the specified cluster
	GetWithContext(context.Context, string) (utils.ClusterData, error)

	// Create a new cluster
	Create(utils.ClusterData) error

	// CreateWithContext creates a new cluster
	CreateWithContext(context.Context, utils.ClusterData) error

	// Delete an existing cluster
	Delete(string) error

	// DeleteWithContext deletes an existing cluster
	DeleteWithContext(context.Context, string) error

	// Update the configuration for a cluster
	Update(utils.ClusterData) error

	// UpdateWithContext updates the configuration for a cluster
	UpdateWithContext(context.Context, utils.ClusterData) error

	// UpdatePeerClusters updates peer cluster names.
	UpdatePeerClusters(string, []string) error

	// UpdatePeerClustersWithContext updates peer cluster names.
	UpdatePeerClustersWithContext(context.Context, string, []string) error

	// GetPeerClusters returns peer-cluster names
	GetPeerClusters(string) ([]string, error)

	// GetPeerClusters returns peer-cluster names
	GetPeerClustersWithContext(context.Context, string) ([]string, error)

	// CreateFailureDomain creates a domain into cluster
	CreateFailureDomain(utils.FailureDomainData) error

	// CreateFailureDomain creates a domain into cluster
	CreateFailureDomainWithContext(context.Context, utils.FailureDomainData) error

	// GetFailureDomain returns the domain registered into a cluster
	GetFailureDomain(clusterName, domainName string) (utils.FailureDomainData, error)

	// GetFailureDomain returns the domain registered into a cluster
	GetFailureDomainWithContext(ctx context.Context, clusterName, domainName string) (utils.FailureDomainData, error)

	// ListFailureDomains returns all registered domains in cluster
	ListFailureDomains(string) (utils.FailureDomainMap, error)

	// ListFailureDomains returns all registered domains in cluster
	ListFailureDomainsWithContext(context.Context, string) (utils.FailureDomainMap, error)

	// DeleteFailureDomain deletes a domain in cluster
	DeleteFailureDomain(utils.FailureDomainData) error

	// DeleteFailureDomain deletes a domain in cluster
	DeleteFailureDomainWithContext(context.Context, utils.FailureDomainData) error

	// UpdateFailureDomain updates a domain into cluster
	UpdateFailureDomain(utils.FailureDomainData) error

	// UpdateFailureDomain updates a domain into cluster
	UpdateFailureDomainWithContext(context.Context, utils.FailureDomainData) error
}

type clusters struct {
	pulsar   *pulsarClient
	basePath string
}

// Clusters is used to access the cluster endpoints.
func (c *pulsarClient) Clusters() Clusters {
	return &clusters{
		pulsar:   c,
		basePath: "/clusters",
	}
}

func (c *clusters) List() ([]string, error) {
	return c.ListWithContext(context.Background())
}

func (c *clusters) ListWithContext(ctx context.Context) ([]string, error) {
	var clusters []string
	err := c.pulsar.Client.Get(ctx, c.pulsar.endpoint(c.basePath), &clusters)
	return clusters, err
}

func (c *clusters) Get(name string) (utils.ClusterData, error) {
	return c.GetWithContext(context.Background(), name)
}

func (c *clusters) GetWithContext(ctx context.Context, name string) (utils.ClusterData, error) {
	cdata := utils.ClusterData{}
	endpoint := c.pulsar.endpoint(c.basePath, name)
	err := c.pulsar.Client.Get(ctx, endpoint, &cdata)
	return cdata, err
}

func (c *clusters) Create(cdata utils.ClusterData) error {
	return c.CreateWithContext(context.Background(), cdata)
}

func (c *clusters) CreateWithContext(ctx context.Context, cdata utils.ClusterData) error {
	endpoint := c.pulsar.endpoint(c.basePath, cdata.Name)
	return c.pulsar.Client.Put(ctx, endpoint, &cdata)
}

func (c *clusters) Delete(name string) error {
	return c.DeleteWithContext(context.Background(), name)
}

func (c *clusters) DeleteWithContext(ctx context.Context, name string) error {
	endpoint := c.pulsar.endpoint(c.basePath, name)
	return c.pulsar.Client.Delete(ctx, endpoint)
}

func (c *clusters) Update(cdata utils.ClusterData) error {
	return c.UpdateWithContext(context.Background(), cdata)
}

func (c *clusters) UpdateWithContext(ctx context.Context, cdata utils.ClusterData) error {
	endpoint := c.pulsar.endpoint(c.basePath, cdata.Name)
	return c.pulsar.Client.Post(ctx, endpoint, &cdata)
}

func (c *clusters) GetPeerClusters(name string) ([]string, error) {
	return c.GetPeerClustersWithContext(context.Background(), name)
}

func (c *clusters) GetPeerClustersWithContext(ctx context.Context, name string) ([]string, error) {
	var peerClusters []string
	endpoint := c.pulsar.endpoint(c.basePath, name, "peers")
	err := c.pulsar.Client.Get(ctx, endpoint, &peerClusters)
	return peerClusters, err
}

func (c *clusters) UpdatePeerClusters(cluster string, peerClusters []string) error {
	return c.UpdatePeerClustersWithContext(context.Background(), cluster, peerClusters)
}

func (c *clusters) UpdatePeerClustersWithContext(ctx context.Context, cluster string, peerClusters []string) error {
	endpoint := c.pulsar.endpoint(c.basePath, cluster, "peers")
	return c.pulsar.Client.Post(ctx, endpoint, peerClusters)
}

func (c *clusters) CreateFailureDomain(data utils.FailureDomainData) error {
	return c.CreateFailureDomainWithContext(context.Background(), data)
}

func (c *clusters) CreateFailureDomainWithContext(ctx context.Context, data utils.FailureDomainData) error {
	endpoint := c.pulsar.endpoint(c.basePath, data.ClusterName, "failureDomains", data.DomainName)
	return c.pulsar.Client.Post(ctx, endpoint, &data)
}

func (c *clusters) GetFailureDomain(clusterName string, domainName string) (utils.FailureDomainData, error) {
	return c.GetFailureDomainWithContext(context.Background(), clusterName, domainName)
}

func (c *clusters) GetFailureDomainWithContext(ctx context.Context, clusterName string, domainName string) (utils.FailureDomainData, error) {
	var res utils.FailureDomainData
	endpoint := c.pulsar.endpoint(c.basePath, clusterName, "failureDomains", domainName)
	err := c.pulsar.Client.Get(ctx, endpoint, &res)
	return res, err
}

func (c *clusters) ListFailureDomains(clusterName string) (utils.FailureDomainMap, error) {
	return c.ListFailureDomainsWithContext(context.Background(), clusterName)
}

func (c *clusters) ListFailureDomainsWithContext(ctx context.Context, clusterName string) (utils.FailureDomainMap, error) {
	var domainData utils.FailureDomainMap
	endpoint := c.pulsar.endpoint(c.basePath, clusterName, "failureDomains")
	err := c.pulsar.Client.Get(ctx, endpoint, &domainData)
	return domainData, err
}

func (c *clusters) DeleteFailureDomain(data utils.FailureDomainData) error {
	return c.DeleteFailureDomainWithContext(context.TODO(), data)
}

func (c *clusters) DeleteFailureDomainWithContext(ctx context.Context, data utils.FailureDomainData) error {
	endpoint := c.pulsar.endpoint(c.basePath, data.ClusterName, "failureDomains", data.DomainName)
	return c.pulsar.Client.Delete(ctx, endpoint)
}

func (c *clusters) UpdateFailureDomain(data utils.FailureDomainData) error {
	return c.UpdateFailureDomainWithContext(context.Background(), data)
}

func (c *clusters) UpdateFailureDomainWithContext(ctx context.Context, data utils.FailureDomainData) error {
	endpoint := c.pulsar.endpoint(c.basePath, data.ClusterName, "failureDomains", data.DomainName)
	return c.pulsar.Client.Post(ctx, endpoint, &data)
}
