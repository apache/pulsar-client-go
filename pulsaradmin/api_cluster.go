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

package pulsaradmin

// Clusters is admin interface for clusters management
type Clusters interface {
	// List returns the list of clusters
	List() ([]string, error)

	// Get the configuration data for the specified cluster
	Get(string) (ClusterData, error)

	// Create a new cluster
	Create(ClusterData) error

	// Delete an existing cluster
	Delete(string) error

	// Update the configuration for a cluster
	Update(ClusterData) error

	// UpdatePeerClusters updates peer cluster names.
	UpdatePeerClusters(string, []string) error

	// GetPeerClusters returns peer-cluster names
	GetPeerClusters(string) ([]string, error)

	// CreateFailureDomain creates a domain into cluster
	CreateFailureDomain(FailureDomainData) error

	// GetFailureDomain returns the domain registered into a cluster
	GetFailureDomain(clusterName, domainName string) (FailureDomainData, error)

	// ListFailureDomains returns all registered domains in cluster
	ListFailureDomains(string) (FailureDomainMap, error)

	// DeleteFailureDomain deletes a domain in cluster
	DeleteFailureDomain(FailureDomainData) error

	// UpdateFailureDomain updates a domain into cluster
	UpdateFailureDomain(FailureDomainData) error
}

type clusters struct {
	pulsar     *pulsarClient
	basePath   string
	apiVersion APIVersion
}

// Clusters is used to access the cluster endpoints.
func (c *pulsarClient) Clusters() Clusters {
	return &clusters{
		pulsar:     c,
		basePath:   "/clusters",
		apiVersion: c.apiProfile.Clusters,
	}
}

func (c *clusters) List() ([]string, error) {
	var clusters []string
	err := c.pulsar.restClient.Get(c.pulsar.endpoint(c.apiVersion, c.basePath), &clusters)
	return clusters, err
}

func (c *clusters) Get(name string) (ClusterData, error) {
	cdata := ClusterData{}
	endpoint := c.pulsar.endpoint(c.apiVersion, c.basePath, name)
	err := c.pulsar.restClient.Get(endpoint, &cdata)
	return cdata, err
}

func (c *clusters) Create(cdata ClusterData) error {
	endpoint := c.pulsar.endpoint(c.apiVersion, c.basePath, cdata.Name)
	return c.pulsar.restClient.Put(endpoint, &cdata)
}

func (c *clusters) Delete(name string) error {
	endpoint := c.pulsar.endpoint(c.apiVersion, c.basePath, name)
	return c.pulsar.restClient.Delete(endpoint)
}

func (c *clusters) Update(cdata ClusterData) error {
	endpoint := c.pulsar.endpoint(c.apiVersion, c.basePath, cdata.Name)
	return c.pulsar.restClient.Post(endpoint, &cdata)
}

func (c *clusters) GetPeerClusters(name string) ([]string, error) {
	var peerClusters []string
	endpoint := c.pulsar.endpoint(c.apiVersion, c.basePath, name, "peers")
	err := c.pulsar.restClient.Get(endpoint, &peerClusters)
	return peerClusters, err
}

func (c *clusters) UpdatePeerClusters(cluster string, peerClusters []string) error {
	endpoint := c.pulsar.endpoint(c.apiVersion, c.basePath, cluster, "peers")
	return c.pulsar.restClient.Post(endpoint, peerClusters)
}

func (c *clusters) CreateFailureDomain(data FailureDomainData) error {
	endpoint := c.pulsar.endpoint(c.apiVersion, c.basePath, data.ClusterName, "failureDomains", data.DomainName)
	return c.pulsar.restClient.Post(endpoint, &data)
}

func (c *clusters) GetFailureDomain(clusterName string, domainName string) (FailureDomainData, error) {
	var res FailureDomainData
	endpoint := c.pulsar.endpoint(c.apiVersion, c.basePath, clusterName, "failureDomains", domainName)
	err := c.pulsar.restClient.Get(endpoint, &res)
	return res, err
}

func (c *clusters) ListFailureDomains(clusterName string) (FailureDomainMap, error) {
	var domainData FailureDomainMap
	endpoint := c.pulsar.endpoint(c.apiVersion, c.basePath, clusterName, "failureDomains")
	err := c.pulsar.restClient.Get(endpoint, &domainData)
	return domainData, err
}

func (c *clusters) DeleteFailureDomain(data FailureDomainData) error {
	endpoint := c.pulsar.endpoint(c.apiVersion, c.basePath, data.ClusterName, "failureDomains", data.DomainName)
	return c.pulsar.restClient.Delete(endpoint)
}

func (c *clusters) UpdateFailureDomain(data FailureDomainData) error {
	endpoint := c.pulsar.endpoint(c.apiVersion, c.basePath, data.ClusterName, "failureDomains", data.DomainName)
	return c.pulsar.restClient.Post(endpoint, &data)
}
