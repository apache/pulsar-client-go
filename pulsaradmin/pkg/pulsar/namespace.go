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

type Namespaces interface {
	// Get the list of all the namespaces for a certain tenant
	GetNamespaces(tenant string) ([]string, error)

	// Get the list of all the topics under a certain namespace
	GetTopics(namespace string) ([]string, error)

	// Get the dump all the policies specified for a namespace
	GetPolicies(namespace string) (*Policies, error)

	// Creates a new empty namespace with no policies attached
	CreateNamespace(namespace string) error

	// Creates a new empty namespace with no policies attached
	CreateNsWithNumBundles(namespace string, numBundles int) error

	// Creates a new namespace with the specified policies
	CreateNsWithPolices(namespace string, polices Policies) error

	// Creates a new empty namespace with no policies attached
	CreateNsWithBundlesData(namespace string, bundleData *BundlesData) error

	// Delete an existing namespace
	DeleteNamespace(namespace string) error

	// Delete an existing bundle in a namespace
	DeleteNamespaceBundle(namespace string, bundleRange string) error
}

type namespaces struct {
	client   *client
	basePath string
}

func (c *client) Namespaces() Namespaces {
	return &namespaces{
		client:   c,
		basePath: "/namespaces",
	}
}

func (n *namespaces) GetNamespaces(tenant string) ([]string, error) {
	var namespaces []string
	endpoint := n.client.endpoint(n.basePath, tenant)
	err := n.client.get(endpoint, &namespaces)
	return namespaces, err
}

func (n *namespaces) GetTopics(namespace string) ([]string, error) {
	var topics []string
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String(), "topics")
	err = n.client.get(endpoint, &topics)
	return topics, err
}

func (n *namespaces) GetPolicies(namespace string) (*Policies, error) {
	var police Policies
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return nil, err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String())
	err = n.client.get(endpoint, &police)
	return &police, err
}

func (n *namespaces) CreateNsWithNumBundles(namespace string, numBundles int) error {
	return n.CreateNsWithBundlesData(namespace, NewBundlesDataWithNumBundles(numBundles))
}

func (n *namespaces) CreateNsWithPolices(namespace string, policies Policies) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String())
	return n.client.put(endpoint, &policies, nil)
}

func (n *namespaces) CreateNsWithBundlesData(namespace string, bundleData *BundlesData) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String())
	polices := new(Policies)
	polices.Bundles = bundleData

	return n.client.put(endpoint, &polices, nil)
}

func (n *namespaces) CreateNamespace(namespace string) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String())
	return n.client.put(endpoint, nil, nil)
}

func (n *namespaces) DeleteNamespace(namespace string) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String())
	return n.client.delete(endpoint, nil)
}

func (n *namespaces) DeleteNamespaceBundle(namespace string, bundleRange string) error {
	ns, err := GetNamespaceName(namespace)
	if err != nil {
		return err
	}
	endpoint := n.client.endpoint(n.basePath, ns.String(), bundleRange)
	return n.client.delete(endpoint, nil)
}
