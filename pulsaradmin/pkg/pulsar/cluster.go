package pulsar

// Clusters  is used to access the cluster endpoints.

type Clusters interface {
	List() ([]string, error)
	Get(string) (ClusterData, error)
	Create(ClusterData) error
	Delete(string) error
	Update(ClusterData) error
	UpdatePeerClusters(string, []string) error
	GetPeerClusters(string) ([]string, error)
	CreateFailureDomain(FailureDomainData) error
	GetFailureDomain(clusterName, domainName string) (FailureDomainData, error)
	ListFailureDomains(string) (FailureDomainMap, error)
	DeleteFailureDomain(FailureDomainData) error
	UpdateFailureDomain(FailureDomainData) error
}

type clusters struct {
	client   *client
	basePath string
}

func (c *client) Clusters() Clusters {
	return &clusters{
		client:   c,
		basePath: "/clusters",
	}
}

func (c *clusters) List() ([]string, error) {
	var clusters []string
	err := c.client.get(c.client.endpoint(c.basePath), &clusters)
	return clusters, err
}

func (c *clusters) Get(name string) (ClusterData, error) {
	cdata := ClusterData{}
	endpoint := c.client.endpoint(c.basePath, name)
	err := c.client.get(endpoint, &cdata)
	return cdata, err
}

func (c *clusters) Create(cdata ClusterData) error {
	endpoint := c.client.endpoint(c.basePath, cdata.Name)
	return c.client.put(endpoint, &cdata, nil)
}

func (c *clusters) Delete(name string) error {
	endpoint := c.client.endpoint(c.basePath, name)
	return c.client.delete(endpoint, nil)
}

func (c *clusters) Update(cdata ClusterData) error {
	endpoint := c.client.endpoint(c.basePath, cdata.Name)
	return c.client.post(endpoint, &cdata, nil)
}

func (c *clusters) GetPeerClusters(name string) ([]string, error) {
	var peerClusters []string
	endpoint := c.client.endpoint(c.basePath, name, "peers")
	err := c.client.get(endpoint, &peerClusters)
	return peerClusters, err
}

func (c *clusters) UpdatePeerClusters(cluster string, peerClusters []string) error {
	endpoint := c.client.endpoint(c.basePath, cluster, "peers")
	return c.client.post(endpoint, peerClusters, nil)
}

func (c *clusters) CreateFailureDomain(data FailureDomainData) error {
	endpoint := c.client.endpoint(c.basePath, data.ClusterName, "failureDomains", data.DomainName)
	return c.client.post(endpoint, &data, nil)
}

func (c *clusters) GetFailureDomain(clusterName string, domainName string) (FailureDomainData, error) {
	var res FailureDomainData
	endpoint := c.client.endpoint(c.basePath, clusterName, "failureDomains", domainName)
	err := c.client.get(endpoint, &res)
	return res, err
}

func (c *clusters) ListFailureDomains(clusterName string) (FailureDomainMap, error) {
	var domainData FailureDomainMap
	endpoint := c.client.endpoint(c.basePath, clusterName, "failureDomains")
	err := c.client.get(endpoint, &domainData)
	return domainData, err
}

func (c *clusters) DeleteFailureDomain(data FailureDomainData) error {
	endpoint := c.client.endpoint(c.basePath, data.ClusterName, "failureDomains", data.DomainName)
	return c.client.delete(endpoint, nil)
}
func (c *clusters) UpdateFailureDomain(data FailureDomainData) error {
	endpoint := c.client.endpoint(c.basePath, data.ClusterName, "failureDomains", data.DomainName)
	return c.client.post(endpoint, &data, nil)
}
