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
	err := c.client.put(endpoint, &cdata, nil)
	return err
}

func (c *clusters) Delete(name string) error {
	endpoint := c.client.endpoint(c.basePath, name)
	return c.client.delete(endpoint, nil)
}

func (c *clusters) Update(cdata ClusterData) error {
	endpoint := c.client.endpoint(c.basePath, cdata.Name)
	return c.client.post(endpoint, &cdata, nil)
}

func (c *clusters) UpdatePeerClusters(cluster string, peerClusters []string) error {
	endpoint := c.client.endpoint(c.basePath, cluster, "peers")
	return c.client.post(endpoint, peerClusters, nil)
}
func (c *clusters) GetPeerClusters(name string) ([]string, error) {
	var peerClusters []string
	endpoint := c.client.endpoint(c.basePath, name, "peers")
	err := c.client.get(endpoint, &peerClusters)
	return peerClusters, err
}
