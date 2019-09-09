package pulsar

type Tenants interface {
	Create(TenantData) error
	Delete(string) error
	Update(TenantData) error
	List() ([]string, error)
	Get(string) (TenantData, error)
}

type tenants struct {
	client *client
	basePath string
}

func (c *client) Tenants() Tenants {
	return &tenants{
		client: c,
		basePath: "/tenants",
	}
}

func (c *tenants) Create(data TenantData) error {
	endpoint := c.client.endpoint(c.basePath, data.Name)
	return c.client.put(endpoint, &data, nil)
}

func (c *tenants) Delete(name string) error {
	endpoint := c.client.endpoint(c.basePath, name)
	return c.client.delete(endpoint, nil)
}

func (c *tenants) Update(data TenantData) error {
	endpoint := c.client.endpoint(c.basePath, data.Name)
	return c.client.post(endpoint, &data, nil)
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
