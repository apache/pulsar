package admin


// Clusters  is used to access the cluster endpoints.
type Clusters interface {
  List() ([]string, error)
  Get(string) (ClusterData, error)
  Create(ClusterData) error
  Delete(string) error
}

type clusters struct {
  client *client
  basepath string
}

func (c *client) Clusters() Clusters {
  return &clusters{client:c, basepath:"/admin/v2/clusters"}
}

func (c *clusters) List() ([]string, error) {
  var clusters []string
  err := c.client.get(c.basepath, &clusters)
  return clusters, err
}

func (c *clusters) Get(name string) (ClusterData, error) {
  cdata := ClusterData{}
  endpoint := endpoint(c.basepath, name)
  err := c.client.get(endpoint, &cdata)
  return cdata, err
}

func (c *clusters) Create(cdata ClusterData) error {
  endpoint := endpoint(c.basepath, cdata.Name)
  err := c.client.put(endpoint, &cdata, nil)
  return err
}

func (c *clusters) Delete(name string) error {
  endpoint := endpoint(c.basepath, name)
  return c.client.delete(endpoint, nil)
}
