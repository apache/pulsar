package admin

// ClusterData information on a cluster
type ClusterData struct {
  Name string `json:"-"`
  ServiceURL string `json:"serviceUrl"`
  BrokerServiceURL string `json:"brokerServiceUrl"`
}
