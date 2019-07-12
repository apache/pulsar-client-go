package pulsar

// ClusterData information on a cluster
type ClusterData struct {
	Name string `json:"-"`
	ServiceURL string `json:"serviceUrl"`
	ServiceURLTls string `json:"serviceUrlTls"`
	BrokerServiceURL string `json:"brokerServiceUrl"`
	BrokerServiceURLTls string `json:"brokerServiceUrlTls"`
	PeerClusterNames []string `json:"peerClusterNames"`
}
