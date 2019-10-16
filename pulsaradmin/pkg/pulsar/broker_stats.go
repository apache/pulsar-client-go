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

type BrokerStats interface {
	// Returns Monitoring metrics
	GetMetrics() ([]Metrics, error)

	// Requests JSON string server mbean dump
	GetMBeans() ([]Metrics, error)

	// Returns JSON string topics stats
	GetTopics() (string, error)

	GetLoadReport() (*LocalBrokerData, error)

	GetAllocatorStats(allocatorName string) (*AllocatorStats, error)
}

type brokerStats struct {
	client   *client
	basePath string
}

func (c *client) BrokerStats() BrokerStats {
	return &brokerStats{
		client:   c,
		basePath: "/broker-stats",
	}
}

func (bs *brokerStats) GetMetrics() ([]Metrics, error) {
	endpoint := bs.client.endpoint(bs.basePath, "/metrics")
	var response []Metrics
	err := bs.client.get(endpoint, &response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (bs *brokerStats) GetMBeans() ([]Metrics, error) {
	endpoint := bs.client.endpoint(bs.basePath, "/mbeans")
	var response []Metrics
	err := bs.client.get(endpoint, &response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (bs *brokerStats) GetTopics() (string, error) {
	endpoint := bs.client.endpoint(bs.basePath, "/topics")
	buf, err := bs.client.getWithQueryParams(endpoint, nil, nil, false)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func (bs *brokerStats) GetLoadReport() (*LocalBrokerData, error) {
	endpoint := bs.client.endpoint(bs.basePath, "/load-report")
	response := NewLocalBrokerData()
	err := bs.client.get(endpoint, &response)
	if err != nil {
		return nil, nil
	}
	return &response, nil
}

func (bs *brokerStats) GetAllocatorStats(allocatorName string) (*AllocatorStats, error) {
	endpoint := bs.client.endpoint(bs.basePath, "/allocator-stats", allocatorName)
	var allocatorStats AllocatorStats
	err := bs.client.get(endpoint, &allocatorStats)
	if err != nil {
		return nil, err
	}
	return &allocatorStats, nil
}
