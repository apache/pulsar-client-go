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

// BrokerStats is admin interface for broker stats management
type BrokerStats interface {
	// GetMetrics returns Monitoring metrics
	GetMetrics() ([]utils.Metrics, error)

	// GetMetricsWithContext returns Monitoring metrics
	GetMetricsWithContext(context.Context) ([]utils.Metrics, error)

	// GetMBeans requests JSON string server mbean dump
	GetMBeans() ([]utils.Metrics, error)

	// GetMBeansWithContext requests JSON string server mbean dump
	GetMBeansWithContext(context.Context) ([]utils.Metrics, error)

	// GetTopics returns JSON string topics stats
	GetTopics() (string, error)

	// GetTopicsWithContext returns JSON string topics stats
	GetTopicsWithContext(context.Context) (string, error)

	// GetLoadReport returns load report of broker
	GetLoadReport() (*utils.LocalBrokerData, error)

	// GetLoadReport returns load report of broker
	GetLoadReportWithContext(context.Context) (*utils.LocalBrokerData, error)

	// GetAllocatorStats returns stats from broker
	GetAllocatorStats(allocatorName string) (*utils.AllocatorStats, error)

	// GetAllocatorStatsWithContext returns stats from broker
	GetAllocatorStatsWithContext(ctx context.Context, allocatorName string) (*utils.AllocatorStats, error)
}

type brokerStats struct {
	pulsar   *pulsarClient
	basePath string
}

// BrokerStats is used to access the broker stats endpoints
func (c *pulsarClient) BrokerStats() BrokerStats {
	return &brokerStats{
		pulsar:   c,
		basePath: "/broker-stats",
	}
}

func (bs *brokerStats) GetMetrics() ([]utils.Metrics, error) {
	return bs.GetMetricsWithContext(context.Background())
}

func (bs *brokerStats) GetMetricsWithContext(ctx context.Context) ([]utils.Metrics, error) {
	endpoint := bs.pulsar.endpoint(bs.basePath, "/metrics")
	var response []utils.Metrics
	err := bs.pulsar.Client.Get(ctx, endpoint, &response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (bs *brokerStats) GetMBeans() ([]utils.Metrics, error) {
	return bs.GetMBeansWithContext(context.Background())
}

func (bs *brokerStats) GetMBeansWithContext(ctx context.Context) ([]utils.Metrics, error) {
	endpoint := bs.pulsar.endpoint(bs.basePath, "/mbeans")
	var response []utils.Metrics
	err := bs.pulsar.Client.Get(ctx, endpoint, &response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (bs *brokerStats) GetTopics() (string, error) {
	return bs.GetTopicsWithContext(context.Background())
}

func (bs *brokerStats) GetTopicsWithContext(ctx context.Context) (string, error) {
	endpoint := bs.pulsar.endpoint(bs.basePath, "/topics")
	buf, err := bs.pulsar.Client.GetWithQueryParams(ctx, endpoint, nil, nil, false)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func (bs *brokerStats) GetLoadReport() (*utils.LocalBrokerData, error) {
	return bs.GetLoadReportWithContext(context.Background())
}

func (bs *brokerStats) GetLoadReportWithContext(ctx context.Context) (*utils.LocalBrokerData, error) {
	endpoint := bs.pulsar.endpoint(bs.basePath, "/load-report")
	response := utils.NewLocalBrokerData()
	err := bs.pulsar.Client.Get(ctx, endpoint, &response)
	if err != nil {
		return nil, nil
	}
	return &response, nil
}

func (bs *brokerStats) GetAllocatorStats(allocatorName string) (*utils.AllocatorStats, error) {
	return bs.GetAllocatorStatsWithContext(context.Background(), allocatorName)
}

func (bs *brokerStats) GetAllocatorStatsWithContext(ctx context.Context, allocatorName string) (*utils.AllocatorStats, error) {
	endpoint := bs.pulsar.endpoint(bs.basePath, "/allocator-stats", allocatorName)
	var allocatorStats utils.AllocatorStats
	err := bs.pulsar.Client.Get(ctx, endpoint, &allocatorStats)
	if err != nil {
		return nil, err
	}
	return &allocatorStats, nil
}
