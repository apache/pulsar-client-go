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

type FunctionsWorker interface {
	// GetFunctionsStats returns all functions stats on a worker
	GetFunctionsStats() ([]*utils.WorkerFunctionInstanceStats, error)

	// GetFunctionsStatsWithContext returns all functions stats on a worker
	GetFunctionsStatsWithContext(context.Context) ([]*utils.WorkerFunctionInstanceStats, error)

	// GetMetrics returns worker metrics
	GetMetrics() ([]*utils.Metrics, error)

	// GetMetricsWithContext returns worker metrics
	GetMetricsWithContext(context.Context) ([]*utils.Metrics, error)

	// GetCluster returns all workers belonging to this cluster
	GetCluster() ([]*utils.WorkerInfo, error)

	// GetClusterWithContext returns all workers belonging to this cluster
	GetClusterWithContext(context.Context) ([]*utils.WorkerInfo, error)

	// GetClusterLeader returns the leader worker of the cluster
	GetClusterLeader() (*utils.WorkerInfo, error)

	// GetClusterLeaderWithContext returns the leader worker of the cluster
	GetClusterLeaderWithContext(context.Context) (*utils.WorkerInfo, error)

	// GetAssignments returns the cluster assignments
	GetAssignments() (map[string][]string, error)

	// GetAssignmentsWithContext returns the cluster assignments
	GetAssignmentsWithContext(context.Context) (map[string][]string, error)
}

type worker struct {
	pulsar          *pulsarClient
	workerPath      string
	workerStatsPath string
}

func (c *pulsarClient) FunctionsWorker() FunctionsWorker {
	return &worker{
		pulsar:          c,
		workerPath:      "/worker",
		workerStatsPath: "/worker-stats",
	}
}

func (w *worker) GetFunctionsStats() ([]*utils.WorkerFunctionInstanceStats, error) {
	return w.GetFunctionsStatsWithContext(context.Background())
}

func (w *worker) GetFunctionsStatsWithContext(ctx context.Context) ([]*utils.WorkerFunctionInstanceStats, error) {
	endpoint := w.pulsar.endpoint(w.workerStatsPath, "functionsmetrics")
	var workerStats []*utils.WorkerFunctionInstanceStats
	err := w.pulsar.Client.Get(ctx, endpoint, &workerStats)
	if err != nil {
		return nil, err
	}
	return workerStats, nil
}

func (w *worker) GetMetrics() ([]*utils.Metrics, error) {
	return w.GetMetricsWithContext(context.Background())
}

func (w *worker) GetMetricsWithContext(ctx context.Context) ([]*utils.Metrics, error) {
	endpoint := w.pulsar.endpoint(w.workerStatsPath, "metrics")
	var metrics []*utils.Metrics
	err := w.pulsar.Client.Get(ctx, endpoint, &metrics)
	if err != nil {
		return nil, err
	}
	return metrics, nil
}

func (w *worker) GetCluster() ([]*utils.WorkerInfo, error) {
	return w.GetClusterWithContext(context.Background())
}

func (w *worker) GetClusterWithContext(ctx context.Context) ([]*utils.WorkerInfo, error) {
	endpoint := w.pulsar.endpoint(w.workerPath, "cluster")
	var workersInfo []*utils.WorkerInfo
	err := w.pulsar.Client.Get(ctx, endpoint, &workersInfo)
	if err != nil {
		return nil, err
	}
	return workersInfo, nil
}

func (w *worker) GetClusterLeader() (*utils.WorkerInfo, error) {
	return w.GetClusterLeaderWithContext(context.Background())
}

func (w *worker) GetClusterLeaderWithContext(ctx context.Context) (*utils.WorkerInfo, error) {
	endpoint := w.pulsar.endpoint(w.workerPath, "cluster", "leader")
	var workerInfo utils.WorkerInfo
	err := w.pulsar.Client.Get(ctx, endpoint, &workerInfo)
	if err != nil {
		return nil, err
	}
	return &workerInfo, nil
}

func (w *worker) GetAssignments() (map[string][]string, error) {
	return w.GetAssignmentsWithContext(context.Background())
}

func (w *worker) GetAssignmentsWithContext(ctx context.Context) (map[string][]string, error) {
	endpoint := w.pulsar.endpoint(w.workerPath, "assignments")
	var assignments map[string][]string
	err := w.pulsar.Client.Get(ctx, endpoint, &assignments)
	if err != nil {
		return nil, err
	}
	return assignments, nil
}
