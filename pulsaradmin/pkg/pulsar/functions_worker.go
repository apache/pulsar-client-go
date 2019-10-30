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

import (
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/utils"
)

type FunctionsWorker interface {
	// Get all functions stats on a worker
	GetFunctionsStats() ([]*utils.WorkerFunctionInstanceStats, error)

	// Get worker metrics
	GetMetrics() ([]*utils.Metrics, error)

	// Get List of all workers belonging to this cluster
	GetCluster() ([]*utils.WorkerInfo, error)

	// Get the worker who is the leader of the clusterv
	GetClusterLeader() (*utils.WorkerInfo, error)

	// Get the function assignment among the cluster
	GetAssignments() (map[string][]string, error)
}

type worker struct {
	client          *client
	workerPath      string
	workerStatsPath string
}

func (c *client) FunctionsWorker() FunctionsWorker {
	return &worker{
		client:          c,
		workerPath:      "/worker",
		workerStatsPath: "/worker-stats",
	}
}

func (w *worker) GetFunctionsStats() ([]*utils.WorkerFunctionInstanceStats, error) {
	endpoint := w.client.endpoint(w.workerStatsPath, "functionsmetrics")
	var workerStats []*utils.WorkerFunctionInstanceStats
	err := w.client.get(endpoint, &workerStats)
	if err != nil {
		return nil, err
	}
	return workerStats, nil
}

func (w *worker) GetMetrics() ([]*utils.Metrics, error) {
	endpoint := w.client.endpoint(w.workerStatsPath, "metrics")
	var metrics []*utils.Metrics
	err := w.client.get(endpoint, &metrics)
	if err != nil {
		return nil, err
	}
	return metrics, nil
}

func (w *worker) GetCluster() ([]*utils.WorkerInfo, error) {
	endpoint := w.client.endpoint(w.workerPath, "cluster")
	var workersInfo []*utils.WorkerInfo
	err := w.client.get(endpoint, &workersInfo)
	if err != nil {
		return nil, err
	}
	return workersInfo, nil
}

func (w *worker) GetClusterLeader() (*utils.WorkerInfo, error) {
	endpoint := w.client.endpoint(w.workerPath, "cluster", "leader")
	var workerInfo utils.WorkerInfo
	err := w.client.get(endpoint, &workerInfo)
	if err != nil {
		return nil, err
	}
	return &workerInfo, nil
}

func (w *worker) GetAssignments() (map[string][]string, error) {
	endpoint := w.client.endpoint(w.workerPath, "assignments")
	var assignments map[string][]string
	err := w.client.get(endpoint, &assignments)
	if err != nil {
		return nil, err
	}
	return assignments, nil
}
