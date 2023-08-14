// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
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
	endpoint := w.pulsar.endpoint(w.workerStatsPath, "functionsmetrics")
	var workerStats []*utils.WorkerFunctionInstanceStats
	err := w.pulsar.Client.Get(endpoint, &workerStats)
	if err != nil {
		return nil, err
	}
	return workerStats, nil
}

func (w *worker) GetMetrics() ([]*utils.Metrics, error) {
	endpoint := w.pulsar.endpoint(w.workerStatsPath, "metrics")
	var metrics []*utils.Metrics
	err := w.pulsar.Client.Get(endpoint, &metrics)
	if err != nil {
		return nil, err
	}
	return metrics, nil
}

func (w *worker) GetCluster() ([]*utils.WorkerInfo, error) {
	endpoint := w.pulsar.endpoint(w.workerPath, "cluster")
	var workersInfo []*utils.WorkerInfo
	err := w.pulsar.Client.Get(endpoint, &workersInfo)
	if err != nil {
		return nil, err
	}
	return workersInfo, nil
}

func (w *worker) GetClusterLeader() (*utils.WorkerInfo, error) {
	endpoint := w.pulsar.endpoint(w.workerPath, "cluster", "leader")
	var workerInfo utils.WorkerInfo
	err := w.pulsar.Client.Get(endpoint, &workerInfo)
	if err != nil {
		return nil, err
	}
	return &workerInfo, nil
}

func (w *worker) GetAssignments() (map[string][]string, error) {
	endpoint := w.pulsar.endpoint(w.workerPath, "assignments")
	var assignments map[string][]string
	err := w.pulsar.Client.Get(endpoint, &assignments)
	if err != nil {
		return nil, err
	}
	return assignments, nil
}
