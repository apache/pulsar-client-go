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

package pulsaradmin

type FunctionsWorker interface {
	// Get all functions stats on a worker
	GetFunctionsStats() ([]*WorkerFunctionInstanceStats, error)

	// Get worker metrics
	GetMetrics() ([]*Metrics, error)

	// Get List of all workers belonging to this cluster
	GetCluster() ([]*WorkerInfo, error)

	// Get the worker who is the leader of the clusterv
	GetClusterLeader() (*WorkerInfo, error)

	// Get the function assignment among the cluster
	GetAssignments() (map[string][]string, error)
}

type worker struct {
	pulsar          *pulsarClient
	workerPath      string
	workerStatsPath string
	apiVersion      APIVersion
}

func (c *pulsarClient) FunctionsWorker() FunctionsWorker {
	return &worker{
		pulsar:          c,
		workerPath:      "/worker",
		workerStatsPath: "/worker-stats",
		apiVersion:      c.apiProfile.FunctionsWorker,
	}
}

func (w *worker) GetFunctionsStats() ([]*WorkerFunctionInstanceStats, error) {
	endpoint := w.pulsar.endpoint(w.apiVersion, w.workerStatsPath, "functionsmetrics")
	var workerStats []*WorkerFunctionInstanceStats
	err := w.pulsar.restClient.Get(endpoint, &workerStats)
	if err != nil {
		return nil, err
	}
	return workerStats, nil
}

func (w *worker) GetMetrics() ([]*Metrics, error) {
	endpoint := w.pulsar.endpoint(w.apiVersion, w.workerStatsPath, "metrics")
	var metrics []*Metrics
	err := w.pulsar.restClient.Get(endpoint, &metrics)
	if err != nil {
		return nil, err
	}
	return metrics, nil
}

func (w *worker) GetCluster() ([]*WorkerInfo, error) {
	endpoint := w.pulsar.endpoint(w.apiVersion, w.workerPath, "cluster")
	var workersInfo []*WorkerInfo
	err := w.pulsar.restClient.Get(endpoint, &workersInfo)
	if err != nil {
		return nil, err
	}
	return workersInfo, nil
}

func (w *worker) GetClusterLeader() (*WorkerInfo, error) {
	endpoint := w.pulsar.endpoint(w.apiVersion, w.workerPath, "cluster", "leader")
	var workerInfo WorkerInfo
	err := w.pulsar.restClient.Get(endpoint, &workerInfo)
	if err != nil {
		return nil, err
	}
	return &workerInfo, nil
}

func (w *worker) GetAssignments() (map[string][]string, error) {
	endpoint := w.pulsar.endpoint(w.apiVersion, w.workerPath, "assignments")
	var assignments map[string][]string
	err := w.pulsar.restClient.Get(endpoint, &assignments)
	if err != nil {
		return nil, err
	}
	return assignments, nil
}
