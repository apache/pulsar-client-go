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

package utils

type FunctionStatus struct {
	NumInstances int                      `json:"numInstances"`
	NumRunning   int                      `json:"numRunning"`
	Instances    []FunctionInstanceStatus `json:"instances"`
}

type FunctionInstanceStatus struct {
	InstanceID int                        `json:"instanceId"`
	Status     FunctionInstanceStatusData `json:"status"`
}

type FunctionInstanceStatusData struct {
	Running                  bool                   `json:"running"`
	Err                      string                 `json:"error"`
	NumRestarts              int64                  `json:"numRestarts"`
	NumReceived              int64                  `json:"numReceived"`
	NumSuccessfullyProcessed int64                  `json:"numSuccessfullyProcessed"`
	NumUserExceptions        int64                  `json:"numUserExceptions"`
	LatestUserExceptions     []ExceptionInformation `json:"latestUserExceptions"`
	NumSystemExceptions      int64                  `json:"numSystemExceptions"`
	LatestSystemExceptions   []ExceptionInformation `json:"latestSystemExceptions"`
	AverageLatency           float64                `json:"averageLatency"`
	LastInvocationTime       int64                  `json:"lastInvocationTime"`
	WorkerID                 string                 `json:"workerId"`
}

type ExceptionInformation struct {
	ExceptionString string `json:"exceptionString"`
	TimestampMs     int64  `json:"timestampMs"`
}
