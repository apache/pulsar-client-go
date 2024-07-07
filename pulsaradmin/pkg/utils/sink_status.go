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

type SinkStatus struct {
	// The total number of sink instances that ought to be running
	NumInstances int `json:"numInstances"`

	// The number of source instances that are actually running
	NumRunning int `json:"numRunning"`

	Instances []*SinkInstanceStatus `json:"instances"`
}

type SinkInstanceStatus struct {
	InstanceID int                    `json:"instanceId"`
	Status     SinkInstanceStatusData `json:"status"`
}

type SinkInstanceStatusData struct {
	// Is this instance running?
	Running bool `json:"running"`

	// Do we have any error while running this instance
	Err string `json:"error"`

	// Number of times this instance has restarted
	NumRestarts int64 `json:"numRestarts"`

	// Number of messages read from Pulsar
	NumReadFromPulsar int64 `json:"numReadFromPulsar"`

	// Number of times there was a system exception handling messages
	NumSystemExceptions int64 `json:"numSystemExceptions"`

	// A list of the most recent system exceptions
	LatestSystemExceptions []ExceptionInformation `json:"latestSystemExceptions"`

	// Number of times there was a sink exception
	NumSinkExceptions int64 `json:"numSinkExceptions"`

	// A list of the most recent sink exceptions
	LatestSinkExceptions []ExceptionInformation `json:"latestSinkExceptions"`

	// Number of messages written to sink
	NumWrittenToSink int64 `json:"numWrittenToSink"`

	// When was the last time we received a Message from Pulsar
	LastReceivedTime int64 `json:"lastReceivedTime"`

	WorkerID string `json:"workerId"`
}
