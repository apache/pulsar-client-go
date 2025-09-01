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

// TransactionCoordinatorStats represents transaction coordinator statistics
type TransactionCoordinatorStats struct {
	CoordinatorID               int                           `json:"coordinatorId"`
	State                       string                        `json:"state"`
	LeastSigBits                int64                         `json:"leastSigBits"`
	MostSigBits                 int64                         `json:"mostSigBits"`
	TotalSize                   int64                         `json:"totalSize"`
	Clusters                    []string                      `json:"clusters"`
	LowWaterMark               int64                         `json:"lowWaterMark"`
	HighWaterMark              int64                         `json:"highWaterMark"`
	TransactionCount           int64                         `json:"transactionCount"`
	ProducedCount              int64                         `json:"producedCount"`
	AckedCount                 int64                         `json:"ackedCount"`
	OngoingTxnCount           int64                         `json:"ongoingTxnCount"`
	RecoveringTxnCount        int64                         `json:"recoveringTxnCount"`
	TimeoutTxnCount           int64                         `json:"timeoutTxnCount"`
	CommittedTxnCount         int64                         `json:"committedTxnCount"`
	AbortedTxnCount           int64                         `json:"abortedTxnCount"`
	ExecutionLatencyPercentiles map[string]float64            `json:"executionLatencyPercentiles"`
	CommitLatencyPercentiles   map[string]float64            `json:"commitLatencyPercentiles"`
	AbortLatencyPercentiles    map[string]float64            `json:"abortLatencyPercentiles"`
}