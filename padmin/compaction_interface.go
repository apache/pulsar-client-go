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

package padmin

type NamespaceCompaction interface {
	// GetMaximumUnCompactedBytes Delete maximum number of uncompacted bytes in a topic before compaction is triggered.
	GetMaximumUnCompactedBytes(tenant, namespace string) (int64, error)
	// SetMaximumUnCompactedBytes Set maximum number of uncompacted bytes in a topic before compaction is triggered.
	SetMaximumUnCompactedBytes(tenant, namespace string, threshold int64) error
	// RemoveMaximumUnCompactedBytes Delete maximum number of uncompacted bytes in a topic before compaction is triggered.
	RemoveMaximumUnCompactedBytes(tenant, namespace string) error
}

type TopicCompaction interface {
	// GetTopicCompactionThreshold Get compaction threshold configuration for specified topic.
	GetTopicCompactionThreshold(tenant, namespace, topic string) (int64, error)
	// SetTopicCompactionThreshold Set compaction threshold configuration for specified topic.
	SetTopicCompactionThreshold(tenant, namespace, topic string, threshold int64) error
	// RemoveTopicCompactionThreshold Remove compaction threshold configuration for specified topic.
	RemoveTopicCompactionThreshold(tenant, namespace, topic string) error
	// GetTopicCompactionStatus Get the status of a compaction operation for a topic.
	GetTopicCompactionStatus(tenant, namespace, topic string) (*LongRunningProcessStatus, error)
	// TriggerTopicCompaction Trigger a compaction operation on a topic.
	TriggerTopicCompaction(tenant, namespace, topic string) error
}

type CompactionStatus string

const (
	NotRun  CompactionStatus = "NOT_RUN"
	Running CompactionStatus = "RUNNING"
	Success CompactionStatus = "SUCCESS"
	ERROR   CompactionStatus = "ERROR"
)

type LongRunningProcessStatus struct {
	Status    CompactionStatus `json:"status"`
	LastError string           `json:"lastError"`
}
