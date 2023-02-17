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

// Topics persistent topic and non-persistent topic interface
type Topics interface {
	CreateNonPartitioned(tenant, namespace, topic string) error
	CreatePartitioned(tenant, namespace, topic string, numPartitions int) error
	DeleteNonPartitioned(tenant, namespace, topic string) error
	DeletePartitioned(tenant, namespace, topic string) error
	ListNonPartitioned(tenant, namespace string) ([]string, error)
	ListPartitioned(tenant, namespace string) ([]string, error)
	ListNamespaceTopics(tenant, namespace string) ([]string, error)
	GetPartitionedMetadata(tenant, namespace, topic string) (*PartitionedMetadata, error)
	TopicRetention
}

// PartitionedMetadata partitioned topic metadata
type PartitionedMetadata struct {
	Deleted    bool  `json:"deleted"`
	Partitions int64 `json:"partitions"`
}
