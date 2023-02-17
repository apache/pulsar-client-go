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

// NamespaceRetention operate interface about retention configuration for namespace.
type NamespaceRetention interface {
	// GetNamespaceRetention get retention configuration for namespace.
	GetNamespaceRetention(tenant, namespace string) (*RetentionConfiguration, error)
	// SetNamespaceRetention set retention configuration for namespace.
	SetNamespaceRetention(tenant, namespace string, cfg *RetentionConfiguration) error
	// RemoveNamespaceRetention remove retention configuration for namespace.
	RemoveNamespaceRetention(tenant, namespace string) error
}

// TopicRetention operate interface about retention configuration for specified topic.
type TopicRetention interface {
	// GetTopicRetention get retention configuration for namespace.
	GetTopicRetention(tenant, namespace, topic string) (*RetentionConfiguration, error)
	// SetTopicRetention set retention configuration for namespace.
	SetTopicRetention(tenant, namespace, topic string, cfg *RetentionConfiguration) error
	// RemoveTopicRetention remove retention configuration for namespace.
	RemoveTopicRetention(tenant, namespace, topic string) error
}

// RetentionConfiguration retention configuration
type RetentionConfiguration struct {
	RetentionSizeInMB      int64 `json:"retentionSizeInMB"`
	RetentionTimeInMinutes int64 `json:"retentionTimeInMinutes"`
}
