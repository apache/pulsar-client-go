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

// NamespaceMessageTTL Discard data after some time (by automatically acknowledging)
type NamespaceMessageTTL interface {
	// GetNamespaceMessageTTL Get the message TTL for the namespace
	GetNamespaceMessageTTL(tenant, namespace string) (int64, error)
	// SetNamespaceMessageTTL Set the message TTL for the namespace
	SetNamespaceMessageTTL(tenant, namespace string, seconds int64) error
	// RemoveNamespaceMessageTTL Remove the message TTL for the namespace
	RemoveNamespaceMessageTTL(tenant, namespace string) error
}

// TopicMessageTTL Discard data after some time (by automatically acknowledging)
type TopicMessageTTL interface {
	// GetTopicMessageTTL Get the message TTL for the topic
	GetTopicMessageTTL(tenant, namespace, topic string) (int64, error)
	// SetTopicMessageTTL Set the message TTL for the topic
	SetTopicMessageTTL(tenant, namespace, topic string, seconds int64) error
	// RemoveTopicMessageTTL Remove the message TTL for the topic
	RemoveTopicMessageTTL(tenant, namespace, topic string) error
}
