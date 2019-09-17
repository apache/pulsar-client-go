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

const (
	FirstBoundary string = "0x00000000"
	LastBoundary  string = "0xffffffff"
)

type Policies struct {
	AuthPolicies                          AuthPolicies                          `json:"auth_policies"`
	ReplicationClusters                   []string                              `json:"replication_clusters"`
	Bundles                               *BundlesData                          `json:"bundles"`
	BacklogQuotaMap                       map[BacklogQuotaType]BacklogQuota     `json:"backlog_quota_map"`
	TopicDispatchRate                     map[string]DispatchRate               `json:"topicDispatchRate"`
	SubscriptionDispatchRate              map[string]DispatchRate               `json:"subscriptionDispatchRate"`
	ReplicatorDispatchRate                map[string]DispatchRate               `json:"replicatorDispatchRate"`
	ClusterSubscribeRate                  map[string]SubscribeRate              `json:"clusterSubscribeRate"`
	Persistence                           *PersistencePolicies                  `json:"persistence"`
	DeduplicationEnabled                  bool                                  `json:"deduplicationEnabled"`
	LatencyStatsSampleRate                map[string]int                        `json:"latency_stats_sample_rate"`
	MessageTtlInSeconds                   int                                   `json:"message_ttl_in_seconds"`
	RetentionPolicies                     *RetentionPolicies                    `json:"retention_policies"`
	Deleted                               bool                                  `json:"deleted"`
	AntiAffinityGroup                     string                                `json:"antiAffinityGroup"`
	EncryptionRequired                    bool                                  `json:"encryption_required"`
	SubscriptionAuthMode                  SubscriptionAuthMode                  `json:"subscription_auth_mode"`
	MaxProducersPerTopic                  int                                   `json:"max_producers_per_topic"`
	MaxConsumersPerTopic                  int                                   `json:"max_consumers_per_topic"`
	MaxConsumersPerSubscription           int                                   `json:"max_consumers_per_subscription"`
	CompactionThreshold                   int64                                 `json:"compaction_threshold"`
	OffloadThreshold                      int64                                 `json:"offload_threshold"`
	OffloadDeletionLagMs                  int64                                 `json:"offload_deletion_lag_ms"`
	SchemaAutoUpdateCompatibilityStrategy SchemaAutoUpdateCompatibilityStrategy `json:"schema_auto_update_compatibility_strategy"`
	SchemaValidationEnforced              bool                                  `json:"schema_validation_enforced"`
}

func NewDefaultPolicies() *Policies {
	return &Policies{
		AuthPolicies:                          *NewAuthPolicies(),
		ReplicationClusters:                   make([]string, 0, 10),
		BacklogQuotaMap:                       make(map[BacklogQuotaType]BacklogQuota),
		TopicDispatchRate:                     make(map[string]DispatchRate),
		SubscriptionDispatchRate:              make(map[string]DispatchRate),
		ReplicatorDispatchRate:                make(map[string]DispatchRate),
		ClusterSubscribeRate:                  make(map[string]SubscribeRate),
		LatencyStatsSampleRate:                make(map[string]int),
		MessageTtlInSeconds:                   0,
		Deleted:                               false,
		EncryptionRequired:                    false,
		SubscriptionAuthMode:                  None,
		MaxProducersPerTopic:                  0,
		MaxConsumersPerSubscription:           0,
		MaxConsumersPerTopic:                  0,
		CompactionThreshold:                   0,
		OffloadThreshold:                      -1,
		SchemaAutoUpdateCompatibilityStrategy: Full,
		SchemaValidationEnforced:              false,
	}
}

type SubscriptionAuthMode string

const (
	None   SubscriptionAuthMode = "None"
	Prefix SubscriptionAuthMode = "Prefix"
)
