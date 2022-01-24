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

import (
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common"
)

const (
	FirstBoundary string = "0x00000000"
	LastBoundary  string = "0xffffffff"
)

type Policies struct {
	Bundles                     *BundlesData                      `json:"bundles"`
	Persistence                 *PersistencePolicies              `json:"persistence"`
	RetentionPolicies           *RetentionPolicies                `json:"retention_policies"`
	SchemaValidationEnforced    bool                              `json:"schema_validation_enforced"`
	DeduplicationEnabled        *bool                             `json:"deduplicationEnabled"`
	Deleted                     bool                              `json:"deleted"`
	EncryptionRequired          bool                              `json:"encryption_required"`
	MessageTTLInSeconds         *int                              `json:"message_ttl_in_seconds"`
	MaxProducersPerTopic        *int                              `json:"max_producers_per_topic"`
	MaxConsumersPerTopic        *int                              `json:"max_consumers_per_topic"`
	MaxConsumersPerSubscription *int                              `json:"max_consumers_per_subscription"`
	CompactionThreshold         *int64                            `json:"compaction_threshold"`
	OffloadThreshold            int64                             `json:"offload_threshold"`
	OffloadDeletionLagMs        *int64                            `json:"offload_deletion_lag_ms"`
	AntiAffinityGroup           string                            `json:"antiAffinityGroup"`
	ReplicationClusters         []string                          `json:"replication_clusters"`
	LatencyStatsSampleRate      map[string]int                    `json:"latency_stats_sample_rate"`
	BacklogQuotaMap             map[BacklogQuotaType]BacklogQuota `json:"backlog_quota_map"`
	TopicDispatchRate           map[string]DispatchRate           `json:"topicDispatchRate"`
	SubscriptionDispatchRate    map[string]DispatchRate           `json:"subscriptionDispatchRate"`
	ReplicatorDispatchRate      map[string]DispatchRate           `json:"replicatorDispatchRate"`
	PublishMaxMessageRate       map[string]PublishRate            `json:"publishMaxMessageRate"`
	ClusterSubscribeRate        map[string]SubscribeRate          `json:"clusterSubscribeRate"`
	TopicAutoCreationConfig     *TopicAutoCreationConfig          `json:"autoTopicCreationOverride"`
	SchemaCompatibilityStrategy SchemaCompatibilityStrategy       `json:"schema_auto_update_compatibility_strategy"`
	AuthPolicies                common.AuthPolicies               `json:"auth_policies"`
	SubscriptionAuthMode        SubscriptionAuthMode              `json:"subscription_auth_mode"`
	IsAllowAutoUpdateSchema     *bool                             `json:"is_allow_auto_update_schema"`
}

func NewDefaultPolicies() *Policies {
	return &Policies{
		AuthPolicies:                *common.NewAuthPolicies(),
		ReplicationClusters:         make([]string, 0, 10),
		BacklogQuotaMap:             make(map[BacklogQuotaType]BacklogQuota),
		TopicDispatchRate:           make(map[string]DispatchRate),
		SubscriptionDispatchRate:    make(map[string]DispatchRate),
		ReplicatorDispatchRate:      make(map[string]DispatchRate),
		PublishMaxMessageRate:       make(map[string]PublishRate),
		ClusterSubscribeRate:        make(map[string]SubscribeRate),
		LatencyStatsSampleRate:      make(map[string]int),
		MessageTTLInSeconds:         nil,
		Deleted:                     false,
		EncryptionRequired:          false,
		SubscriptionAuthMode:        None,
		MaxProducersPerTopic:        nil,
		MaxConsumersPerSubscription: nil,
		MaxConsumersPerTopic:        nil,
		CompactionThreshold:         nil,
		OffloadThreshold:            -1,
		SchemaCompatibilityStrategy: Full,
		SchemaValidationEnforced:    false,
	}
}
