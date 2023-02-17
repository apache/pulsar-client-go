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

const (
	// UrlPath for the Admin API
	UrlPath               = "/admin/v2"
	UrlBookiesAll         = UrlPath + "/bookies/all"
	UrlBookiesRacksInfo   = UrlPath + "/bookies/racks-info"
	UrlBookiesRacksFormat = UrlPath + "/bookies/racks-info/%s"
	UrlClusters           = UrlPath + "/clusters"
	UrlTenants            = UrlPath + "/tenants"
	UrlNamespacesFormat   = UrlPath + "/namespaces/%s/%s"
)

// namespace
const (
	UrlNamespaceRetentionFormat                         = UrlPath + "/namespaces/%s/%s/retention"
	UrlNamespaceGetBacklogQuotaMapFormat                = UrlPath + "/namespaces/%s/%s/backlogQuotaMap"
	UrlNamespaceOperateBacklogQuotaFormat               = UrlPath + "/namespaces/%s/%s/backlogQuota"
	UrlNamespaceClearAllTopicsBacklogFormat             = UrlPath + "/namespaces/%s/%s/clearBacklog"
	UrlNamespaceClearSubscriptionBacklogFormat          = UrlPath + "/namespaces/%s/%s/clearBacklog/%s"
	UrlNamespaceClearAllTopicsBacklogForBundleFormat    = UrlPath + "/namespaces/%s/%s/%s/clearBacklog"
	UrlNamespaceClearSubscriptionBacklogForBundleFormat = UrlPath + "/namespaces/%s/%s/%s/clearBacklog/%s"
	UrlNamespaceCompactionThresholdFormat               = UrlPath + "/namespaces/%s/%s/compactionThreshold"
	UrlNamespaceMessageTTLFormat                        = UrlPath + "/namespaces/%s/%s/messageTTL"
)

// persistent
const (
	UrlPersistentNamespaceFormat                            = UrlPath + "/persistent/%s/%s"
	UrlPersistentTopicFormat                                = UrlPath + "/persistent/%s/%s/%s"
	UrlPersistentPartitionedNamespaceFormat                 = UrlPath + "/persistent/%s/%s/partitioned"
	UrlPersistentPartitionedTopicFormat                     = UrlPath + "/persistent/%s/%s/%s/partitions"
	UrlPersistentPartitionedRetentionFormat                 = UrlPath + "/persistent/%s/%s/%s/retention"
	UrlPersistentTopicGetBacklogQuotaMapFormat              = UrlPath + "/persistent/%s/%s/%s/backlogQuotaMap"
	UrlPersistentTopicOperateBacklogQuotaFormat             = UrlPath + "/persistent/%s/%s/%s/backlogQuota"
	UrlPersistentTopicEstimatedOfflineBacklogFormat         = UrlPath + "/persistent/%s%s%s/backlog"
	UrlPersistentTopicCalculateBacklogSizeByMessageIDFormat = UrlPath + "/persistent/%s/%s/%s/backlogSize"
	UrlPersistentTopicCompactionThresholdFormat             = UrlPath + "/persistent/%s/%s/%s/compactionThreshold"
	UrlPersistentTopicCompactionFormat                      = UrlPath + "/persistent/%s/%s/%s/compaction"
	UrlPersistentTopicMessageTTLFormat                      = UrlPath + "/persistent/%s/%s/%s/messageTTL"
)

// non-persistent
const (
	UrlNonPersistentNamespaceFormat                            = UrlPath + "/non-persistent/%s/%s"
	UrlNonPersistentTopicFormat                                = UrlPath + "/non-persistent/%s/%s/%s"
	UrlNonPersistentPartitionedTopicFormat                     = UrlPath + "/non-persistent/%s/%s/%s/partitions"
	UrlNonPersistentPartitionedNamespaceFormat                 = UrlPath + "/non-persistent/%s/%s/partitioned"
	UrlNonPersistentPartitionedRetentionFormat                 = UrlPath + "/non-persistent/%s/%s/%s/retention"
	UrlNonPersistentTopicGetBacklogQuotaMapFormat              = UrlPath + "/non-persistent/%s/%s/%s/backlogQuotaMap"
	UrlNonPersistentTopicOperateBacklogQuotaFormat             = UrlPath + "/non-persistent/%s/%s/%s/backlogQuota"
	UrlNonPersistentTopicEstimatedOfflineBacklogFormat         = UrlPath + "/non-persistent/%s%s%s/backlog"
	UrlNonPersistentTopicCalculateBacklogSizeByMessageIDFormat = UrlPath + "/non-persistent/%s/%s/%s/backlogSize"
	UrlNonPersistentTopicCompactionThresholdFormat             = UrlPath + "/non-persistent/%s/%s/%s/compactionThreshold"
	UrlNonPersistentTopicCompactionFormat                      = UrlPath + "/non-persistent/%s/%s/%s/compaction"
	UrlNonPersistentTopicMessageTTLFormat                      = UrlPath + "/non-persistent/%s/%s/%s/messageTTL"
)
