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

package admin

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

// TopicPolicies is the scoped admin interface for topic-level policies.
type TopicPolicies interface {
	GetMessageTTL(context.Context, utils.TopicName, bool) (*int, error)
	SetMessageTTL(context.Context, utils.TopicName, int) error
	RemoveMessageTTL(context.Context, utils.TopicName) error

	GetMaxProducers(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxProducers(context.Context, utils.TopicName, int) error
	RemoveMaxProducers(context.Context, utils.TopicName) error

	GetMaxConsumers(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxConsumers(context.Context, utils.TopicName, int) error
	RemoveMaxConsumers(context.Context, utils.TopicName) error

	GetMaxUnackMessagesPerConsumer(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxUnackMessagesPerConsumer(context.Context, utils.TopicName, int) error
	RemoveMaxUnackMessagesPerConsumer(context.Context, utils.TopicName) error

	GetMaxUnackMessagesPerSubscription(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxUnackMessagesPerSubscription(context.Context, utils.TopicName, int) error
	RemoveMaxUnackMessagesPerSubscription(context.Context, utils.TopicName) error

	GetPersistence(context.Context, utils.TopicName, bool) (*utils.PersistenceData, error)
	SetPersistence(context.Context, utils.TopicName, utils.PersistenceData) error
	RemovePersistence(context.Context, utils.TopicName) error

	GetDelayedDelivery(context.Context, utils.TopicName, bool) (*utils.DelayedDeliveryData, error)
	SetDelayedDelivery(context.Context, utils.TopicName, utils.DelayedDeliveryData) error
	RemoveDelayedDelivery(context.Context, utils.TopicName) error

	GetDispatchRate(context.Context, utils.TopicName, bool) (*utils.DispatchRateData, error)
	SetDispatchRate(context.Context, utils.TopicName, utils.DispatchRateData) error
	RemoveDispatchRate(context.Context, utils.TopicName) error

	GetPublishRate(context.Context, utils.TopicName, bool) (*utils.PublishRateData, error)
	SetPublishRate(context.Context, utils.TopicName, utils.PublishRateData) error
	RemovePublishRate(context.Context, utils.TopicName) error

	GetDeduplicationStatus(context.Context, utils.TopicName, bool) (*bool, error)
	SetDeduplicationStatus(context.Context, utils.TopicName, bool) error
	RemoveDeduplicationStatus(context.Context, utils.TopicName) error

	GetRetention(context.Context, utils.TopicName, bool) (*utils.RetentionPolicies, error)
	SetRetention(context.Context, utils.TopicName, utils.RetentionPolicies) error
	RemoveRetention(context.Context, utils.TopicName) error

	GetCompactionThreshold(context.Context, utils.TopicName, bool) (*int64, error)
	SetCompactionThreshold(context.Context, utils.TopicName, int64) error
	RemoveCompactionThreshold(context.Context, utils.TopicName) error

	GetBacklogQuotaMap(context.Context, utils.TopicName, bool) (map[utils.BacklogQuotaType]utils.BacklogQuota, error)
	SetBacklogQuota(context.Context, utils.TopicName, utils.BacklogQuota, utils.BacklogQuotaType) error
	RemoveBacklogQuota(context.Context, utils.TopicName, utils.BacklogQuotaType) error

	GetInactiveTopicPolicies(context.Context, utils.TopicName, bool) (*utils.InactiveTopicPolicies, error)
	SetInactiveTopicPolicies(context.Context, utils.TopicName, utils.InactiveTopicPolicies) error
	RemoveInactiveTopicPolicies(context.Context, utils.TopicName) error

	GetReplicationClusters(context.Context, utils.TopicName, bool) ([]string, error)
	SetReplicationClusters(context.Context, utils.TopicName, []string) error
	RemoveReplicationClusters(context.Context, utils.TopicName) error

	GetSubscribeRate(context.Context, utils.TopicName, bool) (*utils.SubscribeRate, error)
	SetSubscribeRate(context.Context, utils.TopicName, utils.SubscribeRate) error
	RemoveSubscribeRate(context.Context, utils.TopicName) error

	GetSubscriptionDispatchRate(context.Context, utils.TopicName, bool) (*utils.DispatchRateData, error)
	SetSubscriptionDispatchRate(context.Context, utils.TopicName, utils.DispatchRateData) error
	RemoveSubscriptionDispatchRate(context.Context, utils.TopicName) error

	GetMaxConsumersPerSubscription(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxConsumersPerSubscription(context.Context, utils.TopicName, int) error
	RemoveMaxConsumersPerSubscription(context.Context, utils.TopicName) error

	GetMaxMessageSize(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxMessageSize(context.Context, utils.TopicName, int) error
	RemoveMaxMessageSize(context.Context, utils.TopicName) error

	GetMaxSubscriptionsPerTopic(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxSubscriptionsPerTopic(context.Context, utils.TopicName, int) error
	RemoveMaxSubscriptionsPerTopic(context.Context, utils.TopicName) error

	GetSchemaValidationEnforced(context.Context, utils.TopicName, bool) (*bool, error)
	SetSchemaValidationEnforced(context.Context, utils.TopicName, bool) error
	RemoveSchemaValidationEnforced(context.Context, utils.TopicName) error

	GetDeduplicationSnapshotInterval(context.Context, utils.TopicName, bool) (*int, error)
	SetDeduplicationSnapshotInterval(context.Context, utils.TopicName, int) error
	RemoveDeduplicationSnapshotInterval(context.Context, utils.TopicName) error

	GetReplicatorDispatchRate(context.Context, utils.TopicName, bool) (*utils.DispatchRateData, error)
	SetReplicatorDispatchRate(context.Context, utils.TopicName, utils.DispatchRateData) error
	RemoveReplicatorDispatchRate(context.Context, utils.TopicName) error

	GetOffloadPolicies(context.Context, utils.TopicName, bool) (*utils.OffloadPolicies, error)
	SetOffloadPolicies(context.Context, utils.TopicName, utils.OffloadPolicies) error
	RemoveOffloadPolicies(context.Context, utils.TopicName) error

	GetAutoSubscriptionCreation(context.Context, utils.TopicName, bool) (*utils.AutoSubscriptionCreationOverride, error)
	SetAutoSubscriptionCreation(context.Context, utils.TopicName, utils.AutoSubscriptionCreationOverride) error
	RemoveAutoSubscriptionCreation(context.Context, utils.TopicName) error

	GetSchemaCompatibilityStrategy(context.Context, utils.TopicName, bool) (*utils.SchemaCompatibilityStrategy, error)
	SetSchemaCompatibilityStrategy(context.Context, utils.TopicName, utils.SchemaCompatibilityStrategy) error
	RemoveSchemaCompatibilityStrategy(context.Context, utils.TopicName) error
}

// TopicPoliciesProvider provides access to scoped topic policy clients.
type TopicPoliciesProvider interface {
	TopicPolicies(isGlobal bool) TopicPolicies
}

// TopicPoliciesOf returns a scoped topic policy client when the provided admin client supports it.
func TopicPoliciesOf(client Client, isGlobal bool) (TopicPolicies, error) {
	provider, ok := client.(TopicPoliciesProvider)
	if !ok {
		return nil, fmt.Errorf("admin client does not implement TopicPoliciesProvider")
	}
	return provider.TopicPolicies(isGlobal), nil
}

type topicPolicies struct {
	pulsar   *pulsarClient
	basePath string
	isGlobal bool
}

var _ TopicPolicies = &topicPolicies{}
var _ TopicPoliciesProvider = &pulsarClient{}

func (c *pulsarClient) TopicPolicies(isGlobal bool) TopicPolicies {
	return &topicPolicies{
		pulsar:   c,
		basePath: "",
		isGlobal: isGlobal,
	}
}

func (t *topicPolicies) topicEndpoint(topic utils.TopicName, parts ...string) string {
	allParts := make([]string, 0, len(parts)+1)
	allParts = append(allParts, topic.GetRestPath())
	allParts = append(allParts, parts...)
	return t.pulsar.endpoint(t.basePath, allParts...)
}

func (t *topicPolicies) scopedQueryParams(params map[string]string) map[string]string {
	if !t.isGlobal && len(params) == 0 {
		return nil
	}

	out := make(map[string]string, len(params)+1)
	for key, value := range params {
		out[key] = value
	}
	if t.isGlobal {
		out["isGlobal"] = "true"
	}
	return out
}

func (t *topicPolicies) appliedQueryParams(applied bool) map[string]string {
	return t.scopedQueryParams(map[string]string{
		"applied": strconv.FormatBool(applied),
	})
}

func (t *topicPolicies) readPolicyBodyWithContext(
	ctx context.Context,
	endpoint string,
	applied bool,
) ([]byte, error) {
	return t.pulsar.Client.GetWithQueryParamsWithContext(ctx, endpoint, nil, t.appliedQueryParams(applied), false)
}

func (t *topicPolicies) scopedPostWithContext(
	ctx context.Context,
	endpoint string,
	body interface{},
	params map[string]string,
) error {
	return t.pulsar.Client.PostWithQueryParamsWithContext(ctx, endpoint, body, t.scopedQueryParams(params))
}

func (t *topicPolicies) scopedPutWithContext(
	ctx context.Context,
	endpoint string,
	body interface{},
	params map[string]string,
) error {
	return t.pulsar.Client.PutWithQueryParamsWithContext(ctx, endpoint, body, nil, t.scopedQueryParams(params))
}

func (t *topicPolicies) scopedDeleteWithContext(
	ctx context.Context,
	endpoint string,
	params map[string]string,
) error {
	return t.pulsar.Client.DeleteWithQueryParamsWithContext(ctx, endpoint, t.scopedQueryParams(params))
}

func decodeOptionalSchemaCompatibilityStrategy(body []byte) (*utils.SchemaCompatibilityStrategy, error) {
	if isUnsetPolicyBody(body) {
		return nil, nil
	}

	raw := strings.TrimSpace(string(body))
	raw = strings.Trim(raw, "\"")
	if raw == "" {
		return nil, nil
	}

	strategy, err := utils.ParseSchemaCompatibilityStrategy(raw)
	if err != nil {
		return nil, err
	}
	return &strategy, nil
}

func (t *topicPolicies) GetMessageTTL(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*int, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "messageTTL"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[int](body)
}

func (t *topicPolicies) SetMessageTTL(ctx context.Context, topic utils.TopicName, messageTTL int) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "messageTTL"), nil, map[string]string{
		"messageTTL": strconv.Itoa(messageTTL),
	})
}

func (t *topicPolicies) RemoveMessageTTL(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "messageTTL"), map[string]string{
		"messageTTL": strconv.Itoa(0),
	})
}

func (t *topicPolicies) GetMaxProducers(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*int, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "maxProducers"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[int](body)
}

func (t *topicPolicies) SetMaxProducers(ctx context.Context, topic utils.TopicName, maxProducers int) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxProducers"), &maxProducers, nil)
}

func (t *topicPolicies) RemoveMaxProducers(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxProducers"), nil)
}

func (t *topicPolicies) GetMaxConsumers(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*int, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "maxConsumers"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[int](body)
}

func (t *topicPolicies) SetMaxConsumers(ctx context.Context, topic utils.TopicName, maxConsumers int) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxConsumers"), &maxConsumers, nil)
}

func (t *topicPolicies) RemoveMaxConsumers(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxConsumers"), nil)
}

func (t *topicPolicies) GetMaxUnackMessagesPerConsumer(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*int, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "maxUnackedMessagesOnConsumer"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[int](body)
}

func (t *topicPolicies) SetMaxUnackMessagesPerConsumer(
	ctx context.Context,
	topic utils.TopicName,
	maxUnackedNum int,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxUnackedMessagesOnConsumer"), &maxUnackedNum, nil)
}

func (t *topicPolicies) RemoveMaxUnackMessagesPerConsumer(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxUnackedMessagesOnConsumer"), nil)
}

func (t *topicPolicies) GetMaxUnackMessagesPerSubscription(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*int, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "maxUnackedMessagesOnSubscription"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[int](body)
}

func (t *topicPolicies) SetMaxUnackMessagesPerSubscription(
	ctx context.Context,
	topic utils.TopicName,
	maxUnackedNum int,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxUnackedMessagesOnSubscription"), &maxUnackedNum, nil)
}

func (t *topicPolicies) RemoveMaxUnackMessagesPerSubscription(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxUnackedMessagesOnSubscription"), nil)
}

func (t *topicPolicies) GetPersistence(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*utils.PersistenceData, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "persistence"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[utils.PersistenceData](body)
}

func (t *topicPolicies) SetPersistence(
	ctx context.Context,
	topic utils.TopicName,
	persistenceData utils.PersistenceData,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "persistence"), &persistenceData, nil)
}

func (t *topicPolicies) RemovePersistence(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "persistence"), nil)
}

func (t *topicPolicies) GetDelayedDelivery(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*utils.DelayedDeliveryData, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "delayedDelivery"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[utils.DelayedDeliveryData](body)
}

func (t *topicPolicies) SetDelayedDelivery(
	ctx context.Context,
	topic utils.TopicName,
	delayedDeliveryData utils.DelayedDeliveryData,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "delayedDelivery"), &delayedDeliveryData, nil)
}

func (t *topicPolicies) RemoveDelayedDelivery(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "delayedDelivery"), nil)
}

func (t *topicPolicies) GetDispatchRate(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*utils.DispatchRateData, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "dispatchRate"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[utils.DispatchRateData](body)
}

func (t *topicPolicies) SetDispatchRate(
	ctx context.Context,
	topic utils.TopicName,
	dispatchRateData utils.DispatchRateData,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "dispatchRate"), &dispatchRateData, nil)
}

func (t *topicPolicies) RemoveDispatchRate(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "dispatchRate"), nil)
}

func (t *topicPolicies) GetPublishRate(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*utils.PublishRateData, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "publishRate"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[utils.PublishRateData](body)
}

func (t *topicPolicies) SetPublishRate(
	ctx context.Context,
	topic utils.TopicName,
	publishRateData utils.PublishRateData,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "publishRate"), &publishRateData, nil)
}

func (t *topicPolicies) RemovePublishRate(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "publishRate"), nil)
}

func (t *topicPolicies) GetDeduplicationStatus(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*bool, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "deduplicationEnabled"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[bool](body)
}

func (t *topicPolicies) SetDeduplicationStatus(
	ctx context.Context,
	topic utils.TopicName,
	enabled bool,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "deduplicationEnabled"), enabled, nil)
}

func (t *topicPolicies) RemoveDeduplicationStatus(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "deduplicationEnabled"), nil)
}

func (t *topicPolicies) GetRetention(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*utils.RetentionPolicies, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "retention"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[utils.RetentionPolicies](body)
}

func (t *topicPolicies) SetRetention(
	ctx context.Context,
	topic utils.TopicName,
	data utils.RetentionPolicies,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "retention"), data, nil)
}

func (t *topicPolicies) RemoveRetention(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "retention"), nil)
}

func (t *topicPolicies) GetCompactionThreshold(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*int64, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "compactionThreshold"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[int64](body)
}

func (t *topicPolicies) SetCompactionThreshold(
	ctx context.Context,
	topic utils.TopicName,
	threshold int64,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "compactionThreshold"), threshold, nil)
}

func (t *topicPolicies) RemoveCompactionThreshold(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "compactionThreshold"), nil)
}

func (t *topicPolicies) GetBacklogQuotaMap(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (map[utils.BacklogQuotaType]utils.BacklogQuota, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "backlogQuotaMap"), applied)
	if err != nil {
		return nil, err
	}

	decoded, err := decodeOptionalJSON[map[utils.BacklogQuotaType]utils.BacklogQuota](body)
	if err != nil || decoded == nil {
		return nil, err
	}
	return *decoded, nil
}

func (t *topicPolicies) SetBacklogQuota(
	ctx context.Context,
	topic utils.TopicName,
	backlogQuota utils.BacklogQuota,
	backlogQuotaType utils.BacklogQuotaType,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "backlogQuota"), &backlogQuota, map[string]string{
		"backlogQuotaType": string(backlogQuotaType),
	})
}

func (t *topicPolicies) RemoveBacklogQuota(
	ctx context.Context,
	topic utils.TopicName,
	backlogQuotaType utils.BacklogQuotaType,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "backlogQuota"), map[string]string{
		"backlogQuotaType": string(backlogQuotaType),
	})
}

func (t *topicPolicies) GetInactiveTopicPolicies(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*utils.InactiveTopicPolicies, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "inactiveTopicPolicies"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[utils.InactiveTopicPolicies](body)
}

func (t *topicPolicies) SetInactiveTopicPolicies(
	ctx context.Context,
	topic utils.TopicName,
	data utils.InactiveTopicPolicies,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "inactiveTopicPolicies"), data, nil)
}

func (t *topicPolicies) RemoveInactiveTopicPolicies(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "inactiveTopicPolicies"), nil)
}

func (t *topicPolicies) GetReplicationClusters(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) ([]string, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "replication"), applied)
	if err != nil {
		return nil, err
	}

	decoded, err := decodeOptionalJSON[[]string](body)
	if err != nil || decoded == nil {
		return nil, err
	}
	return *decoded, nil
}

func (t *topicPolicies) SetReplicationClusters(
	ctx context.Context,
	topic utils.TopicName,
	data []string,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "replication"), data, nil)
}

func (t *topicPolicies) RemoveReplicationClusters(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "replication"), nil)
}

func (t *topicPolicies) GetSubscribeRate(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*utils.SubscribeRate, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "subscribeRate"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[utils.SubscribeRate](body)
}

func (t *topicPolicies) SetSubscribeRate(
	ctx context.Context,
	topic utils.TopicName,
	subscribeRate utils.SubscribeRate,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "subscribeRate"), &subscribeRate, nil)
}

func (t *topicPolicies) RemoveSubscribeRate(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "subscribeRate"), nil)
}

func (t *topicPolicies) GetSubscriptionDispatchRate(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*utils.DispatchRateData, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "subscriptionDispatchRate"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[utils.DispatchRateData](body)
}

func (t *topicPolicies) SetSubscriptionDispatchRate(
	ctx context.Context,
	topic utils.TopicName,
	dispatchRate utils.DispatchRateData,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "subscriptionDispatchRate"), &dispatchRate, nil)
}

func (t *topicPolicies) RemoveSubscriptionDispatchRate(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "subscriptionDispatchRate"), nil)
}

func (t *topicPolicies) GetMaxConsumersPerSubscription(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*int, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "maxConsumersPerSubscription"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[int](body)
}

func (t *topicPolicies) SetMaxConsumersPerSubscription(
	ctx context.Context,
	topic utils.TopicName,
	maxConsumers int,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxConsumersPerSubscription"), &maxConsumers, nil)
}

func (t *topicPolicies) RemoveMaxConsumersPerSubscription(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxConsumersPerSubscription"), nil)
}

func (t *topicPolicies) GetMaxMessageSize(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*int, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "maxMessageSize"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[int](body)
}

func (t *topicPolicies) SetMaxMessageSize(
	ctx context.Context,
	topic utils.TopicName,
	maxMessageSize int,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxMessageSize"), &maxMessageSize, nil)
}

func (t *topicPolicies) RemoveMaxMessageSize(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxMessageSize"), nil)
}

func (t *topicPolicies) GetMaxSubscriptionsPerTopic(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*int, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "maxSubscriptionsPerTopic"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[int](body)
}

func (t *topicPolicies) SetMaxSubscriptionsPerTopic(
	ctx context.Context,
	topic utils.TopicName,
	maxSubscriptions int,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxSubscriptionsPerTopic"), &maxSubscriptions, nil)
}

func (t *topicPolicies) RemoveMaxSubscriptionsPerTopic(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxSubscriptionsPerTopic"), nil)
}

func (t *topicPolicies) GetSchemaValidationEnforced(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*bool, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "schemaValidationEnforced"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[bool](body)
}

func (t *topicPolicies) SetSchemaValidationEnforced(
	ctx context.Context,
	topic utils.TopicName,
	enabled bool,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "schemaValidationEnforced"), enabled, nil)
}

func (t *topicPolicies) RemoveSchemaValidationEnforced(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "schemaValidationEnforced"), nil)
}

func (t *topicPolicies) GetDeduplicationSnapshotInterval(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*int, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "deduplicationSnapshotInterval"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[int](body)
}

func (t *topicPolicies) SetDeduplicationSnapshotInterval(
	ctx context.Context,
	topic utils.TopicName,
	interval int,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "deduplicationSnapshotInterval"), &interval, nil)
}

func (t *topicPolicies) RemoveDeduplicationSnapshotInterval(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "deduplicationSnapshotInterval"), nil)
}

func (t *topicPolicies) GetReplicatorDispatchRate(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*utils.DispatchRateData, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "replicatorDispatchRate"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[utils.DispatchRateData](body)
}

func (t *topicPolicies) SetReplicatorDispatchRate(
	ctx context.Context,
	topic utils.TopicName,
	dispatchRate utils.DispatchRateData,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "replicatorDispatchRate"), &dispatchRate, nil)
}

func (t *topicPolicies) RemoveReplicatorDispatchRate(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "replicatorDispatchRate"), nil)
}

func (t *topicPolicies) GetOffloadPolicies(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*utils.OffloadPolicies, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "offloadPolicies"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[utils.OffloadPolicies](body)
}

func (t *topicPolicies) SetOffloadPolicies(
	ctx context.Context,
	topic utils.TopicName,
	offloadPolicies utils.OffloadPolicies,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "offloadPolicies"), &offloadPolicies, nil)
}

func (t *topicPolicies) RemoveOffloadPolicies(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "offloadPolicies"), nil)
}

func (t *topicPolicies) GetAutoSubscriptionCreation(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*utils.AutoSubscriptionCreationOverride, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "autoSubscriptionCreation"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[utils.AutoSubscriptionCreationOverride](body)
}

func (t *topicPolicies) SetAutoSubscriptionCreation(
	ctx context.Context,
	topic utils.TopicName,
	autoSubCreation utils.AutoSubscriptionCreationOverride,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "autoSubscriptionCreation"), &autoSubCreation, nil)
}

func (t *topicPolicies) RemoveAutoSubscriptionCreation(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "autoSubscriptionCreation"), nil)
}

func (t *topicPolicies) GetSchemaCompatibilityStrategy(
	ctx context.Context,
	topic utils.TopicName,
	applied bool,
) (*utils.SchemaCompatibilityStrategy, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "schemaCompatibilityStrategy"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalSchemaCompatibilityStrategy(body)
}

func (t *topicPolicies) SetSchemaCompatibilityStrategy(
	ctx context.Context,
	topic utils.TopicName,
	strategy utils.SchemaCompatibilityStrategy,
) error {
	return t.scopedPutWithContext(ctx, t.topicEndpoint(topic, "schemaCompatibilityStrategy"), strategy, nil)
}

func (t *topicPolicies) RemoveSchemaCompatibilityStrategy(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "schemaCompatibilityStrategy"), nil)
}
