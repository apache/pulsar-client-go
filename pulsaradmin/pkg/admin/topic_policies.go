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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

// TopicPolicies is the scoped admin interface for topic-level policies.
type TopicPolicies interface {
	GetMessageTTL(utils.TopicName, bool) (*int, error)
	GetMessageTTLWithContext(context.Context, utils.TopicName, bool) (*int, error)
	SetMessageTTL(utils.TopicName, int) error
	SetMessageTTLWithContext(context.Context, utils.TopicName, int) error
	RemoveMessageTTL(utils.TopicName) error
	RemoveMessageTTLWithContext(context.Context, utils.TopicName) error

	GetMaxProducers(utils.TopicName, bool) (*int, error)
	GetMaxProducersWithContext(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxProducers(utils.TopicName, int) error
	SetMaxProducersWithContext(context.Context, utils.TopicName, int) error
	RemoveMaxProducers(utils.TopicName) error
	RemoveMaxProducersWithContext(context.Context, utils.TopicName) error

	GetMaxConsumers(utils.TopicName, bool) (*int, error)
	GetMaxConsumersWithContext(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxConsumers(utils.TopicName, int) error
	SetMaxConsumersWithContext(context.Context, utils.TopicName, int) error
	RemoveMaxConsumers(utils.TopicName) error
	RemoveMaxConsumersWithContext(context.Context, utils.TopicName) error

	GetMaxUnackMessagesPerConsumer(utils.TopicName, bool) (*int, error)
	GetMaxUnackMessagesPerConsumerWithContext(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxUnackMessagesPerConsumer(utils.TopicName, int) error
	SetMaxUnackMessagesPerConsumerWithContext(context.Context, utils.TopicName, int) error
	RemoveMaxUnackMessagesPerConsumer(utils.TopicName) error
	RemoveMaxUnackMessagesPerConsumerWithContext(context.Context, utils.TopicName) error

	GetMaxUnackMessagesPerSubscription(utils.TopicName, bool) (*int, error)
	GetMaxUnackMessagesPerSubscriptionWithContext(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxUnackMessagesPerSubscription(utils.TopicName, int) error
	SetMaxUnackMessagesPerSubscriptionWithContext(context.Context, utils.TopicName, int) error
	RemoveMaxUnackMessagesPerSubscription(utils.TopicName) error
	RemoveMaxUnackMessagesPerSubscriptionWithContext(context.Context, utils.TopicName) error

	GetPersistence(utils.TopicName, bool) (*utils.PersistenceData, error)
	GetPersistenceWithContext(context.Context, utils.TopicName, bool) (*utils.PersistenceData, error)
	SetPersistence(utils.TopicName, utils.PersistenceData) error
	SetPersistenceWithContext(context.Context, utils.TopicName, utils.PersistenceData) error
	RemovePersistence(utils.TopicName) error
	RemovePersistenceWithContext(context.Context, utils.TopicName) error

	GetDelayedDelivery(utils.TopicName, bool) (*utils.DelayedDeliveryData, error)
	GetDelayedDeliveryWithContext(context.Context, utils.TopicName, bool) (*utils.DelayedDeliveryData, error)
	SetDelayedDelivery(utils.TopicName, utils.DelayedDeliveryData) error
	SetDelayedDeliveryWithContext(context.Context, utils.TopicName, utils.DelayedDeliveryData) error
	RemoveDelayedDelivery(utils.TopicName) error
	RemoveDelayedDeliveryWithContext(context.Context, utils.TopicName) error

	GetDispatchRate(utils.TopicName, bool) (*utils.DispatchRateData, error)
	GetDispatchRateWithContext(context.Context, utils.TopicName, bool) (*utils.DispatchRateData, error)
	SetDispatchRate(utils.TopicName, utils.DispatchRateData) error
	SetDispatchRateWithContext(context.Context, utils.TopicName, utils.DispatchRateData) error
	RemoveDispatchRate(utils.TopicName) error
	RemoveDispatchRateWithContext(context.Context, utils.TopicName) error

	GetPublishRate(utils.TopicName, bool) (*utils.PublishRateData, error)
	GetPublishRateWithContext(context.Context, utils.TopicName, bool) (*utils.PublishRateData, error)
	SetPublishRate(utils.TopicName, utils.PublishRateData) error
	SetPublishRateWithContext(context.Context, utils.TopicName, utils.PublishRateData) error
	RemovePublishRate(utils.TopicName) error
	RemovePublishRateWithContext(context.Context, utils.TopicName) error

	GetDeduplicationStatus(utils.TopicName, bool) (*bool, error)
	GetDeduplicationStatusWithContext(context.Context, utils.TopicName, bool) (*bool, error)
	SetDeduplicationStatus(utils.TopicName, bool) error
	SetDeduplicationStatusWithContext(context.Context, utils.TopicName, bool) error
	RemoveDeduplicationStatus(utils.TopicName) error
	RemoveDeduplicationStatusWithContext(context.Context, utils.TopicName) error

	GetRetention(utils.TopicName, bool) (*utils.RetentionPolicies, error)
	GetRetentionWithContext(context.Context, utils.TopicName, bool) (*utils.RetentionPolicies, error)
	SetRetention(utils.TopicName, utils.RetentionPolicies) error
	SetRetentionWithContext(context.Context, utils.TopicName, utils.RetentionPolicies) error
	RemoveRetention(utils.TopicName) error
	RemoveRetentionWithContext(context.Context, utils.TopicName) error

	GetCompactionThreshold(utils.TopicName, bool) (*int64, error)
	GetCompactionThresholdWithContext(context.Context, utils.TopicName, bool) (*int64, error)
	SetCompactionThreshold(utils.TopicName, int64) error
	SetCompactionThresholdWithContext(context.Context, utils.TopicName, int64) error
	RemoveCompactionThreshold(utils.TopicName) error
	RemoveCompactionThresholdWithContext(context.Context, utils.TopicName) error

	GetBacklogQuotaMap(utils.TopicName, bool) (map[utils.BacklogQuotaType]utils.BacklogQuota, error)
	GetBacklogQuotaMapWithContext(context.Context, utils.TopicName, bool) (map[utils.BacklogQuotaType]utils.BacklogQuota, error)
	SetBacklogQuota(utils.TopicName, utils.BacklogQuota, utils.BacklogQuotaType) error
	SetBacklogQuotaWithContext(context.Context, utils.TopicName, utils.BacklogQuota, utils.BacklogQuotaType) error
	RemoveBacklogQuota(utils.TopicName, utils.BacklogQuotaType) error
	RemoveBacklogQuotaWithContext(context.Context, utils.TopicName, utils.BacklogQuotaType) error

	GetInactiveTopicPolicies(utils.TopicName, bool) (*utils.InactiveTopicPolicies, error)
	GetInactiveTopicPoliciesWithContext(context.Context, utils.TopicName, bool) (*utils.InactiveTopicPolicies, error)
	SetInactiveTopicPolicies(utils.TopicName, utils.InactiveTopicPolicies) error
	SetInactiveTopicPoliciesWithContext(context.Context, utils.TopicName, utils.InactiveTopicPolicies) error
	RemoveInactiveTopicPolicies(utils.TopicName) error
	RemoveInactiveTopicPoliciesWithContext(context.Context, utils.TopicName) error

	GetReplicationClusters(utils.TopicName, bool) ([]string, error)
	GetReplicationClustersWithContext(context.Context, utils.TopicName, bool) ([]string, error)
	SetReplicationClusters(utils.TopicName, []string) error
	SetReplicationClustersWithContext(context.Context, utils.TopicName, []string) error
	RemoveReplicationClusters(utils.TopicName) error
	RemoveReplicationClustersWithContext(context.Context, utils.TopicName) error

	GetSubscribeRate(utils.TopicName, bool) (*utils.SubscribeRate, error)
	GetSubscribeRateWithContext(context.Context, utils.TopicName, bool) (*utils.SubscribeRate, error)
	SetSubscribeRate(utils.TopicName, utils.SubscribeRate) error
	SetSubscribeRateWithContext(context.Context, utils.TopicName, utils.SubscribeRate) error
	RemoveSubscribeRate(utils.TopicName) error
	RemoveSubscribeRateWithContext(context.Context, utils.TopicName) error

	GetSubscriptionDispatchRate(utils.TopicName, bool) (*utils.DispatchRateData, error)
	GetSubscriptionDispatchRateWithContext(context.Context, utils.TopicName, bool) (*utils.DispatchRateData, error)
	SetSubscriptionDispatchRate(utils.TopicName, utils.DispatchRateData) error
	SetSubscriptionDispatchRateWithContext(context.Context, utils.TopicName, utils.DispatchRateData) error
	RemoveSubscriptionDispatchRate(utils.TopicName) error
	RemoveSubscriptionDispatchRateWithContext(context.Context, utils.TopicName) error

	GetMaxConsumersPerSubscription(utils.TopicName, bool) (*int, error)
	GetMaxConsumersPerSubscriptionWithContext(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxConsumersPerSubscription(utils.TopicName, int) error
	SetMaxConsumersPerSubscriptionWithContext(context.Context, utils.TopicName, int) error
	RemoveMaxConsumersPerSubscription(utils.TopicName) error
	RemoveMaxConsumersPerSubscriptionWithContext(context.Context, utils.TopicName) error

	GetMaxMessageSize(utils.TopicName, bool) (*int, error)
	GetMaxMessageSizeWithContext(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxMessageSize(utils.TopicName, int) error
	SetMaxMessageSizeWithContext(context.Context, utils.TopicName, int) error
	RemoveMaxMessageSize(utils.TopicName) error
	RemoveMaxMessageSizeWithContext(context.Context, utils.TopicName) error

	GetMaxSubscriptionsPerTopic(utils.TopicName, bool) (*int, error)
	GetMaxSubscriptionsPerTopicWithContext(context.Context, utils.TopicName, bool) (*int, error)
	SetMaxSubscriptionsPerTopic(utils.TopicName, int) error
	SetMaxSubscriptionsPerTopicWithContext(context.Context, utils.TopicName, int) error
	RemoveMaxSubscriptionsPerTopic(utils.TopicName) error
	RemoveMaxSubscriptionsPerTopicWithContext(context.Context, utils.TopicName) error

	GetSchemaValidationEnforced(utils.TopicName, bool) (*bool, error)
	GetSchemaValidationEnforcedWithContext(context.Context, utils.TopicName, bool) (*bool, error)
	SetSchemaValidationEnforced(utils.TopicName, bool) error
	SetSchemaValidationEnforcedWithContext(context.Context, utils.TopicName, bool) error
	RemoveSchemaValidationEnforced(utils.TopicName) error
	RemoveSchemaValidationEnforcedWithContext(context.Context, utils.TopicName) error

	GetDeduplicationSnapshotInterval(utils.TopicName, bool) (*int, error)
	GetDeduplicationSnapshotIntervalWithContext(context.Context, utils.TopicName, bool) (*int, error)
	SetDeduplicationSnapshotInterval(utils.TopicName, int) error
	SetDeduplicationSnapshotIntervalWithContext(context.Context, utils.TopicName, int) error
	RemoveDeduplicationSnapshotInterval(utils.TopicName) error
	RemoveDeduplicationSnapshotIntervalWithContext(context.Context, utils.TopicName) error

	GetReplicatorDispatchRate(utils.TopicName, bool) (*utils.DispatchRateData, error)
	GetReplicatorDispatchRateWithContext(context.Context, utils.TopicName, bool) (*utils.DispatchRateData, error)
	SetReplicatorDispatchRate(utils.TopicName, utils.DispatchRateData) error
	SetReplicatorDispatchRateWithContext(context.Context, utils.TopicName, utils.DispatchRateData) error
	RemoveReplicatorDispatchRate(utils.TopicName) error
	RemoveReplicatorDispatchRateWithContext(context.Context, utils.TopicName) error

	GetOffloadPolicies(utils.TopicName, bool) (*utils.OffloadPolicies, error)
	GetOffloadPoliciesWithContext(context.Context, utils.TopicName, bool) (*utils.OffloadPolicies, error)
	SetOffloadPolicies(utils.TopicName, utils.OffloadPolicies) error
	SetOffloadPoliciesWithContext(context.Context, utils.TopicName, utils.OffloadPolicies) error
	RemoveOffloadPolicies(utils.TopicName) error
	RemoveOffloadPoliciesWithContext(context.Context, utils.TopicName) error

	GetAutoSubscriptionCreation(utils.TopicName, bool) (*utils.AutoSubscriptionCreationOverride, error)
	GetAutoSubscriptionCreationWithContext(context.Context, utils.TopicName, bool) (*utils.AutoSubscriptionCreationOverride, error)
	SetAutoSubscriptionCreation(utils.TopicName, utils.AutoSubscriptionCreationOverride) error
	SetAutoSubscriptionCreationWithContext(context.Context, utils.TopicName, utils.AutoSubscriptionCreationOverride) error
	RemoveAutoSubscriptionCreation(utils.TopicName) error
	RemoveAutoSubscriptionCreationWithContext(context.Context, utils.TopicName) error

	GetSchemaCompatibilityStrategy(utils.TopicName, bool) (*utils.SchemaCompatibilityStrategy, error)
	GetSchemaCompatibilityStrategyWithContext(context.Context, utils.TopicName, bool) (*utils.SchemaCompatibilityStrategy, error)
	SetSchemaCompatibilityStrategy(utils.TopicName, utils.SchemaCompatibilityStrategy) error
	SetSchemaCompatibilityStrategyWithContext(context.Context, utils.TopicName, utils.SchemaCompatibilityStrategy) error
	RemoveSchemaCompatibilityStrategy(utils.TopicName) error
	RemoveSchemaCompatibilityStrategyWithContext(context.Context, utils.TopicName) error
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

func decodeOptionalJSON[T any](body []byte) (*T, error) {
	if isUnsetPolicyBody(body) {
		return nil, nil
	}

	var out T
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return &out, nil
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

func isUnsetPolicyBody(body []byte) bool {
	trimmed := strings.TrimSpace(string(body))
	return trimmed == "" || trimmed == "null"
}

func (t *topicPolicies) GetMessageTTL(topic utils.TopicName, applied bool) (*int, error) {
	return t.GetMessageTTLWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetMessageTTLWithContext(ctx context.Context, topic utils.TopicName, applied bool) (*int, error) {
	body, err := t.readPolicyBodyWithContext(ctx, t.topicEndpoint(topic, "messageTTL"), applied)
	if err != nil {
		return nil, err
	}
	return decodeOptionalJSON[int](body)
}

func (t *topicPolicies) SetMessageTTL(topic utils.TopicName, messageTTL int) error {
	return t.SetMessageTTLWithContext(context.Background(), topic, messageTTL)
}

func (t *topicPolicies) SetMessageTTLWithContext(ctx context.Context, topic utils.TopicName, messageTTL int) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "messageTTL"), nil, map[string]string{
		"messageTTL": strconv.Itoa(messageTTL),
	})
}

func (t *topicPolicies) RemoveMessageTTL(topic utils.TopicName) error {
	return t.RemoveMessageTTLWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveMessageTTLWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "messageTTL"), map[string]string{
		"messageTTL": strconv.Itoa(0),
	})
}

func (t *topicPolicies) GetMaxProducers(topic utils.TopicName, applied bool) (*int, error) {
	return t.GetMaxProducersWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetMaxProducersWithContext(
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

func (t *topicPolicies) SetMaxProducers(topic utils.TopicName, maxProducers int) error {
	return t.SetMaxProducersWithContext(context.Background(), topic, maxProducers)
}

func (t *topicPolicies) SetMaxProducersWithContext(ctx context.Context, topic utils.TopicName, maxProducers int) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxProducers"), &maxProducers, nil)
}

func (t *topicPolicies) RemoveMaxProducers(topic utils.TopicName) error {
	return t.RemoveMaxProducersWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveMaxProducersWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxProducers"), nil)
}

func (t *topicPolicies) GetMaxConsumers(topic utils.TopicName, applied bool) (*int, error) {
	return t.GetMaxConsumersWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetMaxConsumersWithContext(
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

func (t *topicPolicies) SetMaxConsumers(topic utils.TopicName, maxConsumers int) error {
	return t.SetMaxConsumersWithContext(context.Background(), topic, maxConsumers)
}

func (t *topicPolicies) SetMaxConsumersWithContext(ctx context.Context, topic utils.TopicName, maxConsumers int) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxConsumers"), &maxConsumers, nil)
}

func (t *topicPolicies) RemoveMaxConsumers(topic utils.TopicName) error {
	return t.RemoveMaxConsumersWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveMaxConsumersWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxConsumers"), nil)
}

func (t *topicPolicies) GetMaxUnackMessagesPerConsumer(topic utils.TopicName, applied bool) (*int, error) {
	return t.GetMaxUnackMessagesPerConsumerWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetMaxUnackMessagesPerConsumerWithContext(
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

func (t *topicPolicies) SetMaxUnackMessagesPerConsumer(topic utils.TopicName, maxUnackedNum int) error {
	return t.SetMaxUnackMessagesPerConsumerWithContext(context.Background(), topic, maxUnackedNum)
}

func (t *topicPolicies) SetMaxUnackMessagesPerConsumerWithContext(
	ctx context.Context,
	topic utils.TopicName,
	maxUnackedNum int,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxUnackedMessagesOnConsumer"), &maxUnackedNum, nil)
}

func (t *topicPolicies) RemoveMaxUnackMessagesPerConsumer(topic utils.TopicName) error {
	return t.RemoveMaxUnackMessagesPerConsumerWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveMaxUnackMessagesPerConsumerWithContext(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxUnackedMessagesOnConsumer"), nil)
}

func (t *topicPolicies) GetMaxUnackMessagesPerSubscription(topic utils.TopicName, applied bool) (*int, error) {
	return t.GetMaxUnackMessagesPerSubscriptionWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetMaxUnackMessagesPerSubscriptionWithContext(
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

func (t *topicPolicies) SetMaxUnackMessagesPerSubscription(topic utils.TopicName, maxUnackedNum int) error {
	return t.SetMaxUnackMessagesPerSubscriptionWithContext(context.Background(), topic, maxUnackedNum)
}

func (t *topicPolicies) SetMaxUnackMessagesPerSubscriptionWithContext(
	ctx context.Context,
	topic utils.TopicName,
	maxUnackedNum int,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxUnackedMessagesOnSubscription"), &maxUnackedNum, nil)
}

func (t *topicPolicies) RemoveMaxUnackMessagesPerSubscription(topic utils.TopicName) error {
	return t.RemoveMaxUnackMessagesPerSubscriptionWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveMaxUnackMessagesPerSubscriptionWithContext(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxUnackedMessagesOnSubscription"), nil)
}

func (t *topicPolicies) GetPersistence(topic utils.TopicName, applied bool) (*utils.PersistenceData, error) {
	return t.GetPersistenceWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetPersistenceWithContext(
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

func (t *topicPolicies) SetPersistence(topic utils.TopicName, persistenceData utils.PersistenceData) error {
	return t.SetPersistenceWithContext(context.Background(), topic, persistenceData)
}

func (t *topicPolicies) SetPersistenceWithContext(
	ctx context.Context,
	topic utils.TopicName,
	persistenceData utils.PersistenceData,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "persistence"), &persistenceData, nil)
}

func (t *topicPolicies) RemovePersistence(topic utils.TopicName) error {
	return t.RemovePersistenceWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemovePersistenceWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "persistence"), nil)
}

func (t *topicPolicies) GetDelayedDelivery(topic utils.TopicName, applied bool) (*utils.DelayedDeliveryData, error) {
	return t.GetDelayedDeliveryWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetDelayedDeliveryWithContext(
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

func (t *topicPolicies) SetDelayedDelivery(topic utils.TopicName, delayedDeliveryData utils.DelayedDeliveryData) error {
	return t.SetDelayedDeliveryWithContext(context.Background(), topic, delayedDeliveryData)
}

func (t *topicPolicies) SetDelayedDeliveryWithContext(
	ctx context.Context,
	topic utils.TopicName,
	delayedDeliveryData utils.DelayedDeliveryData,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "delayedDelivery"), &delayedDeliveryData, nil)
}

func (t *topicPolicies) RemoveDelayedDelivery(topic utils.TopicName) error {
	return t.RemoveDelayedDeliveryWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveDelayedDeliveryWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "delayedDelivery"), nil)
}

func (t *topicPolicies) GetDispatchRate(topic utils.TopicName, applied bool) (*utils.DispatchRateData, error) {
	return t.GetDispatchRateWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetDispatchRateWithContext(
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

func (t *topicPolicies) SetDispatchRate(topic utils.TopicName, dispatchRateData utils.DispatchRateData) error {
	return t.SetDispatchRateWithContext(context.Background(), topic, dispatchRateData)
}

func (t *topicPolicies) SetDispatchRateWithContext(
	ctx context.Context,
	topic utils.TopicName,
	dispatchRateData utils.DispatchRateData,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "dispatchRate"), &dispatchRateData, nil)
}

func (t *topicPolicies) RemoveDispatchRate(topic utils.TopicName) error {
	return t.RemoveDispatchRateWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveDispatchRateWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "dispatchRate"), nil)
}

func (t *topicPolicies) GetPublishRate(topic utils.TopicName, applied bool) (*utils.PublishRateData, error) {
	return t.GetPublishRateWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetPublishRateWithContext(
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

func (t *topicPolicies) SetPublishRate(topic utils.TopicName, publishRateData utils.PublishRateData) error {
	return t.SetPublishRateWithContext(context.Background(), topic, publishRateData)
}

func (t *topicPolicies) SetPublishRateWithContext(
	ctx context.Context,
	topic utils.TopicName,
	publishRateData utils.PublishRateData,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "publishRate"), &publishRateData, nil)
}

func (t *topicPolicies) RemovePublishRate(topic utils.TopicName) error {
	return t.RemovePublishRateWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemovePublishRateWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "publishRate"), nil)
}

func (t *topicPolicies) GetDeduplicationStatus(topic utils.TopicName, applied bool) (*bool, error) {
	return t.GetDeduplicationStatusWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetDeduplicationStatusWithContext(
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

func (t *topicPolicies) SetDeduplicationStatus(topic utils.TopicName, enabled bool) error {
	return t.SetDeduplicationStatusWithContext(context.Background(), topic, enabled)
}

func (t *topicPolicies) SetDeduplicationStatusWithContext(
	ctx context.Context,
	topic utils.TopicName,
	enabled bool,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "deduplicationEnabled"), enabled, nil)
}

func (t *topicPolicies) RemoveDeduplicationStatus(topic utils.TopicName) error {
	return t.RemoveDeduplicationStatusWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveDeduplicationStatusWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "deduplicationEnabled"), nil)
}

func (t *topicPolicies) GetRetention(topic utils.TopicName, applied bool) (*utils.RetentionPolicies, error) {
	return t.GetRetentionWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetRetentionWithContext(
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

func (t *topicPolicies) SetRetention(topic utils.TopicName, data utils.RetentionPolicies) error {
	return t.SetRetentionWithContext(context.Background(), topic, data)
}

func (t *topicPolicies) SetRetentionWithContext(
	ctx context.Context,
	topic utils.TopicName,
	data utils.RetentionPolicies,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "retention"), data, nil)
}

func (t *topicPolicies) RemoveRetention(topic utils.TopicName) error {
	return t.RemoveRetentionWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveRetentionWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "retention"), nil)
}

func (t *topicPolicies) GetCompactionThreshold(topic utils.TopicName, applied bool) (*int64, error) {
	return t.GetCompactionThresholdWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetCompactionThresholdWithContext(
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

func (t *topicPolicies) SetCompactionThreshold(topic utils.TopicName, threshold int64) error {
	return t.SetCompactionThresholdWithContext(context.Background(), topic, threshold)
}

func (t *topicPolicies) SetCompactionThresholdWithContext(
	ctx context.Context,
	topic utils.TopicName,
	threshold int64,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "compactionThreshold"), threshold, nil)
}

func (t *topicPolicies) RemoveCompactionThreshold(topic utils.TopicName) error {
	return t.RemoveCompactionThresholdWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveCompactionThresholdWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "compactionThreshold"), nil)
}

func (t *topicPolicies) GetBacklogQuotaMap(
	topic utils.TopicName,
	applied bool,
) (map[utils.BacklogQuotaType]utils.BacklogQuota, error) {
	return t.GetBacklogQuotaMapWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetBacklogQuotaMapWithContext(
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
	topic utils.TopicName,
	backlogQuota utils.BacklogQuota,
	backlogQuotaType utils.BacklogQuotaType,
) error {
	return t.SetBacklogQuotaWithContext(context.Background(), topic, backlogQuota, backlogQuotaType)
}

func (t *topicPolicies) SetBacklogQuotaWithContext(
	ctx context.Context,
	topic utils.TopicName,
	backlogQuota utils.BacklogQuota,
	backlogQuotaType utils.BacklogQuotaType,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "backlogQuota"), &backlogQuota, map[string]string{
		"backlogQuotaType": string(backlogQuotaType),
	})
}

func (t *topicPolicies) RemoveBacklogQuota(topic utils.TopicName, backlogQuotaType utils.BacklogQuotaType) error {
	return t.RemoveBacklogQuotaWithContext(context.Background(), topic, backlogQuotaType)
}

func (t *topicPolicies) RemoveBacklogQuotaWithContext(
	ctx context.Context,
	topic utils.TopicName,
	backlogQuotaType utils.BacklogQuotaType,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "backlogQuota"), map[string]string{
		"backlogQuotaType": string(backlogQuotaType),
	})
}

func (t *topicPolicies) GetInactiveTopicPolicies(
	topic utils.TopicName,
	applied bool,
) (*utils.InactiveTopicPolicies, error) {
	return t.GetInactiveTopicPoliciesWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetInactiveTopicPoliciesWithContext(
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

func (t *topicPolicies) SetInactiveTopicPolicies(topic utils.TopicName, data utils.InactiveTopicPolicies) error {
	return t.SetInactiveTopicPoliciesWithContext(context.Background(), topic, data)
}

func (t *topicPolicies) SetInactiveTopicPoliciesWithContext(
	ctx context.Context,
	topic utils.TopicName,
	data utils.InactiveTopicPolicies,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "inactiveTopicPolicies"), data, nil)
}

func (t *topicPolicies) RemoveInactiveTopicPolicies(topic utils.TopicName) error {
	return t.RemoveInactiveTopicPoliciesWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveInactiveTopicPoliciesWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "inactiveTopicPolicies"), nil)
}

func (t *topicPolicies) GetReplicationClusters(topic utils.TopicName, applied bool) ([]string, error) {
	return t.GetReplicationClustersWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetReplicationClustersWithContext(
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

func (t *topicPolicies) SetReplicationClusters(topic utils.TopicName, data []string) error {
	return t.SetReplicationClustersWithContext(context.Background(), topic, data)
}

func (t *topicPolicies) SetReplicationClustersWithContext(
	ctx context.Context,
	topic utils.TopicName,
	data []string,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "replication"), data, nil)
}

func (t *topicPolicies) RemoveReplicationClusters(topic utils.TopicName) error {
	return t.RemoveReplicationClustersWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveReplicationClustersWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "replication"), nil)
}

func (t *topicPolicies) GetSubscribeRate(topic utils.TopicName, applied bool) (*utils.SubscribeRate, error) {
	return t.GetSubscribeRateWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetSubscribeRateWithContext(
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

func (t *topicPolicies) SetSubscribeRate(topic utils.TopicName, subscribeRate utils.SubscribeRate) error {
	return t.SetSubscribeRateWithContext(context.Background(), topic, subscribeRate)
}

func (t *topicPolicies) SetSubscribeRateWithContext(
	ctx context.Context,
	topic utils.TopicName,
	subscribeRate utils.SubscribeRate,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "subscribeRate"), &subscribeRate, nil)
}

func (t *topicPolicies) RemoveSubscribeRate(topic utils.TopicName) error {
	return t.RemoveSubscribeRateWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveSubscribeRateWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "subscribeRate"), nil)
}

func (t *topicPolicies) GetSubscriptionDispatchRate(
	topic utils.TopicName,
	applied bool,
) (*utils.DispatchRateData, error) {
	return t.GetSubscriptionDispatchRateWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetSubscriptionDispatchRateWithContext(
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

func (t *topicPolicies) SetSubscriptionDispatchRate(topic utils.TopicName, dispatchRate utils.DispatchRateData) error {
	return t.SetSubscriptionDispatchRateWithContext(context.Background(), topic, dispatchRate)
}

func (t *topicPolicies) SetSubscriptionDispatchRateWithContext(
	ctx context.Context,
	topic utils.TopicName,
	dispatchRate utils.DispatchRateData,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "subscriptionDispatchRate"), &dispatchRate, nil)
}

func (t *topicPolicies) RemoveSubscriptionDispatchRate(topic utils.TopicName) error {
	return t.RemoveSubscriptionDispatchRateWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveSubscriptionDispatchRateWithContext(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "subscriptionDispatchRate"), nil)
}

func (t *topicPolicies) GetMaxConsumersPerSubscription(topic utils.TopicName, applied bool) (*int, error) {
	return t.GetMaxConsumersPerSubscriptionWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetMaxConsumersPerSubscriptionWithContext(
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

func (t *topicPolicies) SetMaxConsumersPerSubscription(topic utils.TopicName, maxConsumers int) error {
	return t.SetMaxConsumersPerSubscriptionWithContext(context.Background(), topic, maxConsumers)
}

func (t *topicPolicies) SetMaxConsumersPerSubscriptionWithContext(
	ctx context.Context,
	topic utils.TopicName,
	maxConsumers int,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxConsumersPerSubscription"), &maxConsumers, nil)
}

func (t *topicPolicies) RemoveMaxConsumersPerSubscription(topic utils.TopicName) error {
	return t.RemoveMaxConsumersPerSubscriptionWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveMaxConsumersPerSubscriptionWithContext(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxConsumersPerSubscription"), nil)
}

func (t *topicPolicies) GetMaxMessageSize(topic utils.TopicName, applied bool) (*int, error) {
	return t.GetMaxMessageSizeWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetMaxMessageSizeWithContext(
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

func (t *topicPolicies) SetMaxMessageSize(topic utils.TopicName, maxMessageSize int) error {
	return t.SetMaxMessageSizeWithContext(context.Background(), topic, maxMessageSize)
}

func (t *topicPolicies) SetMaxMessageSizeWithContext(
	ctx context.Context,
	topic utils.TopicName,
	maxMessageSize int,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxMessageSize"), &maxMessageSize, nil)
}

func (t *topicPolicies) RemoveMaxMessageSize(topic utils.TopicName) error {
	return t.RemoveMaxMessageSizeWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveMaxMessageSizeWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxMessageSize"), nil)
}

func (t *topicPolicies) GetMaxSubscriptionsPerTopic(topic utils.TopicName, applied bool) (*int, error) {
	return t.GetMaxSubscriptionsPerTopicWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetMaxSubscriptionsPerTopicWithContext(
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

func (t *topicPolicies) SetMaxSubscriptionsPerTopic(topic utils.TopicName, maxSubscriptions int) error {
	return t.SetMaxSubscriptionsPerTopicWithContext(context.Background(), topic, maxSubscriptions)
}

func (t *topicPolicies) SetMaxSubscriptionsPerTopicWithContext(
	ctx context.Context,
	topic utils.TopicName,
	maxSubscriptions int,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "maxSubscriptionsPerTopic"), &maxSubscriptions, nil)
}

func (t *topicPolicies) RemoveMaxSubscriptionsPerTopic(topic utils.TopicName) error {
	return t.RemoveMaxSubscriptionsPerTopicWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveMaxSubscriptionsPerTopicWithContext(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "maxSubscriptionsPerTopic"), nil)
}

func (t *topicPolicies) GetSchemaValidationEnforced(topic utils.TopicName, applied bool) (*bool, error) {
	return t.GetSchemaValidationEnforcedWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetSchemaValidationEnforcedWithContext(
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

func (t *topicPolicies) SetSchemaValidationEnforced(topic utils.TopicName, enabled bool) error {
	return t.SetSchemaValidationEnforcedWithContext(context.Background(), topic, enabled)
}

func (t *topicPolicies) SetSchemaValidationEnforcedWithContext(
	ctx context.Context,
	topic utils.TopicName,
	enabled bool,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "schemaValidationEnforced"), enabled, nil)
}

func (t *topicPolicies) RemoveSchemaValidationEnforced(topic utils.TopicName) error {
	return t.RemoveSchemaValidationEnforcedWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveSchemaValidationEnforcedWithContext(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "schemaValidationEnforced"), nil)
}

func (t *topicPolicies) GetDeduplicationSnapshotInterval(topic utils.TopicName, applied bool) (*int, error) {
	return t.GetDeduplicationSnapshotIntervalWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetDeduplicationSnapshotIntervalWithContext(
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

func (t *topicPolicies) SetDeduplicationSnapshotInterval(topic utils.TopicName, interval int) error {
	return t.SetDeduplicationSnapshotIntervalWithContext(context.Background(), topic, interval)
}

func (t *topicPolicies) SetDeduplicationSnapshotIntervalWithContext(
	ctx context.Context,
	topic utils.TopicName,
	interval int,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "deduplicationSnapshotInterval"), &interval, nil)
}

func (t *topicPolicies) RemoveDeduplicationSnapshotInterval(topic utils.TopicName) error {
	return t.RemoveDeduplicationSnapshotIntervalWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveDeduplicationSnapshotIntervalWithContext(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "deduplicationSnapshotInterval"), nil)
}

func (t *topicPolicies) GetReplicatorDispatchRate(
	topic utils.TopicName,
	applied bool,
) (*utils.DispatchRateData, error) {
	return t.GetReplicatorDispatchRateWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetReplicatorDispatchRateWithContext(
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

func (t *topicPolicies) SetReplicatorDispatchRate(topic utils.TopicName, dispatchRate utils.DispatchRateData) error {
	return t.SetReplicatorDispatchRateWithContext(context.Background(), topic, dispatchRate)
}

func (t *topicPolicies) SetReplicatorDispatchRateWithContext(
	ctx context.Context,
	topic utils.TopicName,
	dispatchRate utils.DispatchRateData,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "replicatorDispatchRate"), &dispatchRate, nil)
}

func (t *topicPolicies) RemoveReplicatorDispatchRate(topic utils.TopicName) error {
	return t.RemoveReplicatorDispatchRateWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveReplicatorDispatchRateWithContext(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "replicatorDispatchRate"), nil)
}

func (t *topicPolicies) GetOffloadPolicies(topic utils.TopicName, applied bool) (*utils.OffloadPolicies, error) {
	return t.GetOffloadPoliciesWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetOffloadPoliciesWithContext(
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

func (t *topicPolicies) SetOffloadPolicies(topic utils.TopicName, offloadPolicies utils.OffloadPolicies) error {
	return t.SetOffloadPoliciesWithContext(context.Background(), topic, offloadPolicies)
}

func (t *topicPolicies) SetOffloadPoliciesWithContext(
	ctx context.Context,
	topic utils.TopicName,
	offloadPolicies utils.OffloadPolicies,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "offloadPolicies"), &offloadPolicies, nil)
}

func (t *topicPolicies) RemoveOffloadPolicies(topic utils.TopicName) error {
	return t.RemoveOffloadPoliciesWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveOffloadPoliciesWithContext(ctx context.Context, topic utils.TopicName) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "offloadPolicies"), nil)
}

func (t *topicPolicies) GetAutoSubscriptionCreation(
	topic utils.TopicName,
	applied bool,
) (*utils.AutoSubscriptionCreationOverride, error) {
	return t.GetAutoSubscriptionCreationWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetAutoSubscriptionCreationWithContext(
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
	topic utils.TopicName,
	autoSubCreation utils.AutoSubscriptionCreationOverride,
) error {
	return t.SetAutoSubscriptionCreationWithContext(context.Background(), topic, autoSubCreation)
}

func (t *topicPolicies) SetAutoSubscriptionCreationWithContext(
	ctx context.Context,
	topic utils.TopicName,
	autoSubCreation utils.AutoSubscriptionCreationOverride,
) error {
	return t.scopedPostWithContext(ctx, t.topicEndpoint(topic, "autoSubscriptionCreation"), &autoSubCreation, nil)
}

func (t *topicPolicies) RemoveAutoSubscriptionCreation(topic utils.TopicName) error {
	return t.RemoveAutoSubscriptionCreationWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveAutoSubscriptionCreationWithContext(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "autoSubscriptionCreation"), nil)
}

func (t *topicPolicies) GetSchemaCompatibilityStrategy(
	topic utils.TopicName,
	applied bool,
) (*utils.SchemaCompatibilityStrategy, error) {
	return t.GetSchemaCompatibilityStrategyWithContext(context.Background(), topic, applied)
}

func (t *topicPolicies) GetSchemaCompatibilityStrategyWithContext(
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
	topic utils.TopicName,
	strategy utils.SchemaCompatibilityStrategy,
) error {
	return t.SetSchemaCompatibilityStrategyWithContext(context.Background(), topic, strategy)
}

func (t *topicPolicies) SetSchemaCompatibilityStrategyWithContext(
	ctx context.Context,
	topic utils.TopicName,
	strategy utils.SchemaCompatibilityStrategy,
) error {
	return t.scopedPutWithContext(ctx, t.topicEndpoint(topic, "schemaCompatibilityStrategy"), strategy, nil)
}

func (t *topicPolicies) RemoveSchemaCompatibilityStrategy(topic utils.TopicName) error {
	return t.RemoveSchemaCompatibilityStrategyWithContext(context.Background(), topic)
}

func (t *topicPolicies) RemoveSchemaCompatibilityStrategyWithContext(
	ctx context.Context,
	topic utils.TopicName,
) error {
	return t.scopedDeleteWithContext(ctx, t.topicEndpoint(topic, "schemaCompatibilityStrategy"), nil)
}
