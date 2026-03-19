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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

type topicPolicyRequest struct {
	method string
	path   string
	query  url.Values
	body   string
}

func newTopicPolicyTestClient(t *testing.T, handler http.HandlerFunc) (Client, *pulsarClient) {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	client, err := New(&config.Config{WebServiceURL: server.URL})
	require.NoError(t, err)

	pulsarClient, ok := client.(*pulsarClient)
	require.True(t, ok)
	return client, pulsarClient
}

func mustTopicName(t *testing.T, topic string) *utils.TopicName {
	t.Helper()

	topicName, err := utils.GetTopicName(topic)
	require.NoError(t, err)
	return topicName
}

func TestTopicPoliciesOf(t *testing.T) {
	client, err := New(&config.Config{})
	require.NoError(t, err)

	local, err := TopicPoliciesOf(client, false)
	require.NoError(t, err)
	require.NotNil(t, local)
	assert.False(t, local.(*topicPolicies).isGlobal)

	global, err := TopicPoliciesOf(client, true)
	require.NoError(t, err)
	require.NotNil(t, global)
	assert.True(t, global.(*topicPolicies).isGlobal)
}

func TestTopicPoliciesScopeAndAppliedParams(t *testing.T) {
	requests := make([]topicPolicyRequest, 0, 4)
	client, pulsarClient := newTopicPolicyTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		requests = append(requests, topicPolicyRequest{
			method: r.Method,
			path:   r.URL.EscapedPath(),
			query:  r.URL.Query(),
			body:   string(body),
		})

		switch {
		case r.Method == http.MethodGet:
			_, err = w.Write([]byte("10"))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})

	topic := mustTopicName(t, "persistent://public/default/scoped-policy")

	localPolicies, err := TopicPoliciesOf(client, false)
	require.NoError(t, err)
	globalPolicies, err := TopicPoliciesOf(client, true)
	require.NoError(t, err)

	ttl, err := localPolicies.GetMessageTTL(*topic, true)
	require.NoError(t, err)
	require.NotNil(t, ttl)
	assert.Equal(t, 10, *ttl)

	ttl, err = globalPolicies.GetMessageTTL(*topic, false)
	require.NoError(t, err)
	require.NotNil(t, ttl)
	assert.Equal(t, 10, *ttl)

	err = globalPolicies.SetMaxProducers(*topic, 3)
	require.NoError(t, err)

	err = globalPolicies.RemoveRetention(*topic)
	require.NoError(t, err)

	expectedMessageTTLPath := pulsarClient.endpoint("", topic.GetRestPath(), "messageTTL")
	expectedMaxProducersPath := pulsarClient.endpoint("", topic.GetRestPath(), "maxProducers")
	expectedRetentionPath := pulsarClient.endpoint("", topic.GetRestPath(), "retention")
	decodedExpectedMessageTTLPath, err := url.PathUnescape(expectedMessageTTLPath)
	require.NoError(t, err)
	decodedExpectedMaxProducersPath, err := url.PathUnescape(expectedMaxProducersPath)
	require.NoError(t, err)
	decodedExpectedRetentionPath, err := url.PathUnescape(expectedRetentionPath)
	require.NoError(t, err)

	require.Len(t, requests, 4)

	assert.Equal(t, http.MethodGet, requests[0].method)
	assert.Equal(t, decodedExpectedMessageTTLPath, requests[0].path)
	assert.Equal(t, "true", requests[0].query.Get("applied"))
	assert.Empty(t, requests[0].query.Get("isGlobal"))

	assert.Equal(t, http.MethodGet, requests[1].method)
	assert.Equal(t, decodedExpectedMessageTTLPath, requests[1].path)
	assert.Equal(t, "false", requests[1].query.Get("applied"))
	assert.Equal(t, "true", requests[1].query.Get("isGlobal"))

	assert.Equal(t, http.MethodPost, requests[2].method)
	assert.Equal(t, decodedExpectedMaxProducersPath, requests[2].path)
	assert.Equal(t, "true", requests[2].query.Get("isGlobal"))
	assert.Contains(t, requests[2].body, "3")

	assert.Equal(t, http.MethodDelete, requests[3].method)
	assert.Equal(t, decodedExpectedRetentionPath, requests[3].path)
	assert.Equal(t, "true", requests[3].query.Get("isGlobal"))
}

func TestTopicPoliciesNullDecodingAndLegacyDefaults(t *testing.T) {
	client, _ := newTopicPolicyTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("null"))
		require.NoError(t, err)
	})

	topic := mustTopicName(t, "persistent://public/default/null-policy")
	policies, err := TopicPoliciesOf(client, false)
	require.NoError(t, err)

	ttl, err := policies.GetMessageTTL(*topic, false)
	require.NoError(t, err)
	assert.Nil(t, ttl)

	deduplication, err := policies.GetDeduplicationStatus(*topic, false)
	require.NoError(t, err)
	assert.Nil(t, deduplication)

	threshold, err := policies.GetCompactionThreshold(*topic, false)
	require.NoError(t, err)
	assert.Nil(t, threshold)

	strategy, err := policies.GetSchemaCompatibilityStrategy(*topic, false)
	require.NoError(t, err)
	assert.Nil(t, strategy)

	inactivePolicies, err := policies.GetInactiveTopicPolicies(*topic, false)
	require.NoError(t, err)
	assert.Nil(t, inactivePolicies)

	replicationClusters, err := policies.GetReplicationClusters(*topic, false)
	require.NoError(t, err)
	assert.Nil(t, replicationClusters)

	backlogQuotaMap, err := policies.GetBacklogQuotaMap(*topic, false)
	require.NoError(t, err)
	assert.Nil(t, backlogQuotaMap)

	legacyTTL, err := client.Topics().GetMessageTTL(*topic)
	require.NoError(t, err)
	assert.Equal(t, -1, legacyTTL)

	legacyDeduplication, err := client.Topics().GetDeduplicationStatus(*topic)
	require.NoError(t, err)
	assert.False(t, legacyDeduplication)

	legacyThreshold, err := client.Topics().GetCompactionThreshold(*topic, false)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), legacyThreshold)

	legacyStrategy, err := client.Topics().GetSchemaCompatibilityStrategyApplied(*topic, false)
	require.NoError(t, err)
	assert.Equal(t, utils.SchemaCompatibilityStrategyUndefined, legacyStrategy)

	legacyInactivePolicies, err := client.Topics().GetInactiveTopicPolicies(*topic, false)
	require.NoError(t, err)
	assert.Equal(t, utils.InactiveTopicPolicies{}, legacyInactivePolicies)

	legacyReplicationClusters, err := client.Topics().GetReplicationClusters(*topic)
	require.NoError(t, err)
	assert.Nil(t, legacyReplicationClusters)

	legacyBacklogQuotaMap, err := client.Topics().GetBacklogQuotaMap(*topic, false)
	require.NoError(t, err)
	assert.Nil(t, legacyBacklogQuotaMap)
}

func TestLocalTopicPoliciesParityWithTopics(t *testing.T) {
	client, _ := newTopicPolicyTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("600"))
		require.NoError(t, err)
	})

	topic := mustTopicName(t, "persistent://public/default/parity-policy")
	localPolicies, err := TopicPoliciesOf(client, false)
	require.NoError(t, err)

	newTTL, err := localPolicies.GetMessageTTL(*topic, false)
	require.NoError(t, err)
	require.NotNil(t, newTTL)

	legacyTTL, err := client.Topics().GetMessageTTL(*topic)
	require.NoError(t, err)

	assert.Equal(t, legacyTTL, *newTTL)
}
