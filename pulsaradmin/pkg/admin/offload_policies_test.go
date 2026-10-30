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
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

func TestOffloadPoliciesTopicAppliedAndNamespaceAPIs(t *testing.T) {
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

		if r.Method == http.MethodGet {
			_, err = w.Write([]byte(`{"managedLedgerOffloadDriver":"aws-s3",` +
				`"managedLedgerOffloadBucket":"universal-bucket",` +
				`"managedLedgerOffloadRegion":"us-west-2",` +
				`"managedLedgerOffloadServiceEndpoint":"https://storage.example.com",` +
				`"s3ManagedLedgerOffloadCredentialId":"access-key"}`))
			require.NoError(t, err)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	topic := mustTopicName(t, "persistent://public/default/offload-policy")
	namespace, err := utils.GetNamespaceName("public/default")
	require.NoError(t, err)

	topicPolicies, err := client.Topics().GetOffloadPoliciesApplied(*topic, true)
	require.NoError(t, err)
	require.NotNil(t, topicPolicies)
	assert.Equal(t, "aws-s3", topicPolicies.ManagedLedgerOffloadDriver)
	assert.Equal(t, "universal-bucket", topicPolicies.ManagedLedgerOffloadBucket)
	assert.Equal(t, "us-west-2", topicPolicies.ManagedLedgerOffloadRegion)
	assert.Equal(t, "https://storage.example.com", topicPolicies.ManagedLedgerOffloadServiceEndpoint)
	assert.Equal(t, "access-key", topicPolicies.S3ManagedLedgerOffloadCredentialID)

	err = client.Namespaces().SetOffloadPolicies(*namespace, utils.OffloadPolicies{
		ManagedLedgerOffloadDriver:          "aws-s3",
		ManagedLedgerOffloadBucket:          "universal-bucket",
		ManagedLedgerOffloadRegion:          "us-west-2",
		ManagedLedgerOffloadServiceEndpoint: "https://storage.example.com",
		S3ManagedLedgerOffloadCredentialID:  "access-key",
	})
	require.NoError(t, err)

	namespacePolicies, err := client.Namespaces().GetOffloadPolicies(*namespace)
	require.NoError(t, err)
	require.NotNil(t, namespacePolicies)
	assert.Equal(t, "universal-bucket", namespacePolicies.ManagedLedgerOffloadBucket)

	err = client.Namespaces().RemoveOffloadPolicies(*namespace)
	require.NoError(t, err)

	expectedTopicPath := pulsarClient.endpoint("", topic.GetRestPath(), "offloadPolicies")
	decodedExpectedTopicPath, err := url.PathUnescape(expectedTopicPath)
	require.NoError(t, err)
	expectedNamespacePath := pulsarClient.endpoint("/namespaces", namespace.String(), "offloadPolicies")
	decodedExpectedNamespacePath, err := url.PathUnescape(expectedNamespacePath)
	require.NoError(t, err)
	expectedNamespaceRemovePath := pulsarClient.endpoint("/namespaces", namespace.String(), "removeOffloadPolicies")
	decodedExpectedNamespaceRemovePath, err := url.PathUnescape(expectedNamespaceRemovePath)
	require.NoError(t, err)

	require.Len(t, requests, 4)

	assert.Equal(t, http.MethodGet, requests[0].method)
	assert.Equal(t, decodedExpectedTopicPath, requests[0].path)
	assert.Equal(t, "true", requests[0].query.Get("applied"))

	assert.Equal(t, http.MethodPost, requests[1].method)
	assert.Equal(t, decodedExpectedNamespacePath, requests[1].path)
	assert.JSONEq(t, `{"managedLedgerOffloadDriver":"aws-s3",`+
		`"s3ManagedLedgerOffloadCredentialId":"access-key",`+
		`"managedLedgerOffloadBucket":"universal-bucket",`+
		`"managedLedgerOffloadRegion":"us-west-2",`+
		`"managedLedgerOffloadServiceEndpoint":"https://storage.example.com"}`, requests[1].body)

	assert.Equal(t, http.MethodGet, requests[2].method)
	assert.Equal(t, decodedExpectedNamespacePath, requests[2].path)

	assert.Equal(t, http.MethodDelete, requests[3].method)
	assert.Equal(t, decodedExpectedNamespaceRemovePath, requests[3].path)
}

func TestNamespaceOffloadPoliciesNullDecoding(t *testing.T) {
	client, _ := newTopicPolicyTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("null"))
		require.NoError(t, err)
	})

	namespace, err := utils.GetNamespaceName("public/default")
	require.NoError(t, err)

	policies, err := client.Namespaces().GetOffloadPolicies(*namespace)
	require.NoError(t, err)
	assert.Nil(t, policies)
}

func TestScopedTopicOffloadPoliciesGlobalParam(t *testing.T) {
	requests := make([]topicPolicyRequest, 0, 2)
	client, _ := newTopicPolicyTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		requests = append(requests, topicPolicyRequest{
			method: r.Method,
			path:   r.URL.EscapedPath(),
			query:  r.URL.Query(),
			body:   string(body),
		})
		w.WriteHeader(http.StatusNoContent)
	})

	topic := mustTopicName(t, "persistent://public/default/global-offload-policy")
	policies, err := TopicPoliciesOf(client, true)
	require.NoError(t, err)

	err = policies.SetOffloadPolicies(context.Background(), *topic, utils.OffloadPolicies{
		ManagedLedgerOffloadDriver: "aws-s3",
		ManagedLedgerOffloadBucket: "universal-bucket",
	})
	require.NoError(t, err)
	err = policies.RemoveOffloadPolicies(context.Background(), *topic)
	require.NoError(t, err)

	require.Len(t, requests, 2)
	assert.Equal(t, "true", requests[0].query.Get("isGlobal"))
	assert.Contains(t, requests[0].body, "managedLedgerOffloadBucket")
	assert.Equal(t, "true", requests[1].query.Get("isGlobal"))
}
