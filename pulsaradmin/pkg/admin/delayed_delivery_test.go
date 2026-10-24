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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

func TestNamespacesDelayedDeliveryAPIs(t *testing.T) {
	for _, withContext := range []bool{false, true} {
		t.Run(fmt.Sprintf("withContext=%t", withContext), func(t *testing.T) {
			const body = `{"active":false,"tickTime":100,"maxDeliveryDelayInMillis":60000}`
			requests := make(chan topicPolicyRequest, 3)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
				data, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				requests <- topicPolicyRequest{
					method: r.Method, path: r.URL.EscapedPath(), query: r.URL.Query(), body: string(data),
				}
				if r.Method == http.MethodGet {
					_, err = io.WriteString(w, body)
					assert.NoError(t, err)
				} else {
					w.WriteHeader(http.StatusNoContent)
				}
			}))
			t.Cleanup(server.Close)
			client, err := New(&config.Config{WebServiceURL: server.URL, Token: "test-token"})
			require.NoError(t, err)
			namespaces := client.Namespaces()
			want := utils.NewDelayedDeliveryDataWithMaxDelay(100, false, 60000)
			var got *utils.DelayedDeliveryData
			if withContext {
				got, err = namespaces.GetDelayedDeliveryWithContext(context.Background(), "public/default")
				require.NoError(t, err)
				require.NoError(t, namespaces.SetDelayedDeliveryWithContext(context.Background(), "public/default", *want))
				require.NoError(t, namespaces.RemoveDelayedDeliveryWithContext(context.Background(), "public/default"))
			} else {
				got, err = namespaces.GetDelayedDelivery("public/default")
				require.NoError(t, err)
				require.NoError(t, namespaces.SetDelayedDelivery("public/default", *want))
				require.NoError(t, namespaces.RemoveDelayedDelivery("public/default"))
			}
			assert.Equal(t, want, got)
			for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
				request := <-requests
				assert.Equal(t, method, request.method)
				assert.Equal(t, "/admin/v2/namespaces/public/default/delayedDelivery", request.path)
				assert.Empty(t, request.query)
				if method == http.MethodPost {
					assert.JSONEq(t, body, request.body)
				} else {
					assert.Empty(t, request.body)
				}
			}
		})
	}
}

func TestNamespacesDelayedDeliveryDecoding(t *testing.T) {
	for _, test := range []struct {
		name    string
		status  int
		body    string
		want    *utils.DelayedDeliveryData
		wantErr bool
	}{
		{name: "no content", status: http.StatusNoContent},
		{name: "empty", status: http.StatusOK},
		{name: "null", status: http.StatusOK, body: " null "},
		{
			name: "without max delay", status: http.StatusOK,
			body: `{"active":true,"tickTime":1000}`, want: utils.NewDelayedDeliveryData(1000, true),
		},
		{name: "invalid JSON", status: http.StatusOK, body: "{", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, _ := newTopicPolicyTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, err := io.WriteString(w, test.body)
				assert.NoError(t, err)
			})
			got, err := client.Namespaces().GetDelayedDelivery("public/default")
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, test.want, got)
		})
	}
}

func TestNamespacesDelayedDeliveryErrors(t *testing.T) {
	for _, operation := range []struct {
		name string
		call func(Namespaces, context.Context, string) error
	}{
		{"get", func(n Namespaces, ctx context.Context, ns string) error {
			policy, err := n.GetDelayedDeliveryWithContext(ctx, ns)
			assert.Nil(t, policy)
			return err
		}},
		{"set", func(n Namespaces, ctx context.Context, ns string) error {
			return n.SetDelayedDeliveryWithContext(ctx, ns, *utils.NewDelayedDeliveryData(100, true))
		}},
		{"remove", func(n Namespaces, ctx context.Context, ns string) error {
			return n.RemoveDelayedDeliveryWithContext(ctx, ns)
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusConflict,
				http.StatusInternalServerError} {
				t.Run(http.StatusText(status), func(t *testing.T) {
					client, _ := newTopicPolicyTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
						w.WriteHeader(status)
						_, err := io.WriteString(w, `{"reason":"policy operation failed"}`)
						assert.NoError(t, err)
					})
					err := operation.call(client.Namespaces(), context.Background(), "public/default")
					var responseError rest.Error
					require.ErrorAs(t, err, &responseError)
					assert.Equal(t, status, responseError.Code)
				})
			}
			t.Run("invalid namespace", func(t *testing.T) {
				client, _ := newTopicPolicyTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
					t.Error("invalid namespace must not issue an HTTP request")
					w.WriteHeader(http.StatusBadRequest)
				})
				for _, namespace := range []string{"", "public", "public/", "public/default/extra"} {
					require.Error(t, operation.call(client.Namespaces(), context.Background(), namespace))
				}
			})
			t.Run("canceled context", func(t *testing.T) {
				client, _ := newTopicPolicyTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
					t.Error("canceled context must not issue an HTTP request")
					w.WriteHeader(http.StatusBadRequest)
				})
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				require.ErrorIs(t, operation.call(client.Namespaces(), ctx, "public/default"), context.Canceled)
			})
		})
	}
}

func TestTopicPoliciesDelayedDeliveryMaxDelay(t *testing.T) {
	const body = `{"active":true,"tickTime":100,"maxDeliveryDelayInMillis":60000}`
	client, _ := newTopicPolicyTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/admin/v2/persistent/public/default/delayed-delivery/delayedDelivery", r.URL.Path)
		switch r.Method {
		case http.MethodGet:
			_, err := io.WriteString(w, body)
			assert.NoError(t, err)
		case http.MethodPost:
			data, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.JSONEq(t, body, string(data))
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected method: %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	topic := mustTopicName(t, "persistent://public/default/delayed-delivery")
	want := utils.NewDelayedDeliveryDataWithMaxDelay(100, true, 60000)
	require.NoError(t, client.Topics().SetDelayedDelivery(*topic, *want))
	got, err := client.Topics().GetDelayedDelivery(*topic)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestNamespacesDelayedDeliveryIntegration(t *testing.T) {
	client, err := New(&config.Config{})
	require.NoError(t, err)
	namespaces := client.Namespaces()
	namespace := fmt.Sprintf("public/delayed-delivery-%d", time.Now().UnixNano())
	require.NoError(t, namespaces.CreateNamespace(namespace))
	t.Cleanup(func() {
		assert.NoError(t, namespaces.DeleteNamespace(namespace))
	})
	policy, err := namespaces.GetDelayedDelivery(namespace)
	require.NoError(t, err)
	require.Nil(t, policy)
	for _, want := range []utils.DelayedDeliveryData{
		*utils.NewDelayedDeliveryDataWithMaxDelay(100, true, 60000),
		*utils.NewDelayedDeliveryDataWithMaxDelay(200, false, 120000),
		*utils.NewDelayedDeliveryData(1000, true),
	} {
		require.NoError(t, namespaces.SetDelayedDelivery(namespace, want))
		policy, err = namespaces.GetDelayedDelivery(namespace)
		require.NoError(t, err)
		require.Equal(t, &want, policy)
	}
	require.NoError(t, namespaces.RemoveDelayedDelivery(namespace))
	policy, err = namespaces.GetDelayedDelivery(namespace)
	require.NoError(t, err)
	require.Nil(t, policy)
}
