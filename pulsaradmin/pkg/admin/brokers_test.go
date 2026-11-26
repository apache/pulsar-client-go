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
	"os"
	"strings"
	"testing"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestBrokerHealthCheckWithTopicVersion(t *testing.T) {
	readFile, err := os.ReadFile("../../../integration-tests/tokens/admin-token")
	assert.NoError(t, err)
	cfg := &config.Config{
		Token: string(readFile),
	}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)
	err = admin.Brokers().HealthCheck()
	assert.NoError(t, err)
	err = admin.Brokers().HealthCheckWithTopicVersion(utils.TopicVersionV1)
	assert.NoError(t, err)
	err = admin.Brokers().HealthCheckWithTopicVersion(utils.TopicVersionV2)
	assert.NoError(t, err)
}

func TestGetLeaderBroker(t *testing.T) {
	readFile, err := os.ReadFile("../../../integration-tests/tokens/admin-token")
	assert.NoError(t, err)
	cfg := &config.Config{
		Token: string(readFile),
	}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)
	leaderBroker, err := admin.Brokers().GetLeaderBroker()
	assert.NoError(t, err)
	assert.NotNil(t, leaderBroker)
	assert.NotEmpty(t, leaderBroker.ServiceURL)
	assert.NotEmpty(t, leaderBroker.BrokerID)
}

func TestGetAllActiveBrokers(t *testing.T) {
	readFile, err := os.ReadFile("../../../integration-tests/tokens/admin-token")
	assert.NoError(t, err)
	cfg := &config.Config{
		Token: string(readFile),
	}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	brokers, err := admin.Brokers().GetListActiveBrokers()
	assert.NoError(t, err)
	assert.NotEmpty(t, brokers)
}

func TestUpdateDynamicConfiguration(t *testing.T) {
	readFile, err := os.ReadFile("../../../integration-tests/tokens/admin-token")
	assert.NoError(t, err)
	cfg := &config.Config{
		Token: string(readFile),
	}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	err = admin.Brokers().UpdateDynamicConfiguration("allowAutoSubscriptionCreation", "true")
	assert.NoError(t, err)

	configurations, err := admin.Brokers().GetDynamicConfigurationNames()
	assert.NoError(t, err)
	assert.NotEmpty(t, configurations)
}

func TestUpdateDynamicConfigurationEscapedValueE2E(t *testing.T) {
	readFile, err := os.ReadFile("../../../integration-tests/tokens/admin-token")
	require.NoError(t, err)
	cfg := &config.Config{
		Token: string(readFile),
	}
	admin, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, admin)
	t.Cleanup(func() {
		assert.NoError(t, admin.Brokers().DeleteDynamicConfiguration("loadBalancerSheddingExcludedNamespaces"))
	})

	err = admin.Brokers().UpdateDynamicConfiguration("loadBalancerSheddingExcludedNamespaces", "my-tenant/my-namespace")
	require.NoError(t, err)

	configurations, err := admin.Brokers().GetAllDynamicConfigurations()
	require.NoError(t, err)
	require.NotEmpty(t, configurations)
	assert.Equal(t, "my-tenant/my-namespace", configurations["loadBalancerSheddingExcludedNamespaces"])
}

func TestUpdateDynamicConfigurationEscapesConfigValue(t *testing.T) {
	admin := &pulsarClient{
		APIVersion: config.V2,
		Client: &rest.Client{
			ServiceURL: "http://example.com",
			HTTPClient: &http.Client{
				Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					require.Equal(t, http.MethodPost, r.Method)
					require.Equal(
						t,
						"/admin/v2/brokers/configuration/loadBalancerSheddingExcludedNamespaces/my-tenant%2Fmy-namespace",
						r.URL.EscapedPath(),
					)
					return &http.Response{
						StatusCode: http.StatusNoContent,
						Body:       io.NopCloser(strings.NewReader("")),
						Header:     make(http.Header),
						Request:    r,
					}, nil
				}),
			},
		},
	}

	err := admin.Brokers().UpdateDynamicConfiguration("loadBalancerSheddingExcludedNamespaces", "my-tenant/my-namespace")
	require.NoError(t, err)
}

func TestUpdateDynamicConfigurationDoesNotCleanConfigPath(t *testing.T) {
	admin := &pulsarClient{
		APIVersion: config.V2,
		Client: &rest.Client{
			ServiceURL: "http://example.com",
			HTTPClient: &http.Client{
				Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					require.Equal(t, http.MethodPost, r.Method)
					require.Equal(
						t,
						"/admin/v2/brokers/configuration/loadBalancerSheddingExcludedNamespaces/..",
						r.URL.EscapedPath(),
					)
					return &http.Response{
						StatusCode: http.StatusNoContent,
						Body:       io.NopCloser(strings.NewReader("")),
						Header:     make(http.Header),
						Request:    r,
					}, nil
				}),
			},
		},
	}

	err := admin.Brokers().UpdateDynamicConfiguration("loadBalancerSheddingExcludedNamespaces", "..")
	require.NoError(t, err)
}

func TestUpdateDynamicConfigurationWithCustomURL(t *testing.T) {
	// The broker round-trip for a config value containing '/' is covered by
	// TestUpdateDynamicConfigurationEscapedValueE2E. Here an in-memory transport checks that a custom URL
	// keeps its escaped RawPath on the wire instead of sending literal slashes.
	value := `{"key/123":"https://example.com/"}`
	encoded := url.QueryEscape(value)
	const configPath = "/admin/v2/brokers/configuration/loadBalancerSheddingExcludedNamespaces/"

	client := rest.Client{
		ServiceURL:  "http://example.com",
		VersionInfo: ReleaseVersion,
		HTTPClient: &http.Client{
			Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, http.MethodPost, r.Method)
				require.Equal(t, configPath+encoded, r.URL.EscapedPath())
				require.Equal(t, configPath+value, r.URL.Path)
				return &http.Response{
					StatusCode: http.StatusNoContent,
					Body:       io.NopCloser(strings.NewReader("")),
					Header:     make(http.Header),
					Request:    r,
				}, nil
			}),
		},
	}

	resp, err := client.MakeRequestWithURLWithContext(context.Background(), http.MethodPost, &url.URL{
		Scheme:  "http",
		Host:    "example.com",
		Path:    configPath + value,
		RawPath: configPath + encoded,
	})
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}
