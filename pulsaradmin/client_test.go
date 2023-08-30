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

package pulsaradmin

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPulsarClientEndpointEscapes(t *testing.T) {
	client := pulsarClient{restClient: nil}
	actual := client.endpoint(APIV2, "/myendpoint", "abc%? /def", "ghi")
	expected := "/admin/v2/myendpoint/abc%25%3F%20%2Fdef/ghi"
	assert.Equal(t, expected, actual)
}

func TestNew(t *testing.T) {
	config := ClientConfig{}
	admin, err := NewClient(config)
	require.NoError(t, err)
	require.NotNil(t, admin)
}

func TestNewClientWithAuthProvider(t *testing.T) {
	provider := AuthProviderToken("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9." +
		"eyJzdWIiOiJhZG1pbiIsImlhdCI6MTUxNjIzOTAyMn0.sVt6cyu3HKd89LcQvZVMNbqT0DTl3FvG9oYbj8hBDqU")

	transport, err := provider(&http.Transport{})
	require.NoError(t, err)
	require.NotNil(t, transport)

	client, err := NewClient(ClientConfig{
		AuthProvider: provider,
	})
	require.NoError(t, err)
	require.NotNil(t, client)
}

type customAuthTransport struct {
	T http.RoundTripper
}

func (c *customAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	panic("implement me")
}

func customAuthProvider(t *http.Transport) (http.RoundTripper, error) {
	return &customAuthTransport{t}, nil
}

func TestNewWithCustomAuthProviderWithTransport(t *testing.T) {
	config := ClientConfig{
		AuthProvider: customAuthProvider,
		APIProfile:   &APIProfile{},
	}
	defaultTransport, err := defaultTransport(config)
	require.NoError(t, err)

	config.CustomTransport = defaultTransport

	client, err := NewClient(config)
	require.NoError(t, err)
	require.NotNil(t, client)

	underlyingClient, ok := client.(*pulsarClient)
	require.True(t, ok)
	require.NotNil(t, underlyingClient)
	require.NotNil(t, underlyingClient.restClient)
	require.NotNil(t, underlyingClient.restClient.HTTPClient)
	require.NotNil(t, underlyingClient.restClient.HTTPClient.Transport)
	require.IsType(t, &customAuthTransport{}, underlyingClient.restClient.HTTPClient.Transport)
	// Expected the custom transport will be retained
	require.Equal(t, defaultTransport, underlyingClient.restClient.HTTPClient.Transport.(*customAuthTransport).T)
}

func TestNewWithTlsAllowInsecure(t *testing.T) {
	client, err := NewClient(ClientConfig{
		TLSConfig: TLSConfig{
			AllowInsecureConnection: true,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	underlyingClient, ok := client.(*pulsarClient)
	require.True(t, ok)
	require.NotNil(t, underlyingClient)
	require.NotNil(t, underlyingClient.restClient.HTTPClient.Transport)
	tr, ok := underlyingClient.restClient.HTTPClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.NotNil(t, tr.TLSClientConfig)
	require.True(t, tr.TLSClientConfig.InsecureSkipVerify)
}
