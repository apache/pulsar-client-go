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

import (
	"net/http"
	"testing"

	"github.com/streamnative/pulsar-admin-go/pkg/auth"
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPulsarClientEndpointEscapes(t *testing.T) {
	client := pulsarClient{Client: nil, APIVersion: common.V2}
	actual := client.endpoint("/myendpoint", "abc%? /def", "ghi")
	expected := "/admin/v2/myendpoint/abc%25%3F%20%2Fdef/ghi"
	assert.Equal(t, expected, actual)
}

func TestNew(t *testing.T) {
	config := &common.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)
}

func TestNewWithAuthProvider(t *testing.T) {
	config := &common.Config{}

	tokenAuth, err := auth.NewAuthenticationToken("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."+
		"eyJzdWIiOiJhZG1pbiIsImlhdCI6MTUxNjIzOTAyMn0.sVt6cyu3HKd89LcQvZVMNbqT0DTl3FvG9oYbj8hBDqU", nil)
	require.NoError(t, err)
	require.NotNil(t, tokenAuth)

	admin, err := NewPulsarClientWithAuthProvider(config, tokenAuth)
	require.NoError(t, err)
	require.NotNil(t, admin)
}

type customAuthProvider struct {
	transport http.RoundTripper
}

var _ auth.Provider = &customAuthProvider{}

func (c *customAuthProvider) RoundTrip(req *http.Request) (*http.Response, error) {
	panic("implement me")
}

func (c *customAuthProvider) Transport() http.RoundTripper {
	return c.transport
}

func (c *customAuthProvider) WithTransport(transport http.RoundTripper) {
	c.transport = transport
}

func TestNewWithCustomAuthProviderWithTransport(t *testing.T) {
	config := &common.Config{}
	defaultTransport, err := auth.NewDefaultTransport(config)
	require.NoError(t, err)

	customAuthProvider := &customAuthProvider{
		transport: defaultTransport,
	}

	admin, err := NewPulsarClientWithAuthProvider(config, customAuthProvider)
	require.NoError(t, err)
	require.NotNil(t, admin)

	// Expected the transport of customAuthProvider will not be overwritten.
	require.Equal(t, defaultTransport, admin.(*pulsarClient).Client.HTTPClient.Transport)
}

func TestNewWithTlsAllowInsecure(t *testing.T) {
	config := &common.Config{
		TLSAllowInsecureConnection: true,
	}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	pulsarClientS := admin.(*pulsarClient)
	require.NotNil(t, pulsarClientS.Client.HTTPClient.Transport)
	tr := pulsarClientS.Client.HTTPClient.Transport.(*http.Transport)
	require.NotNil(t, tr)
	require.NotNil(t, tr.TLSClientConfig)
	require.True(t, tr.TLSClientConfig.InsecureSkipVerify)
}
