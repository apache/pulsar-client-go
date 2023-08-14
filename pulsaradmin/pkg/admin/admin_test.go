// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/auth"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
)

func TestPulsarClientEndpointEscapes(t *testing.T) {
	client := pulsarClient{Client: nil, APIVersion: config.V2}
	actual := client.endpoint("/myendpoint", "abc%? /def", "ghi")
	expected := "/admin/v2/myendpoint/abc%25%3F%20%2Fdef/ghi"
	assert.Equal(t, expected, actual)
}

func TestNew(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)
}

func TestNewWithAuthProvider(t *testing.T) {
	config := &config.Config{}

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
	config := &config.Config{}
	defaultTransport, err := auth.NewDefaultTransport(config)
	require.NoError(t, err)

	customAuthProvider := &customAuthProvider{
		transport: defaultTransport,
	}

	admin, err := NewPulsarClientWithAuthProvider(config, customAuthProvider)
	require.NoError(t, err)
	require.NotNil(t, admin)

	// Expected the customAuthProvider will not be overwritten.
	require.Equal(t, customAuthProvider, admin.(*pulsarClient).Client.HTTPClient.Transport)
}

func TestNewWithTlsAllowInsecure(t *testing.T) {
	config := &config.Config{
		TLSAllowInsecureConnection: true,
	}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	pulsarClientS := admin.(*pulsarClient)
	require.NotNil(t, pulsarClientS.Client.HTTPClient.Transport)

	ap := pulsarClientS.Client.HTTPClient.Transport.(*auth.DefaultProvider)
	tr := ap.Transport().(*http.Transport)
	require.NotNil(t, tr)
	require.NotNil(t, tr.TLSClientConfig)
	require.True(t, tr.TLSClientConfig.InsecureSkipVerify)
}
