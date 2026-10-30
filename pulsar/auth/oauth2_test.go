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

package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/pulsar-client-go/oauth2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var expectedClientID atomic.Value
var expectedClientSecret atomic.Value

func oauth2TLSCertPath(name string) string {
	return filepath.Join("..", "..", "integration-tests", "certs", name)
}

func oauth2IssuerURL(serverURL string) string {
	return strings.Replace(serverURL, "127.0.0.1", "localhost", 1)
}

// mockOAuthServer will mock a oauth service for the tests
func mockOAuthServer() *httptest.Server {
	return mockOAuthServerWithToken("token-content")
}

// mockOAuthServerWithToken will mock a oauth service for the tests with a custom token.
func mockOAuthServerWithToken(token string) *httptest.Server {
	// prepare a port for the mocked server
	server := httptest.NewUnstartedServer(http.DefaultServeMux)

	// mock the used REST path for the tests
	mockedHandler := http.NewServeMux()
	mockedHandler.HandleFunc("/.well-known/openid-configuration", func(writer http.ResponseWriter, _ *http.Request) {
		s := fmt.Sprintf(`{
    "issuer":"%s",
    "authorization_endpoint":"%s/authorize",
    "token_endpoint":"%s/oauth/token",
    "device_authorization_endpoint":"%s/oauth/device/code"
}`, server.URL, server.URL, server.URL, server.URL)
		fmt.Fprintln(writer, s)
	})
	mockedHandler.HandleFunc("/oauth/token", func(writer http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(writer, "invalid form", http.StatusBadRequest)
			return
		}
		clientID := r.FormValue("client_id")
		clientSecret := r.FormValue("client_secret")
		if clientID != expectedClientID.Load().(string) || clientSecret != expectedClientSecret.Load().(string) {
			http.Error(writer, "invalid client credentials", http.StatusUnauthorized)
			return
		}
		fmt.Fprintf(writer, "{\n  \"access_token\": \"%s\",\n  \"token_type\": \"Bearer\"\n}\n", token)
	})
	mockedHandler.HandleFunc("/authorize", func(writer http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(writer, "true")
	})

	server.Config.Handler = mockedHandler
	server.Start()

	return server
}

func mockOAuthTLSServer(
	t *testing.T,
	token string,
	requireClientCert bool,
	tokenHandler func(*testing.T, http.ResponseWriter, *http.Request, string),
) *httptest.Server {
	t.Helper()

	serverCert, err := tls.LoadX509KeyPair(oauth2TLSCertPath("broker-cert.pem"), oauth2TLSCertPath("broker-key.pem"))
	require.NoError(t, err)

	server := httptest.NewUnstartedServer(http.DefaultServeMux)
	mockedHandler := http.NewServeMux()
	mockedHandler.HandleFunc("/.well-known/openid-configuration", func(writer http.ResponseWriter, _ *http.Request) {
		issuerURL := oauth2IssuerURL(server.URL)
		s := fmt.Sprintf(`{
    "issuer":"%s",
    "authorization_endpoint":"%s/authorize",
    "token_endpoint":"%s/oauth/token",
    "device_authorization_endpoint":"%s/oauth/device/code"
}`, issuerURL, issuerURL, issuerURL, issuerURL)
		fmt.Fprintln(writer, s)
	})
	mockedHandler.HandleFunc("/oauth/token", func(writer http.ResponseWriter, r *http.Request) {
		tokenHandler(t, writer, r, token)
	})
	mockedHandler.HandleFunc("/authorize", func(writer http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(writer, "true")
	})

	server.Config.Handler = mockedHandler
	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
	}

	if requireClientCert {
		caCerts, err := os.ReadFile(oauth2TLSCertPath("cacert.pem"))
		require.NoError(t, err)
		server.TLS.ClientCAs = x509.NewCertPool()
		server.TLS.ClientCAs.AppendCertsFromPEM(caCerts)
		server.TLS.ClientAuth = tls.RequireAndVerifyClientCert
	}

	server.StartTLS()
	return server
}

// mockKeyFile will mock a temp key file for testing.
func mockKeyFile(server string) (string, error) {
	return mockKeyFileWithClientID(server, "client-id")
}

func mockKeyFileWithClientID(server string, clientID string) (string, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	kf, err := os.CreateTemp(pwd, "test_oauth2")
	if err != nil {
		return "", err
	}
	_, err = kf.WriteString(fmt.Sprintf(`{
  "type":"resource",
  "client_id":"%s",
  "client_secret":"client-secret",
  "client_email":"oauth@test.org",
  "issuer_url":"%s",
  "scope": "test-scope"
}`, clientID, server))
	if err != nil {
		return "", err
	}

	return kf.Name(), nil
}

func mockKeyFileWithoutIssuer() (string, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	kf, err := os.CreateTemp(pwd, "test_oauth2")
	if err != nil {
		return "", err
	}
	_, err = kf.WriteString(`{
  "type":"resource",
  "client_id":"client-id",
  "client_secret":"client-secret",
  "client_email":"oauth@test.org",
  "scope": "test-scope"
}`)
	if err != nil {
		return "", err
	}

	return kf.Name(), nil
}

func TestNewAuthenticationOAuth2WithParams(t *testing.T) {
	server := mockOAuthServer()
	defer server.Close()
	expectedClientID.Store("client-id")
	expectedClientSecret.Store("client-secret")
	kf, err := mockKeyFile(server.URL)
	defer os.Remove(kf)
	if err != nil {
		t.Fatal(errors.Wrap(err, "create mocked key file failed"))
	}

	testData := []map[string]string{
		{
			ConfigParamType:      ConfigParamTypeClientCredentials,
			ConfigParamIssuerURL: server.URL,
			ConfigParamClientID:  "client-id",
			ConfigParamAudience:  "audience",
			ConfigParamKeyFile:   kf,
			ConfigParamScope:     "profile",
		},
		{
			ConfigParamType:      ConfigParamTypeClientCredentials,
			ConfigParamIssuerURL: server.URL,
			ConfigParamClientID:  "client-id",
			ConfigParamAudience:  "audience",
			ConfigParamKeyFile:   fmt.Sprintf("file://%s", kf),
			ConfigParamScope:     "profile",
		},
		{
			ConfigParamType:      ConfigParamTypeClientCredentials,
			ConfigParamIssuerURL: server.URL,
			ConfigParamClientID:  "client-id",
			ConfigParamAudience:  "audience",
			ConfigParamKeyFile: "data://" + fmt.Sprintf(`{
  "type":"resource",
  "client_id":"client-id",
  "client_secret":"client-secret",
  "client_email":"oauth@test.org",
  "issuer_url":"%s"
}`, server.URL),
			ConfigParamScope: "profile",
		},
	}

	for i := range testData {
		params := testData[i]
		auth, err := NewAuthenticationOAuth2WithParams(params)
		if err != nil {
			t.Fatal(err)
		}
		err = auth.Init()
		if err != nil {
			t.Fatal(err)
		}

		token, err := auth.GetData()
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "token-content", string(token))
	}
}

func TestOAuth2IssuerOverrideUsesAuthParams(t *testing.T) {
	expectedClientID.Store("client-id")
	expectedClientSecret.Store("client-secret")
	serverFromKeyFile := mockOAuthServerWithToken("token-from-keyfile")
	defer serverFromKeyFile.Close()
	serverFromParams := mockOAuthServerWithToken("token-from-params")
	defer serverFromParams.Close()

	kf, err := mockKeyFile(serverFromKeyFile.URL)
	defer os.Remove(kf)
	require.NoError(t, err)

	params := map[string]string{
		ConfigParamType:      ConfigParamTypeClientCredentials,
		ConfigParamIssuerURL: serverFromParams.URL,
		ConfigParamClientID:  "client-id",
		ConfigParamAudience:  "audience",
		ConfigParamKeyFile:   kf,
		ConfigParamScope:     "profile",
	}

	auth, err := NewAuthenticationOAuth2WithParams(params)
	require.NoError(t, err)
	require.NoError(t, auth.Init())

	token, err := auth.GetData()
	require.NoError(t, err)
	assert.Equal(t, "token-from-params", string(token))
}

func TestOAuth2MissingIssuerReturnsError(t *testing.T) {
	expectedClientID.Store("client-id")
	expectedClientSecret.Store("client-secret")
	kf, err := mockKeyFileWithoutIssuer()
	defer os.Remove(kf)
	require.NoError(t, err)

	params := map[string]string{
		ConfigParamType:     ConfigParamTypeClientCredentials,
		ConfigParamClientID: "client-id",
		ConfigParamAudience: "audience",
		ConfigParamKeyFile:  kf,
		ConfigParamScope:    "profile",
	}

	auth, err := NewAuthenticationOAuth2WithParams(params)
	require.NoError(t, err)
	require.NoError(t, auth.Init())

	_, err = auth.GetData()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "issuer url is required for client credentials flow")
}

func TestOAuth2ClientSecretPostWithTrustCertsFilePath(t *testing.T) {
	expectedClientID.Store("client-id")
	expectedClientSecret.Store("client-secret")
	server := mockOAuthTLSServer(t, "token-content", false, func(
		t *testing.T, writer http.ResponseWriter, r *http.Request, token string,
	) {
		require.NoError(t, r.ParseForm())
		assert.Equal(t, expectedClientID.Load().(string), r.FormValue("client_id"))
		assert.Equal(t, expectedClientSecret.Load().(string), r.FormValue("client_secret"))
		fmt.Fprintf(writer, "{\n  \"access_token\": \"%s\",\n  \"token_type\": \"Bearer\"\n}\n", token)
	})
	defer server.Close()

	kf, err := mockKeyFile(oauth2IssuerURL(server.URL))
	require.NoError(t, err)
	defer os.Remove(kf)

	auth, err := NewAuthenticationOAuth2WithParams(map[string]string{
		ConfigParamType:               ConfigParamTypeClientCredentials,
		ConfigParamIssuerURL:          oauth2IssuerURL(server.URL),
		ConfigParamClientID:           "client-id",
		ConfigParamAudience:           "audience",
		ConfigParamKeyFile:            kf,
		ConfigParamScope:              "profile",
		ConfigParamTrustCertsFilePath: oauth2TLSCertPath("cacert.pem"),
	})
	require.NoError(t, err)
	require.NoError(t, auth.Init())

	token, err := auth.GetData()
	require.NoError(t, err)
	assert.Equal(t, "token-content", string(token))
}

func TestOAuth2ClientSecretPostWithoutTrustCertsFilePathFailsAgainstCustomCA(t *testing.T) {
	expectedClientID.Store("client-id")
	expectedClientSecret.Store("client-secret")
	server := mockOAuthTLSServer(t, "token-content", false, func(
		t *testing.T, writer http.ResponseWriter, r *http.Request, token string,
	) {
		require.NoError(t, r.ParseForm())
		fmt.Fprintf(writer, "{\n  \"access_token\": \"%s\",\n  \"token_type\": \"Bearer\"\n}\n", token)
	})
	defer server.Close()

	kf, err := mockKeyFile(oauth2IssuerURL(server.URL))
	require.NoError(t, err)
	defer os.Remove(kf)

	auth, err := NewAuthenticationOAuth2WithParams(map[string]string{
		ConfigParamType:      ConfigParamTypeClientCredentials,
		ConfigParamIssuerURL: oauth2IssuerURL(server.URL),
		ConfigParamClientID:  "client-id",
		ConfigParamAudience:  "audience",
		ConfigParamKeyFile:   kf,
		ConfigParamScope:     "profile",
	})
	require.NoError(t, err)
	require.NoError(t, auth.Init())

	_, err = auth.GetData()
	require.Error(t, err)
}

func TestOAuth2ClientSecretPostWithClientCertificate(t *testing.T) {
	expectedClientID.Store("client-id")
	expectedClientSecret.Store("client-secret")
	server := mockOAuthTLSServer(t, "token-content", true, func(
		t *testing.T, writer http.ResponseWriter, r *http.Request, token string,
	) {
		require.NoError(t, r.ParseForm())
		assert.Equal(t, expectedClientID.Load().(string), r.FormValue("client_id"))
		assert.Equal(t, expectedClientSecret.Load().(string), r.FormValue("client_secret"))
		require.NotNil(t, r.TLS)
		require.NotEmpty(t, r.TLS.PeerCertificates)
		fmt.Fprintf(writer, "{\n  \"access_token\": \"%s\",\n  \"token_type\": \"Bearer\"\n}\n", token)
	})
	defer server.Close()

	kf, err := mockKeyFile(oauth2IssuerURL(server.URL))
	require.NoError(t, err)
	defer os.Remove(kf)

	auth, err := NewAuthenticationOAuth2WithParams(map[string]string{
		ConfigParamType:               ConfigParamTypeClientCredentials,
		ConfigParamIssuerURL:          oauth2IssuerURL(server.URL),
		ConfigParamClientID:           "client-id",
		ConfigParamAudience:           "audience",
		ConfigParamKeyFile:            kf,
		ConfigParamScope:              "profile",
		ConfigParamTLSCertFile:        oauth2TLSCertPath("client-cert.pem"),
		ConfigParamTLSKeyFile:         oauth2TLSCertPath("client-key.pem"),
		ConfigParamTrustCertsFilePath: oauth2TLSCertPath("cacert.pem"),
	})
	require.NoError(t, err)
	require.NoError(t, auth.Init())

	token, err := auth.GetData()
	require.NoError(t, err)
	assert.Equal(t, "token-content", string(token))
}

func TestOAuth2TLSClientAuth(t *testing.T) {
	expectedClientID.Store("pulsar-client")
	server := mockOAuthTLSServer(t, "mtls-token", true, func(
		t *testing.T, writer http.ResponseWriter, r *http.Request, token string,
	) {
		require.NoError(t, r.ParseForm())
		assert.Equal(t, expectedClientID.Load().(string), r.FormValue("client_id"))
		assert.Empty(t, r.FormValue("client_secret"))
		assert.Equal(t, "profile", r.FormValue("scope"))
		require.NotNil(t, r.TLS)
		require.NotEmpty(t, r.TLS.PeerCertificates)
		fmt.Fprintf(writer, "{\n  \"access_token\": \"%s\",\n  \"token_type\": \"Bearer\"\n}\n", token)
	})
	defer server.Close()

	auth, err := NewAuthenticationOAuth2WithParams(map[string]string{
		ConfigParamType:                    ConfigParamTypeClientCredentials,
		ConfigParamIssuerURL:               oauth2IssuerURL(server.URL),
		ConfigParamAudience:                "audience",
		ConfigParamScope:                   "profile",
		ConfigParamTokenEndpointAuthMethod: oauth2.TokenEndpointAuthMethodTLSClientAuth,
		ConfigParamTLSCertFile:             oauth2TLSCertPath("client-cert.pem"),
		ConfigParamTLSKeyFile:              oauth2TLSCertPath("client-key.pem"),
		ConfigParamTrustCertsFilePath:      oauth2TLSCertPath("cacert.pem"),
	})
	require.NoError(t, err)
	require.NoError(t, auth.Init())

	token, err := auth.GetData()
	require.NoError(t, err)
	assert.Equal(t, "mtls-token", string(token))
}

func TestOAuth2TLSClientAuthIgnoresKeyFileClientID(t *testing.T) {
	expectedClientID.Store("pulsar-client")
	server := mockOAuthTLSServer(t, "mtls-token", true, func(
		t *testing.T, writer http.ResponseWriter, r *http.Request, token string,
	) {
		require.NoError(t, r.ParseForm())
		assert.Equal(t, expectedClientID.Load().(string), r.FormValue("client_id"))
		assert.Empty(t, r.FormValue("client_secret"))
		fmt.Fprintf(writer, "{\n  \"access_token\": \"%s\",\n  \"token_type\": \"Bearer\"\n}\n", token)
	})
	defer server.Close()

	kf, err := mockKeyFileWithClientID(oauth2IssuerURL(server.URL), "wrong-client-id")
	require.NoError(t, err)
	defer os.Remove(kf)

	auth, err := NewAuthenticationOAuth2WithParams(map[string]string{
		ConfigParamType:                    ConfigParamTypeClientCredentials,
		ConfigParamIssuerURL:               oauth2IssuerURL(server.URL),
		ConfigParamAudience:                "audience",
		ConfigParamKeyFile:                 kf,
		ConfigParamTokenEndpointAuthMethod: oauth2.TokenEndpointAuthMethodTLSClientAuth,
		ConfigParamTLSCertFile:             oauth2TLSCertPath("client-cert.pem"),
		ConfigParamTLSKeyFile:              oauth2TLSCertPath("client-key.pem"),
		ConfigParamTrustCertsFilePath:      oauth2TLSCertPath("cacert.pem"),
	})
	require.NoError(t, err)
	require.NoError(t, auth.Init())

	token, err := auth.GetData()
	require.NoError(t, err)
	assert.Equal(t, "mtls-token", string(token))
}

func TestOAuth2KeyFileReloading(t *testing.T) {
	server := mockOAuthServer()
	defer server.Close()
	expectedClientID.Store("client-id")
	expectedClientSecret.Store("client-secret")
	kf, err := mockKeyFile(server.URL)
	defer os.Remove(kf)
	require.NoError(t, err)

	params := map[string]string{
		ConfigParamType:      ConfigParamTypeClientCredentials,
		ConfigParamIssuerURL: server.URL,
		ConfigParamClientID:  "client-id",
		ConfigParamAudience:  "audience",
		ConfigParamKeyFile:   fmt.Sprintf("file://%s", kf),
		ConfigParamScope:     "profile",
	}

	auth, err := NewAuthenticationOAuth2WithParams(params)
	require.NoError(t, err)
	err = auth.Init()
	require.NoError(t, err)

	token, err := auth.GetData()
	require.NoError(t, err)
	assert.Equal(t, "token-content", string(token))

	expectedClientSecret.Store("new-client-secret")
	_, err = auth.GetData()
	require.Error(t, err) // The token refresh should be failed after updating the client-secret

	// now update the key file to have different client credentials
	keyFile, err := os.OpenFile(kf, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	require.NoError(t, err)
	_, err = keyFile.WriteString(fmt.Sprintf(`{
  "type":"resource",
  "client_id":"client-id",
  "client_secret":"new-client-secret",
  "client_email":"oauth@test.org",
  "issuer_url":"%s"
}`, server.URL))
	require.NoError(t, err)
	require.NoError(t, keyFile.Close())

	token, err = auth.GetData()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "token-content", string(token))
}

func TestGrantProviderScopes(t *testing.T) {
	expectedClientID.Store("client-id")
	expectedClientSecret.Store("client-secret")
	server := mockOAuthServer()
	defer server.Close()
	kf, err := mockKeyFile(server.URL)
	defer os.Remove(kf)
	require.NoError(t, err)

	grantProvider := oauth2.DefaultGrantProvider{}
	grant, err := grantProvider.GetGrant("test-audience", &oauth2.ClientCredentialsFlowOptions{
		KeyFile:          kf,
		AdditionalScopes: []string{"scope1", "scope2"},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"scope1", "scope2", "test-scope"}, grant.Scopes)
}
