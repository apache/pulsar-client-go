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

package oauth2

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/pulsar-client-go/oauth2/clock"
	"github.com/apache/pulsar-client-go/oauth2/clock/testing"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

type MockClientCredentialsProvider struct {
	Called                  bool
	ClientCredentialsResult *KeyFile
	ReturnsError            error
}

func (m *MockClientCredentialsProvider) GetClientCredentials() (*KeyFile, error) {
	m.Called = true
	return m.ClientCredentialsResult, m.ReturnsError
}

var _ ClientCredentialsProvider = &MockClientCredentialsProvider{}

type StaticGrantProvider struct {
	Grant *AuthorizationGrant
	Error error
}

func (s *StaticGrantProvider) GetGrant(_ string, _ *ClientCredentialsFlowOptions) (*AuthorizationGrant, error) {
	return s.Grant, s.Error
}

var _ GrantProvider = &StaticGrantProvider{}

var clientCredentials = KeyFile{
	Type:         "resource",
	ClientID:     "test_clientID",
	ClientSecret: "test_clientSecret",
	ClientEmail:  "test_clientEmail",
	IssuerURL:    "http://issuer",
	Scope:        "test_scope",
}

func oauth2TestCertPath(name string) string {
	return filepath.Join("..", "integration-tests", "certs", name)
}

func mockWellKnownServer(tokenEndpoint string) *httptest.Server {
	handler := http.NewServeMux()
	handler.HandleFunc("/.well-known/openid-configuration", func(writer http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(writer, "{\n  \"token_endpoint\": \"%s\"\n}\n", tokenEndpoint)
	})
	return httptest.NewServer(handler)
}

func mockKeyFileWithIssuer(issuerURL string) (string, error) {
	kf, err := os.CreateTemp("", "test_oauth2")
	if err != nil {
		return "", err
	}
	_, err = kf.WriteString(fmt.Sprintf(`{
  "type":"resource",
  "client_id":"client-id",
  "client_secret":"client-secret",
  "client_email":"oauth@test.org",
  "issuer_url":"%s"
}`, issuerURL))
	if err != nil {
		_ = kf.Close()
		return "", err
	}
	if err := kf.Close(); err != nil {
		return "", err
	}
	return kf.Name(), nil
}

func mockKeyFileWithoutIssuer() (string, error) {
	kf, err := os.CreateTemp("", "test_oauth2")
	if err != nil {
		return "", err
	}
	_, err = kf.WriteString(`{
  "type":"resource",
  "client_id":"client-id",
  "client_secret":"client-secret",
  "client_email":"oauth@test.org"
}`)
	if err != nil {
		_ = kf.Close()
		return "", err
	}
	if err := kf.Close(); err != nil {
		return "", err
	}
	return kf.Name(), nil
}

var _ = ginkgo.Describe("ClientCredentialsFlow", func() {
	ginkgo.Describe("Authorize", func() {

		var mockClock clock.Clock
		var mockTokenExchanger *MockTokenExchanger
		var mockGrantProvider *MockGrantProvider

		ginkgo.BeforeEach(func() {
			mockClock = testing.NewFakeClock(time.Unix(0, 0))
			expectedTokens := TokenResult{AccessToken: "accessToken", RefreshToken: "refreshToken", ExpiresIn: 1234}
			mockTokenExchanger = &MockTokenExchanger{
				ReturnsTokens: &expectedTokens,
			}
			mockGrantProvider = &MockGrantProvider{
				keyFile: &clientCredentials,
			}
		})

		ginkgo.It("invokes TokenExchanger with credentials", func() {
			provider := newClientCredentialsFlow(
				ClientCredentialsFlowOptions{
					KeyFile: "test_keyfile",
				},
				mockTokenExchanger,
				mockGrantProvider,
				mockClock,
			)

			_, err := provider.Authorize("test_audience")
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(mockTokenExchanger.CalledWithRequest).To(gomega.Equal(&ClientCredentialsExchangeRequest{
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				ClientID:      clientCredentials.ClientID,
				ClientSecret:  clientCredentials.ClientSecret,
				Audience:      "test_audience",
				Scopes:        []string{clientCredentials.Scope},
				AuthMethod:    TokenEndpointAuthMethodClientSecretPost,
			}))
		})

		ginkgo.It("passes tls client auth to the token exchanger without key file credentials", func() {
			provider := newClientCredentialsFlow(
				ClientCredentialsFlowOptions{
					ClientID:                defaultTLSClientID,
					TokenEndpointAuthMethod: TokenEndpointAuthMethodTLSClientAuth,
				},
				mockTokenExchanger,
				&StaticGrantProvider{
					Grant: &AuthorizationGrant{
						Type:          GrantTypeClientCredentials,
						Audience:      "test_audience",
						ClientID:      defaultTLSClientID,
						TokenEndpoint: oidcEndpoints.TokenEndpoint,
						Scopes:        []string{"scope1", "scope2"},
					},
				},
				mockClock,
			)

			_, err := provider.Authorize("test_audience")
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(mockTokenExchanger.CalledWithRequest).To(gomega.Equal(&ClientCredentialsExchangeRequest{
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				ClientID:      defaultTLSClientID,
				ClientSecret:  "",
				Audience:      "test_audience",
				Scopes:        []string{"scope1", "scope2"},
				AuthMethod:    TokenEndpointAuthMethodTLSClientAuth,
			}))
		})

		ginkgo.It("uses the default client id for tls client auth when options client id is unset", func() {
			tokenEndpoint := "https://options.example/token"
			serverFromOptions := mockWellKnownServer(tokenEndpoint)
			defer serverFromOptions.Close()

			provider := newClientCredentialsFlow(
				ClientCredentialsFlowOptions{
					IssuerURL:               serverFromOptions.URL,
					AdditionalScopes:        []string{"scope1", "scope2"},
					TokenEndpointAuthMethod: TokenEndpointAuthMethodTLSClientAuth,
				},
				mockTokenExchanger,
				&DefaultGrantProvider{oidcClient: serverFromOptions.Client()},
				mockClock,
			)

			_, err := provider.Authorize("test_audience")
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(mockTokenExchanger.CalledWithRequest).To(gomega.Equal(&ClientCredentialsExchangeRequest{
				TokenEndpoint: tokenEndpoint,
				ClientID:      defaultTLSClientID,
				ClientSecret:  "",
				Audience:      "test_audience",
				Scopes:        []string{"scope1", "scope2"},
				AuthMethod:    TokenEndpointAuthMethodTLSClientAuth,
			}))
		})

		ginkgo.It("returns TokensResult from TokenExchanger", func() {
			provider := newClientCredentialsFlow(
				ClientCredentialsFlowOptions{
					KeyFile: "test_keyfile",
				},
				mockTokenExchanger,
				mockGrantProvider,
				mockClock,
			)

			grant, err := provider.Authorize("test_audience")
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			expected := convertToOAuth2Token(mockTokenExchanger.ReturnsTokens, mockClock)
			gomega.Expect(*grant.Token).To(gomega.Equal(expected))
		})

		ginkgo.It("returns an error if token exchanger errors", func() {
			mockTokenExchanger.ReturnsError = errors.New("someerror")
			mockTokenExchanger.ReturnsTokens = nil

			provider := newClientCredentialsFlow(
				ClientCredentialsFlowOptions{
					KeyFile: "test_keyfile",
				},
				mockTokenExchanger,
				mockGrantProvider,
				mockClock,
			)

			_, err := provider.Authorize("test_audience")
			gomega.Expect(err.Error()).To(gomega.Equal("authentication failed using client credentials: " +
				"could not exchange client credentials: someerror"))
		})
	})
})

var _ = ginkgo.Describe("DefaultGrantProvider", func() {
	ginkgo.It("prefers issuer url from options over key file", func() {
		keyFileTokenEndpoint := "http://keyfile.example/token"
		optionsTokenEndpoint := "http://options.example/token"
		serverFromKeyFile := mockWellKnownServer(keyFileTokenEndpoint)
		defer serverFromKeyFile.Close()
		serverFromOptions := mockWellKnownServer(optionsTokenEndpoint)
		defer serverFromOptions.Close()

		keyFile, err := mockKeyFileWithIssuer(serverFromKeyFile.URL)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		defer os.Remove(keyFile)

		provider := DefaultGrantProvider{}
		grant, err := provider.GetGrant("test-audience", &ClientCredentialsFlowOptions{
			KeyFile:   keyFile,
			IssuerURL: serverFromOptions.URL,
		})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(grant.TokenEndpoint).To(gomega.Equal(optionsTokenEndpoint))
	})

	ginkgo.It("returns an error when issuer url is missing", func() {
		keyFile, err := mockKeyFileWithoutIssuer()
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		defer os.Remove(keyFile)

		provider := DefaultGrantProvider{}
		_, err = provider.GetGrant("test-audience", &ClientCredentialsFlowOptions{
			KeyFile: keyFile,
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.Equal("issuer url is required for client credentials flow"))
	})

	ginkgo.It("builds tls client auth grant without reading key file", func() {
		tokenEndpoint := "https://options.example/token"
		serverFromOptions := mockWellKnownServer(tokenEndpoint)
		defer serverFromOptions.Close()

		provider := DefaultGrantProvider{}
		grant, err := provider.GetGrant("test-audience", &ClientCredentialsFlowOptions{
			KeyFile:                 "/not-exist-key.json",
			ClientID:                "explicit-client-id",
			IssuerURL:               serverFromOptions.URL,
			AdditionalScopes:        []string{"scope1", "scope2"},
			TokenEndpointAuthMethod: TokenEndpointAuthMethodTLSClientAuth,
		})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(grant.ClientID).To(gomega.Equal("explicit-client-id"))
		gomega.Expect(grant.ClientCredentials).To(gomega.BeNil())
		gomega.Expect(grant.Scopes).To(gomega.Equal([]string{"scope1", "scope2"}))
		gomega.Expect(grant.TokenEndpoint).To(gomega.Equal(tokenEndpoint))
	})

	ginkgo.It("uses the default client id for tls client auth when unset", func() {
		tokenEndpoint := "https://options.example/token"
		serverFromOptions := mockWellKnownServer(tokenEndpoint)
		defer serverFromOptions.Close()

		provider := DefaultGrantProvider{}
		grant, err := provider.GetGrant("test-audience", &ClientCredentialsFlowOptions{
			IssuerURL:               serverFromOptions.URL,
			TokenEndpointAuthMethod: TokenEndpointAuthMethodTLSClientAuth,
		})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(grant.ClientID).To(gomega.Equal(defaultTLSClientID))
	})
})

var _ = ginkgo.Describe("NewDefaultClientCredentialsFlow", func() {
	ginkgo.It("uses client_secret_post by default", func() {
		flow, err := NewDefaultClientCredentialsFlow(ClientCredentialsFlowOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(flow.options.TokenEndpointAuthMethod).To(gomega.Equal(TokenEndpointAuthMethodClientSecretPost))
	})

	ginkgo.It("returns an error for an unsupported auth method", func() {
		_, err := NewDefaultClientCredentialsFlow(ClientCredentialsFlowOptions{
			TokenEndpointAuthMethod: "unsupported",
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.Equal("unsupported token endpoint auth method: unsupported"))
	})

	ginkgo.It("returns an error when tls client auth cert files are missing", func() {
		_, err := NewDefaultClientCredentialsFlow(ClientCredentialsFlowOptions{
			IssuerURL:               "https://issuer.example",
			TokenEndpointAuthMethod: TokenEndpointAuthMethodTLSClientAuth,
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.Equal("tlsCertFile and tlsKeyFile are required for tls_client_auth"))
	})

	ginkgo.It("allows tls client auth flow creation when issuer url is missing", func() {
		flow, err := NewDefaultClientCredentialsFlow(ClientCredentialsFlowOptions{
			TokenEndpointAuthMethod: TokenEndpointAuthMethodTLSClientAuth,
			TLSCertFile:             oauth2TestCertPath("client-cert.pem"),
			TLSKeyFile:              oauth2TestCertPath("client-key.pem"),
		})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(flow).ToNot(gomega.BeNil())
	})

	ginkgo.It("allows client_secret_post without cert files", func() {
		flow, err := NewDefaultClientCredentialsFlow(ClientCredentialsFlowOptions{
			TokenEndpointAuthMethod: TokenEndpointAuthMethodClientSecretPost,
		})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(flow).ToNot(gomega.BeNil())
	})

	ginkgo.It("configures client certificate for client_secret_post when cert files are provided", func() {
		flow, err := NewDefaultClientCredentialsFlow(ClientCredentialsFlowOptions{
			TokenEndpointAuthMethod: TokenEndpointAuthMethodClientSecretPost,
			TLSCertFile:             oauth2TestCertPath("client-cert.pem"),
			TLSKeyFile:              oauth2TestCertPath("client-key.pem"),
		})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		retriever, ok := flow.exchanger.(*TokenRetriever)
		gomega.Expect(ok).To(gomega.BeTrue())
		httpClient, ok := retriever.transport.(*http.Client)
		gomega.Expect(ok).To(gomega.BeTrue())
		transport, ok := httpClient.Transport.(*http.Transport)
		gomega.Expect(ok).To(gomega.BeTrue())
		gomega.Expect(transport.TLSClientConfig).ToNot(gomega.BeNil())
		gomega.Expect(transport.TLSClientConfig.GetClientCertificate).ToNot(gomega.BeNil())
	})

	ginkgo.It("returns an error when only one cert file is provided for client_secret_post", func() {
		_, err := NewDefaultClientCredentialsFlow(ClientCredentialsFlowOptions{
			TokenEndpointAuthMethod: TokenEndpointAuthMethodClientSecretPost,
			TLSCertFile:             oauth2TestCertPath("client-cert.pem"),
		})
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.Equal("tlsCertFile and tlsKeyFile must be specified together"))
	})

	ginkgo.It("configures root CAs when trustCertsFilePath is provided", func() {
		flow, err := NewDefaultClientCredentialsFlow(ClientCredentialsFlowOptions{
			TrustCertsFilePath: oauth2TestCertPath("cacert.pem"),
		})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		retriever, ok := flow.exchanger.(*TokenRetriever)
		gomega.Expect(ok).To(gomega.BeTrue())
		httpClient, ok := retriever.transport.(*http.Client)
		gomega.Expect(ok).To(gomega.BeTrue())
		transport, ok := httpClient.Transport.(*http.Transport)
		gomega.Expect(ok).To(gomega.BeTrue())
		gomega.Expect(transport.TLSClientConfig).ToNot(gomega.BeNil())
		gomega.Expect(transport.TLSClientConfig.RootCAs).ToNot(gomega.BeNil())
	})

	ginkgo.It("allows tls client auth without a key file", func() {
		flow, err := NewDefaultClientCredentialsFlow(ClientCredentialsFlowOptions{
			IssuerURL:               "https://issuer.example",
			TokenEndpointAuthMethod: TokenEndpointAuthMethodTLSClientAuth,
			TLSCertFile:             oauth2TestCertPath("client-cert.pem"),
			TLSKeyFile:              oauth2TestCertPath("client-key.pem"),
		})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(flow).ToNot(gomega.BeNil())
	})
})

var _ = ginkgo.Describe("ClientCredentialsGrantRefresher", func() {
	ginkgo.Describe("NewDefaultClientCredentialsGrantRefresher", func() {
		var mockClock clock.Clock

		ginkgo.BeforeEach(func() {
			mockClock = testing.NewFakeClock(time.Unix(0, 0))
		})

		ginkgo.It("accepts tls client auth", func() {
			refresher, err := NewDefaultClientCredentialsGrantRefresher(mockClock, ClientCredentialsFlowOptions{
				TokenEndpointAuthMethod: TokenEndpointAuthMethodTLSClientAuth,
				TLSCertFile:             oauth2TestCertPath("client-cert.pem"),
				TLSKeyFile:              oauth2TestCertPath("client-key.pem"),
			})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(refresher.authMethod).To(gomega.Equal(TokenEndpointAuthMethodTLSClientAuth))

			retriever, ok := refresher.exchanger.(*TokenRetriever)
			gomega.Expect(ok).To(gomega.BeTrue())
			httpClient, ok := retriever.transport.(*http.Client)
			gomega.Expect(ok).To(gomega.BeTrue())
			transport, ok := httpClient.Transport.(*http.Transport)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(transport.TLSClientConfig).ToNot(gomega.BeNil())
			gomega.Expect(transport.TLSClientConfig.GetClientCertificate).ToNot(gomega.BeNil())
		})

		ginkgo.It("configures root CAs from options", func() {
			refresher, err := NewDefaultClientCredentialsGrantRefresher(mockClock, ClientCredentialsFlowOptions{
				TrustCertsFilePath: oauth2TestCertPath("cacert.pem"),
			})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())

			retriever, ok := refresher.exchanger.(*TokenRetriever)
			gomega.Expect(ok).To(gomega.BeTrue())
			httpClient, ok := retriever.transport.(*http.Client)
			gomega.Expect(ok).To(gomega.BeTrue())
			transport, ok := httpClient.Transport.(*http.Transport)
			gomega.Expect(ok).To(gomega.BeTrue())
			gomega.Expect(transport.TLSClientConfig).ToNot(gomega.BeNil())
			gomega.Expect(transport.TLSClientConfig.RootCAs).ToNot(gomega.BeNil())
		})
	})

	ginkgo.Describe("Refresh", func() {
		var mockClock clock.Clock
		var mockTokenExchanger *MockTokenExchanger

		ginkgo.BeforeEach(func() {
			mockClock = testing.NewFakeClock(time.Unix(0, 0))
			expectedTokens := TokenResult{AccessToken: "accessToken", RefreshToken: "refreshToken", ExpiresIn: 1234}
			mockTokenExchanger = &MockTokenExchanger{
				ReturnsTokens: &expectedTokens,
			}
		})

		ginkgo.It("invokes TokenExchanger with credentials", func() {
			refresher := &ClientCredentialsGrantRefresher{
				clock:     mockClock,
				exchanger: mockTokenExchanger,
			}
			og := &AuthorizationGrant{
				Type:              GrantTypeClientCredentials,
				Audience:          "test_audience",
				ClientCredentials: &clientCredentials,
				TokenEndpoint:     oidcEndpoints.TokenEndpoint,
				Token:             nil,
				Scopes:            []string{"profile"},
			}
			_, err := refresher.Refresh(og)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(mockTokenExchanger.CalledWithRequest).To(gomega.Equal(&ClientCredentialsExchangeRequest{
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				ClientID:      clientCredentials.ClientID,
				ClientSecret:  clientCredentials.ClientSecret,
				Audience:      og.Audience,
				Scopes:        og.Scopes,
				AuthMethod:    TokenEndpointAuthMethodClientSecretPost,
			}))
		})

		ginkgo.It("returns a valid grant", func() {
			refresher := &ClientCredentialsGrantRefresher{
				clock:     mockClock,
				exchanger: mockTokenExchanger,
			}
			og := &AuthorizationGrant{
				Type:              GrantTypeClientCredentials,
				Audience:          "test_audience",
				ClientCredentials: &clientCredentials,
				TokenEndpoint:     oidcEndpoints.TokenEndpoint,
				Token:             nil,
				Scopes:            []string{"profile"},
			}
			ng, err := refresher.Refresh(og)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(ng.Audience).To(gomega.Equal("test_audience"))
			gomega.Expect(ng.ClientID).To(gomega.Equal(""))
			gomega.Expect(*ng.ClientCredentials).To(gomega.Equal(clientCredentials))
			gomega.Expect(ng.TokenEndpoint).To(gomega.Equal(oidcEndpoints.TokenEndpoint))
			expected := convertToOAuth2Token(mockTokenExchanger.ReturnsTokens, mockClock)
			gomega.Expect(*ng.Token).To(gomega.Equal(expected))
			gomega.Expect(ng.Scopes).To(gomega.Equal([]string{"profile"}))
		})

		ginkgo.It("passes tls client auth during refresh", func() {
			refresher := &ClientCredentialsGrantRefresher{
				clock:      mockClock,
				exchanger:  mockTokenExchanger,
				authMethod: TokenEndpointAuthMethodTLSClientAuth,
			}
			og := &AuthorizationGrant{
				Type:          GrantTypeClientCredentials,
				Audience:      "test_audience",
				ClientID:      defaultTLSClientID,
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				Token:         nil,
				Scopes:        []string{"profile"},
			}
			_, err := refresher.Refresh(og)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(mockTokenExchanger.CalledWithRequest).To(gomega.Equal(&ClientCredentialsExchangeRequest{
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				ClientID:      defaultTLSClientID,
				ClientSecret:  "",
				Audience:      og.Audience,
				Scopes:        og.Scopes,
				AuthMethod:    TokenEndpointAuthMethodTLSClientAuth,
			}))
		})

		ginkgo.It("infers tls client auth when grant credentials are nil", func() {
			refresher := &ClientCredentialsGrantRefresher{
				clock:     mockClock,
				exchanger: mockTokenExchanger,
			}
			og := &AuthorizationGrant{
				Type:          GrantTypeClientCredentials,
				Audience:      "test_audience",
				ClientID:      defaultTLSClientID,
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				Scopes:        []string{"profile"},
			}
			_, err := refresher.Refresh(og)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(mockTokenExchanger.CalledWithRequest).To(gomega.Equal(&ClientCredentialsExchangeRequest{
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				ClientID:      defaultTLSClientID,
				ClientSecret:  "",
				Audience:      og.Audience,
				Scopes:        og.Scopes,
				AuthMethod:    TokenEndpointAuthMethodTLSClientAuth,
			}))
		})
	})
})
