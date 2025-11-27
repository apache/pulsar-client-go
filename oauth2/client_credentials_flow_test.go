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

var clientCredentials = KeyFile{
	Type:         "resource",
	ClientID:     "test_clientID",
	ClientSecret: "test_clientSecret",
	ClientEmail:  "test_clientEmail",
	IssuerURL:    "http://issuer",
	Scope:        "test_scope",
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

var _ = ginkgo.Describe("ClientCredentialsGrantRefresher", func() {

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
	})
})
