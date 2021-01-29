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

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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
}

var _ = Describe("ClientCredentialsFlow", func() {
	Describe("Authorize", func() {

		var mockClock clock.Clock
		var mockTokenExchanger *MockTokenExchanger

		BeforeEach(func() {
			mockClock = testing.NewFakeClock(time.Unix(0, 0))
			expectedTokens := TokenResult{AccessToken: "accessToken", RefreshToken: "refreshToken", ExpiresIn: 1234}
			mockTokenExchanger = &MockTokenExchanger{
				ReturnsTokens: &expectedTokens,
			}
		})

		It("invokes TokenExchanger with credentials", func() {
			provider := newClientCredentialsFlow(
				ClientCredentialsFlowOptions{
					KeyFile: "test_keyfile",
				},
				&clientCredentials,
				oidcEndpoints,
				mockTokenExchanger,
				mockClock,
			)

			_, err := provider.Authorize("test_audience")
			Expect(err).ToNot(HaveOccurred())
			Expect(mockTokenExchanger.CalledWithRequest).To(Equal(&ClientCredentialsExchangeRequest{
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				ClientID:      clientCredentials.ClientID,
				ClientSecret:  clientCredentials.ClientSecret,
				Audience:      "test_audience",
			}))
		})

		It("returns TokensResult from TokenExchanger", func() {
			provider := newClientCredentialsFlow(
				ClientCredentialsFlowOptions{
					KeyFile: "test_keyfile",
				},
				&clientCredentials,
				oidcEndpoints,
				mockTokenExchanger,
				mockClock,
			)

			grant, err := provider.Authorize("test_audience")
			Expect(err).ToNot(HaveOccurred())
			expected := convertToOAuth2Token(mockTokenExchanger.ReturnsTokens, mockClock)
			Expect(*grant.Token).To(Equal(expected))
		})

		It("returns an error if token exchanger errors", func() {
			mockTokenExchanger.ReturnsError = errors.New("someerror")
			mockTokenExchanger.ReturnsTokens = nil

			provider := newClientCredentialsFlow(
				ClientCredentialsFlowOptions{
					KeyFile: "test_keyfile",
				},
				&clientCredentials,
				oidcEndpoints,
				mockTokenExchanger,
				mockClock,
			)

			_, err := provider.Authorize("test_audience")
			Expect(err.Error()).To(Equal("authentication failed using client credentials: " +
				"could not exchange client credentials: someerror"))
		})
	})
})

var _ = Describe("ClientCredentialsGrantRefresher", func() {

	Describe("Refresh", func() {
		var mockClock clock.Clock
		var mockTokenExchanger *MockTokenExchanger

		BeforeEach(func() {
			mockClock = testing.NewFakeClock(time.Unix(0, 0))
			expectedTokens := TokenResult{AccessToken: "accessToken", RefreshToken: "refreshToken", ExpiresIn: 1234}
			mockTokenExchanger = &MockTokenExchanger{
				ReturnsTokens: &expectedTokens,
			}
		})

		It("invokes TokenExchanger with credentials", func() {
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
			}
			_, err := refresher.Refresh(og)
			Expect(err).ToNot(HaveOccurred())
			Expect(mockTokenExchanger.CalledWithRequest).To(Equal(&ClientCredentialsExchangeRequest{
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				ClientID:      clientCredentials.ClientID,
				ClientSecret:  clientCredentials.ClientSecret,
				Audience:      og.Audience,
			}))
		})

		It("returns a valid grant", func() {
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
			}
			ng, err := refresher.Refresh(og)
			Expect(err).ToNot(HaveOccurred())
			Expect(ng.Audience).To(Equal("test_audience"))
			Expect(ng.ClientID).To(Equal(""))
			Expect(*ng.ClientCredentials).To(Equal(clientCredentials))
			Expect(ng.TokenEndpoint).To(Equal(oidcEndpoints.TokenEndpoint))
			expected := convertToOAuth2Token(mockTokenExchanger.ReturnsTokens, mockClock)
			Expect(*ng.Token).To(Equal(expected))
		})
	})
})
