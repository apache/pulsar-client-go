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
	"golang.org/x/oauth2"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

type MockDeviceCodeProvider struct {
	Called                     bool
	CalledWithAudience         string
	CalledWithAdditionalScopes []string
	DeviceCodeResult           *DeviceCodeResult
	ReturnsError               error
}

func (cp *MockDeviceCodeProvider) GetCode(audience string, additionalScopes ...string) (*DeviceCodeResult, error) {
	cp.Called = true
	cp.CalledWithAudience = audience
	cp.CalledWithAdditionalScopes = additionalScopes
	return cp.DeviceCodeResult, cp.ReturnsError
}

type MockDeviceCodeCallback struct {
	Called           bool
	DeviceCodeResult *DeviceCodeResult
	ReturnsError     error
}

func (c *MockDeviceCodeCallback) Callback(code *DeviceCodeResult) error {
	c.Called = true
	c.DeviceCodeResult = code
	if c.ReturnsError != nil {
		return c.ReturnsError
	}
	return nil
}

var _ = ginkgo.Describe("DeviceCodeFlow", func() {

	ginkgo.Describe("Authorize", func() {
		const audience = "test_clientID"

		var mockClock clock.Clock
		var mockCodeProvider *MockDeviceCodeProvider
		var mockTokenExchanger *MockTokenExchanger
		var mockCallback *MockDeviceCodeCallback
		var flow *DeviceCodeFlow

		ginkgo.BeforeEach(func() {
			mockClock = testing.NewFakeClock(time.Unix(0, 0))

			mockCodeProvider = &MockDeviceCodeProvider{
				DeviceCodeResult: &DeviceCodeResult{
					DeviceCode:              "test_deviceCode",
					UserCode:                "test_userCode",
					VerificationURI:         "http://verification_uri",
					VerificationURIComplete: "http://verification_uri_complete",
					ExpiresIn:               10,
					Interval:                5,
				},
			}

			expectedTokens := TokenResult{AccessToken: "accessToken", RefreshToken: "refreshToken", ExpiresIn: 1234}
			mockTokenExchanger = &MockTokenExchanger{
				ReturnsTokens: &expectedTokens,
			}

			mockCallback = &MockDeviceCodeCallback{}

			opts := DeviceCodeFlowOptions{
				IssuerEndpoint:   "http://issuer",
				ClientID:         "test_clientID",
				AdditionalScopes: nil,
				AllowRefresh:     true,
			}
			flow = newDeviceCodeFlow(
				opts,
				oidcEndpoints,
				mockCodeProvider,
				mockTokenExchanger,
				mockCallback.Callback,
				mockClock,
			)
		})

		ginkgo.It("invokes DeviceCodeProvider", func() {
			_, _ = flow.Authorize(audience)
			gomega.Expect(mockCodeProvider.Called).To(gomega.BeTrue())
			gomega.Expect(mockCodeProvider.CalledWithAdditionalScopes).To(gomega.ContainElement("offline_access"))
		})

		ginkgo.It("invokes callback with returned code", func() {
			_, _ = flow.Authorize(audience)
			gomega.Expect(mockCallback.Called).To(gomega.BeTrue())
			gomega.Expect(mockCallback.DeviceCodeResult).To(gomega.Equal(mockCodeProvider.DeviceCodeResult))
		})

		ginkgo.It("invokes TokenExchanger with returned code", func() {
			_, _ = flow.Authorize(audience)
			gomega.Expect(mockTokenExchanger.CalledWithRequest).To(gomega.Equal(&DeviceCodeExchangeRequest{
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				ClientID:      "test_clientID",
				PollInterval:  time.Duration(5) * time.Second,
				DeviceCode:    "test_deviceCode",
			}))
		})

		ginkgo.It("returns an authorization grant", func() {
			grant, _ := flow.Authorize(audience)
			gomega.Expect(grant).ToNot(gomega.BeNil())
			gomega.Expect(grant.Audience).To(gomega.Equal(audience))
			gomega.Expect(grant.ClientID).To(gomega.Equal("test_clientID"))
			gomega.Expect(grant.ClientCredentials).To(gomega.BeNil())
			gomega.Expect(grant.TokenEndpoint).To(gomega.Equal(oidcEndpoints.TokenEndpoint))
			expected := convertToOAuth2Token(mockTokenExchanger.ReturnsTokens, mockClock)
			gomega.Expect(*grant.Token).To(gomega.Equal(expected))
		})
	})
})

var _ = ginkgo.Describe("DeviceAuthorizationGrantRefresher", func() {

	ginkgo.Describe("Refresh", func() {
		var mockClock clock.Clock
		var mockTokenExchanger *MockTokenExchanger
		var refresher *DeviceAuthorizationGrantRefresher
		var grant *AuthorizationGrant

		ginkgo.BeforeEach(func() {
			mockClock = testing.NewFakeClock(time.Unix(0, 0))

			mockTokenExchanger = &MockTokenExchanger{}

			refresher = &DeviceAuthorizationGrantRefresher{
				exchanger: mockTokenExchanger,
				clock:     mockClock,
			}

			token := oauth2.Token{AccessToken: "gat", RefreshToken: "grt", Expiry: time.Unix(1, 0)}
			grant = &AuthorizationGrant{
				Type:          GrantTypeDeviceCode,
				ClientID:      "test_clientID",
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				Token:         &token,
			}
		})

		ginkgo.It("invokes the token exchanger", func() {
			mockTokenExchanger.ReturnsTokens = &TokenResult{
				AccessToken: "new token",
			}

			_, _ = refresher.Refresh(grant)
			gomega.Expect(*mockTokenExchanger.RefreshCalledWithRequest).To(gomega.Equal(RefreshTokenExchangeRequest{
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				ClientID:      grant.ClientID,
				RefreshToken:  "grt",
			}))
		})

		ginkgo.It("returns the refreshed access token from the TokenExchanger", func() {
			mockTokenExchanger.ReturnsTokens = &TokenResult{
				AccessToken: "new token",
			}

			grant, _ = refresher.Refresh(grant)
			gomega.Expect(grant.Token.AccessToken).To(gomega.Equal(mockTokenExchanger.ReturnsTokens.AccessToken))
		})

		ginkgo.It("preserves the existing refresh token from the TokenExchanger", func() {
			mockTokenExchanger.ReturnsTokens = &TokenResult{
				AccessToken: "new token",
			}

			grant, _ = refresher.Refresh(grant)
			gomega.Expect(grant.Token.RefreshToken).To(gomega.Equal("grt"))
		})

		ginkgo.It("returns the refreshed refresh token from the TokenExchanger", func() {
			mockTokenExchanger.ReturnsTokens = &TokenResult{
				AccessToken:  "new token",
				RefreshToken: "new token",
			}

			grant, _ = refresher.Refresh(grant)
			gomega.Expect(grant.Token.RefreshToken).To(gomega.Equal("new token"))
		})

		ginkgo.It("returns a meaningful expiration time", func() {
			mockTokenExchanger.ReturnsTokens = &TokenResult{
				AccessToken: "new token",
				ExpiresIn:   60,
			}

			grant, _ = refresher.Refresh(grant)
			gomega.Expect(grant.Token.Expiry).To(gomega.Equal(mockClock.Now().Add(time.Duration(60) * time.Second)))
		})

		ginkgo.It("returns an error when TokenExchanger does", func() {
			mockTokenExchanger.ReturnsError = errors.New("someerror")

			_, err := refresher.Refresh(grant)
			gomega.Expect(err.Error()).To(gomega.Equal("could not exchange refresh token: someerror"))
		})
	})
})
