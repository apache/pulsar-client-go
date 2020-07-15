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
	"golang.org/x/oauth2"
	"k8s.io/utils/clock/testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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

var _ = Describe("DeviceCodeFlow", func() {

	Describe("Authorize", func() {
		const audience = "test_clientID"

		var mockClock clock.Clock
		var mockCodeProvider *MockDeviceCodeProvider
		var mockTokenExchanger *MockTokenExchanger
		var mockCallback *MockDeviceCodeCallback
		var flow *DeviceCodeFlow

		BeforeEach(func() {
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

		It("invokes DeviceCodeProvider", func() {
			_, _ = flow.Authorize(audience)
			Expect(mockCodeProvider.Called).To(BeTrue())
			Expect(mockCodeProvider.CalledWithAdditionalScopes).To(ContainElement("offline_access"))
		})

		It("invokes callback with returned code", func() {
			_, _ = flow.Authorize(audience)
			Expect(mockCallback.Called).To(BeTrue())
			Expect(mockCallback.DeviceCodeResult).To(Equal(mockCodeProvider.DeviceCodeResult))
		})

		It("invokes TokenExchanger with returned code", func() {
			_, _ = flow.Authorize(audience)
			Expect(mockTokenExchanger.CalledWithRequest).To(Equal(&DeviceCodeExchangeRequest{
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				ClientID:      "test_clientID",
				PollInterval:  time.Duration(5) * time.Second,
				DeviceCode:    "test_deviceCode",
			}))
		})

		It("returns an authorization grant", func() {
			grant, _ := flow.Authorize(audience)
			Expect(grant).ToNot(BeNil())
			Expect(grant.Audience).To(Equal(audience))
			Expect(grant.ClientID).To(Equal("test_clientID"))
			Expect(grant.ClientCredentials).To(BeNil())
			Expect(grant.TokenEndpoint).To(Equal(oidcEndpoints.TokenEndpoint))
			expected := convertToOAuth2Token(mockTokenExchanger.ReturnsTokens, mockClock)
			Expect(*grant.Token).To(Equal(expected))
		})
	})
})

var _ = Describe("DeviceAuthorizationGrantRefresher", func() {

	Describe("Refresh", func() {
		var mockClock clock.Clock
		var mockTokenExchanger *MockTokenExchanger
		var refresher *DeviceAuthorizationGrantRefresher
		var grant *AuthorizationGrant

		BeforeEach(func() {
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

		It("invokes the token exchanger", func() {
			mockTokenExchanger.ReturnsTokens = &TokenResult{
				AccessToken: "new token",
			}

			_, _ = refresher.Refresh(grant)
			Expect(*mockTokenExchanger.RefreshCalledWithRequest).To(Equal(RefreshTokenExchangeRequest{
				TokenEndpoint: oidcEndpoints.TokenEndpoint,
				ClientID:      grant.ClientID,
				RefreshToken:  "grt",
			}))
		})

		It("returns the refreshed access token from the TokenExchanger", func() {
			mockTokenExchanger.ReturnsTokens = &TokenResult{
				AccessToken: "new token",
			}

			grant, _ = refresher.Refresh(grant)
			Expect(grant.Token.AccessToken).To(Equal(mockTokenExchanger.ReturnsTokens.AccessToken))
		})

		It("preserves the existing refresh token from the TokenExchanger", func() {
			mockTokenExchanger.ReturnsTokens = &TokenResult{
				AccessToken: "new token",
			}

			grant, _ = refresher.Refresh(grant)
			Expect(grant.Token.RefreshToken).To(Equal("grt"))
		})

		It("returns the refreshed refresh token from the TokenExchanger", func() {
			mockTokenExchanger.ReturnsTokens = &TokenResult{
				AccessToken:  "new token",
				RefreshToken: "new token",
			}

			grant, _ = refresher.Refresh(grant)
			Expect(grant.Token.RefreshToken).To(Equal("new token"))
		})

		It("returns a meaningful expiration time", func() {
			mockTokenExchanger.ReturnsTokens = &TokenResult{
				AccessToken: "new token",
				ExpiresIn:   60,
			}

			grant, _ = refresher.Refresh(grant)
			Expect(grant.Token.Expiry).To(Equal(mockClock.Now().Add(time.Duration(60) * time.Second)))
		})

		It("returns an error when TokenExchanger does", func() {
			mockTokenExchanger.ReturnsError = errors.New("someerror")

			_, err := refresher.Refresh(grant)
			Expect(err.Error()).To(Equal("could not exchange refresh token: someerror"))
		})
	})
})
