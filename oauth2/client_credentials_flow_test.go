// Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.

package auth

import (
	"errors"
	"time"

	"k8s.io/utils/clock"
	"k8s.io/utils/clock/testing"

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
	Type:         KeyFileTypeServiceAccount,
	ClientID:     "test_clientID",
	ClientSecret: "test_clientSecret",
	ClientEmail:  "test_clientEmail",
}

var _ = Describe("ClientCredentialsFlow", func() {
	issuer := Issuer{
		IssuerEndpoint: "http://issuer",
		ClientID:       "",
		Audience:       "test_audience",
	}

	Describe("Authorize", func() {

		var mockClock clock.Clock
		var mockCredsProvider *MockClientCredentialsProvider
		var mockTokenExchanger *MockTokenExchanger

		BeforeEach(func() {
			mockClock = testing.NewFakeClock(time.Unix(0, 0))

			mockCredsProvider = &MockClientCredentialsProvider{
				ClientCredentialsResult: &clientCredentials,
			}

			expectedTokens := TokenResult{AccessToken: "accessToken", RefreshToken: "refreshToken", ExpiresIn: 1234}
			mockTokenExchanger = &MockTokenExchanger{
				ReturnsTokens: &expectedTokens,
			}
		})

		It("invokes TokenExchanger with credentials", func() {
			provider := NewClientCredentialsFlow(
				issuer,
				mockCredsProvider,
				mockTokenExchanger,
				mockClock,
			)

			_, err := provider.Authorize()
			Expect(err).ToNot(HaveOccurred())
			Expect(mockCredsProvider.Called).To(BeTrue())
			Expect(mockTokenExchanger.CalledWithRequest).To(Equal(&ClientCredentialsExchangeRequest{
				ClientID:     mockCredsProvider.ClientCredentialsResult.ClientID,
				ClientSecret: mockCredsProvider.ClientCredentialsResult.ClientSecret,
				Audience:     issuer.Audience,
			}))
		})

		It("returns TokensResult from TokenExchanger", func() {
			provider := NewClientCredentialsFlow(
				issuer,
				mockCredsProvider,
				mockTokenExchanger,
				mockClock,
			)

			grant, err := provider.Authorize()
			Expect(err).ToNot(HaveOccurred())
			expected := convertToOAuth2Token(mockTokenExchanger.ReturnsTokens, mockClock)
			Expect(*grant.Token).To(Equal(expected))
		})

		It("returns an error if client credentials request errors", func() {
			mockCredsProvider.ReturnsError = errors.New("someerror")

			provider := NewClientCredentialsFlow(
				issuer,
				mockCredsProvider,
				mockTokenExchanger,
				mockClock,
			)

			_, err := provider.Authorize()
			Expect(err.Error()).To(Equal("could not get client credentials: someerror"))
		})

		It("returns an error if token exchanger errors", func() {
			mockTokenExchanger.ReturnsError = errors.New("someerror")
			mockTokenExchanger.ReturnsTokens = nil

			provider := NewClientCredentialsFlow(
				issuer,
				mockCredsProvider,
				mockTokenExchanger,
				mockClock,
			)

			_, err := provider.Authorize()
			Expect(err.Error()).To(Equal("authentication failed using client credentials: " +
				"could not exchange client credentials: someerror"))
		})
	})
})

var _ = Describe("ClientCredentialsGrantRefresher", func() {
	issuer := Issuer{
		IssuerEndpoint: "http://issuer",
		ClientID:       "",
		Audience:       "test_audience",
	}

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
				issuerData: issuer,
				clock:      mockClock,
				exchanger:  mockTokenExchanger,
			}
			og := &AuthorizationGrant{
				Type:              GrantTypeClientCredentials,
				ClientCredentials: &clientCredentials,
				Token:             nil,
			}
			_, err := refresher.Refresh(og)
			Expect(err).ToNot(HaveOccurred())
			Expect(mockTokenExchanger.CalledWithRequest).To(Equal(&ClientCredentialsExchangeRequest{
				ClientID:     clientCredentials.ClientID,
				ClientSecret: clientCredentials.ClientSecret,
				Audience:     issuer.Audience,
			}))
		})

		It("returns a valid grant", func() {
			refresher := &ClientCredentialsGrantRefresher{
				issuerData: issuer,
				clock:      mockClock,
				exchanger:  mockTokenExchanger,
			}
			og := &AuthorizationGrant{
				Type:              GrantTypeClientCredentials,
				ClientCredentials: &clientCredentials,
				Token:             nil,
			}
			ng, err := refresher.Refresh(og)
			Expect(err).ToNot(HaveOccurred())
			expected := convertToOAuth2Token(mockTokenExchanger.ReturnsTokens, mockClock)
			Expect(*ng.Token).To(Equal(expected))
		})
	})
})
