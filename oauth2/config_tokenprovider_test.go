// Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.

package auth

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type mockConfigProvider struct {
	ReturnAccessToken         string
	ReturnRefreshToken        string
	GetTokensCalledIdentifier string
	SavedIdentifier           string
	SavedAccessToken          string
	SavedRefreshToken         string
}

func (m *mockConfigProvider) GetTokens(identifier string) (string, string) {
	m.GetTokensCalledIdentifier = identifier
	return m.ReturnAccessToken, m.ReturnRefreshToken
}

func (m *mockConfigProvider) SaveTokens(identifier, accessToken, refreshToken string) {
	m.SavedIdentifier = identifier
	m.SavedAccessToken = accessToken
	m.SavedRefreshToken = refreshToken
}

var _ = Describe("main", func() {
	Describe("configCachingProvider", func() {
		It("sets up the identifier using the clientID and audience", func() {
			p := NewConfigBackedCachingProvider("iamclientid", "iamaudience", &mockConfigProvider{})

			Expect(p.identifier).To(Equal("iamclientid-iamaudience"))
		})

		It("gets tokens from the config provider", func() {
			c := &mockConfigProvider{
				ReturnAccessToken:  "accessToken",
				ReturnRefreshToken: "refreshToken",
			}
			p := ConfigBackedCachingProvider{
				identifier: "iamidentifier",
				config:     c,
			}

			r, err := p.GetTokens()

			Expect(err).NotTo(HaveOccurred())
			Expect(c.GetTokensCalledIdentifier).To(Equal(p.identifier))
			Expect(r).To(Equal(&TokenResult{
				AccessToken:  c.ReturnAccessToken,
				RefreshToken: c.ReturnRefreshToken,
			}))
		})

		It("caches the tokens in the config provider", func() {
			c := &mockConfigProvider{}
			p := ConfigBackedCachingProvider{
				identifier: "iamidentifier",
				config:     c,
			}
			toSave := &TokenResult{
				AccessToken:  "accessToken",
				RefreshToken: "refreshToken",
			}

			p.CacheTokens(toSave)

			Expect(c.SavedIdentifier).To(Equal(p.identifier))
			Expect(c.SavedAccessToken).To(Equal(toSave.AccessToken))
			Expect(c.SavedRefreshToken).To(Equal(toSave.RefreshToken))
		})
	})
})
