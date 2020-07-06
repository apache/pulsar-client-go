// Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.

package auth

import "fmt"

type configProvider interface {
	GetTokens(identifier string) (string, string)
	SaveTokens(identifier, accessToken, refreshToken string)
}

// ConfigBackedCachingProvider wraps a configProvider in order to conform to
// the cachingProvider interface
type ConfigBackedCachingProvider struct {
	identifier string
	config     configProvider
}

// NewConfigBackedCachingProvider builds and returns a CachingTokenProvider
// that utilizes a configProvider to cache tokens
func NewConfigBackedCachingProvider(clientID, audience string, config configProvider) *ConfigBackedCachingProvider {
	return &ConfigBackedCachingProvider{
		identifier: fmt.Sprintf("%s-%s", clientID, audience),
		config:     config,
	}
}

// GetTokens gets the tokens from the cache and returns them as a TokenResult
func (c *ConfigBackedCachingProvider) GetTokens() (*TokenResult, error) {
	accessToken, refreshToken := c.config.GetTokens(c.identifier)
	return &TokenResult{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

// CacheTokens caches the id and refresh token from TokenResult in the
// configProvider
func (c *ConfigBackedCachingProvider) CacheTokens(toCache *TokenResult) error {
	c.config.SaveTokens(c.identifier, toCache.AccessToken, toCache.RefreshToken)
	return nil
}
