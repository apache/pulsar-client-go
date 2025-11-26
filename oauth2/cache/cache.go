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

package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/oauth2"
	"github.com/apache/pulsar-client-go/oauth2/clock"
	xoauth2 "golang.org/x/oauth2"
)

// A CachingTokenSource is anything that can return a token, and is backed by a cache.
type CachingTokenSource interface {
	xoauth2.TokenSource

	// InvalidateToken is called when the token is rejected by the resource server.
	InvalidateToken() error
}

const (
	// expiryDelta adjusts the token TTL to avoid using tokens which are almost expired
	expiryDelta = time.Duration(60) * time.Second
)

// tokenCache implements a cache for the token associated with a specific audience.
// it is advisable to use a token cache instance per audience.
type tokenCache struct {
	clock    clock.Clock
	lock     sync.Mutex
	audience string
	token    *xoauth2.Token
	flow     *oauth2.ClientCredentialsFlow
}

func NewDefaultTokenCache(audience string,
	flow *oauth2.ClientCredentialsFlow) (CachingTokenSource, error) {
	if flow == nil {
		return nil, fmt.Errorf("flow cannot be nil")
	}
	cache := &tokenCache{
		clock:    clock.RealClock{},
		audience: audience,
		flow:     flow,
	}
	return cache, nil
}

var _ CachingTokenSource = &tokenCache{}

// Token returns a valid access token, if available.
func (t *tokenCache) Token() (*xoauth2.Token, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// use the cached access token if it isn't expired
	if t.token != nil && t.validateAccessToken(*t.token) {
		return t.token, nil
	}

	grant, err := t.flow.Authorize(t.audience)
	if err != nil {
		return nil, err
	}
	if grant.Token == nil {
		return nil, fmt.Errorf("authorization succeeded but no token was returned")
	}
	t.token = grant.Token

	return t.token, nil
}

// InvalidateToken clears the cached access token (likely due to a response from the resource server).
func (t *tokenCache) InvalidateToken() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.token = nil
	return nil
}

// validateAccessToken checks the validity of the cached access token
func (t *tokenCache) validateAccessToken(token xoauth2.Token) bool {
	if token.AccessToken == "" {
		return false
	}
	if !token.Expiry.IsZero() && t.clock.Now().After(token.Expiry.Round(0).Add(-expiryDelta)) {
		return false
	}
	return true
}
