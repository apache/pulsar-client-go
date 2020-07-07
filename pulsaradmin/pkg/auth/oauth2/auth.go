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
	"fmt"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/streamnative/pulsar-admin-go/pkg/auth/oauth2/plugin"
	"golang.org/x/oauth2"
)

const (
	ClaimNameUserName = "https://pulsar.apache.org/username"
)

// Flow abstracts an OAuth 2.0 authentication and authorization flow
type Flow interface {
	// Authorize obtains an authorization grant based on an OAuth 2.0 authorization flow.
	// The method returns a grant and (optionally) an initial access token.
	Authorize() (AuthorizationGrant, *oauth2.Token, error)
}

// AuthorizationGrant is a credential representing the resource owner's authorization
// to access its protected resources, used by the client to obtain an access token
type AuthorizationGrant interface {
	// Refresh obtains a fresh access token based on this grant
	Refresh() (*oauth2.Token, error)
}

// TokenResult holds token information
type TokenResult struct {
	AccessToken  string `json:"access_token"`
	IDToken      string `json:"id_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

// Issuer holds information about the issuer of tokens
type Issuer struct {
	IssuerEndpoint string
	ClientID       string
	Audience       string
}

func convertToOAuth2Token(token *TokenResult, clock plugin.Clock) oauth2.Token {
	return oauth2.Token{
		AccessToken:  token.AccessToken,
		TokenType:    "bearer",
		RefreshToken: token.RefreshToken,
		Expiry:       clock.Now().Add(time.Duration(token.ExpiresIn) * time.Second),
	}
}

// ExtractUserName extracts the username claim from an authorization grant
func ExtractUserName(token oauth2.Token) (string, error) {
	p := jwt.Parser{}
	claims := jwt.MapClaims{}
	if _, _, err := p.ParseUnverified(token.AccessToken, claims); err != nil {
		return "", fmt.Errorf("unable to decode the access token: %v", err)
	}
	username, ok := claims[ClaimNameUserName]
	if !ok {
		return "", fmt.Errorf("access token doesn't contain a username claim")
	}
	switch v := username.(type) {
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("access token contains an unsupported username claim")
	}
}
