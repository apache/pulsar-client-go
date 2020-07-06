// Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.

package auth

import (
	"fmt"
	"time"

	"github.com/dgrijalva/jwt-go"
	"golang.org/x/oauth2"
	"k8s.io/utils/clock"
)

const (
	ClaimNameUserName = "https://streamnative.io/username"
)

// Flow abstracts an OAuth 2.0 authentication and authorization flow
type Flow interface {
	// Authorize obtains an authorization grant based on an OAuth 2.0 authorization flow.
	// The method returns a grant which may contain an initial access token.
	Authorize() (*AuthorizationGrant, error)
}

// AuthorizationGrantRefresher refreshes OAuth 2.0 authorization grant
type AuthorizationGrantRefresher interface {
	// Refresh refreshes an authorization grant to contain a fresh access token
	Refresh(grant *AuthorizationGrant) (*AuthorizationGrant, error)
}

type AuthorizationGrantType string

const (
	// GrantTypeClientCredentials represents a client credentials grant
	GrantTypeClientCredentials AuthorizationGrantType = "client_credentials"

	// GrantTypeDeviceCode represents a device code grant
	GrantTypeDeviceCode AuthorizationGrantType = "device_code"
)

// AuthorizationGrant is a credential representing the resource owner's authorization
// to access its protected resources, and is used by the client to obtain an access token
type AuthorizationGrant struct {
	// Type describes the type of authorization grant represented by this structure
	Type AuthorizationGrantType `json:"type"`

	// ClientCredentials is credentials data for the client credentials grant type
	ClientCredentials *KeyFile `json:"client_credentials,omitempty"`

	// Token contains an access token in the client credentials grant type,
	// and a refresh token in the device authorization grant type
	Token *oauth2.Token `json:"token,omitempty"`
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

func convertToOAuth2Token(token *TokenResult, clock clock.Clock) oauth2.Token {
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
