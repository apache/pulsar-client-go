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
	"net/http"

	"github.com/apache/pulsar-client-go/oauth2/clock"

	"github.com/pkg/errors"
)

// ClientCredentialsFlow takes care of the mechanics needed for getting an access
// token using the OAuth 2.0 "Client Credentials Flow"
type ClientCredentialsFlow struct {
	issuerData Issuer
	provider   ClientCredentialsProvider
	exchanger  ClientCredentialsExchanger
	clock      clock.Clock
}

// ClientCredentialsProvider abstracts getting client credentials
type ClientCredentialsProvider interface {
	GetClientCredentials() (*KeyFile, error)
}

// ClientCredentialsExchanger abstracts exchanging client credentials for tokens
type ClientCredentialsExchanger interface {
	ExchangeClientCredentials(req ClientCredentialsExchangeRequest) (*TokenResult, error)
}

func NewClientCredentialsFlow(
	issuerData Issuer,
	provider ClientCredentialsProvider,
	exchanger ClientCredentialsExchanger,
	clock clock.Clock) *ClientCredentialsFlow {
	return &ClientCredentialsFlow{
		issuerData: issuerData,
		provider:   provider,
		exchanger:  exchanger,
		clock:      clock,
	}
}

// NewDefaultClientCredentialsFlow provides an easy way to build up a default
// client credentials flow with all the correct configuration.
func NewDefaultClientCredentialsFlow(issuerData Issuer, keyFile string) (*ClientCredentialsFlow, error) {
	wellKnownEndpoints, err := GetOIDCWellKnownEndpointsFromIssuerURL(issuerData.IssuerEndpoint)
	if err != nil {
		return nil, err
	}

	credsProvider := NewClientCredentialsProviderFromKeyFile(keyFile)

	tokenRetriever := NewTokenRetriever(
		*wellKnownEndpoints,
		&http.Client{})

	return NewClientCredentialsFlow(
		issuerData,
		credsProvider,
		tokenRetriever,
		clock.RealClock{}), nil
}

var _ Flow = &ClientCredentialsFlow{}

func (c *ClientCredentialsFlow) Authorize() (*AuthorizationGrant, error) {
	keyFile, err := c.provider.GetClientCredentials()
	if err != nil {
		return nil, errors.Wrap(err, "could not get client credentials")
	}
	grant := &AuthorizationGrant{
		Type:              GrantTypeClientCredentials,
		ClientCredentials: keyFile,
	}

	// test the credentials and obtain an initial access token
	refresher := &ClientCredentialsGrantRefresher{
		issuerData: c.issuerData,
		exchanger:  c.exchanger,
		clock:      c.clock,
	}
	grant, err = refresher.Refresh(grant)
	if err != nil {
		return nil, errors.Wrap(err, "authentication failed using client credentials")
	}
	return grant, nil
}

type ClientCredentialsGrantRefresher struct {
	issuerData Issuer
	exchanger  ClientCredentialsExchanger
	clock      clock.Clock
}

func NewDefaultClientCredentialsGrantRefresher(issuerData Issuer,
	clock clock.Clock) (*ClientCredentialsGrantRefresher, error) {
	wellKnownEndpoints, err := GetOIDCWellKnownEndpointsFromIssuerURL(issuerData.IssuerEndpoint)
	if err != nil {
		return nil, err
	}

	tokenRetriever := NewTokenRetriever(
		*wellKnownEndpoints,
		&http.Client{})

	return &ClientCredentialsGrantRefresher{
		issuerData: issuerData,
		exchanger:  tokenRetriever,
		clock:      clock,
	}, nil
}

var _ AuthorizationGrantRefresher = &ClientCredentialsGrantRefresher{}

func (g *ClientCredentialsGrantRefresher) Refresh(grant *AuthorizationGrant) (*AuthorizationGrant, error) {
	if grant.Type != GrantTypeClientCredentials {
		return nil, errors.New("unsupported grant type")
	}

	exchangeRequest := ClientCredentialsExchangeRequest{
		Audience:     g.issuerData.Audience,
		ClientID:     grant.ClientCredentials.ClientID,
		ClientSecret: grant.ClientCredentials.ClientSecret,
	}
	tr, err := g.exchanger.ExchangeClientCredentials(exchangeRequest)
	if err != nil {
		return nil, errors.Wrap(err, "could not exchange client credentials")
	}

	token := convertToOAuth2Token(tr, g.clock)
	grant = &AuthorizationGrant{
		Type:              GrantTypeClientCredentials,
		ClientCredentials: grant.ClientCredentials,
		Token:             &token,
	}
	return grant, nil
}
