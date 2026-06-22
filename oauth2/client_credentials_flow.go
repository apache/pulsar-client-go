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
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"strings"

	"github.com/apache/pulsar-client-go/oauth2/clock"

	"github.com/pkg/errors"
)

// ClientCredentialsFlow takes care of the mechanics needed for getting an access
// token using the OAuth 2.0 "Client Credentials Flow"
type ClientCredentialsFlow struct {
	options       ClientCredentialsFlowOptions
	exchanger     ClientCredentialsExchanger
	grantProvider GrantProvider
	clock         clock.Clock
}

// ClientCredentialsProvider abstracts getting client credentials
type ClientCredentialsProvider interface {
	GetClientCredentials() (*KeyFile, error)
}

// ClientCredentialsExchanger abstracts exchanging client credentials for tokens
type ClientCredentialsExchanger interface {
	ExchangeClientCredentials(req ClientCredentialsExchangeRequest) (*TokenResult, error)
}

// GrantProvider abstracts the creation of authorization grants from credentials
type GrantProvider interface {
	GetGrant(audience string, options *ClientCredentialsFlowOptions) (*AuthorizationGrant, error)
}

const (
	TokenEndpointAuthMethodClientSecretPost = "client_secret_post"
	TokenEndpointAuthMethodTLSClientAuth    = "tls_client_auth"
	defaultTLSClientID                      = "pulsar-client"
)

// ClientCredentialsFlowOptions configures OAuth 2.0 client credentials authentication.
//
// TokenEndpointAuthMethod defaults to client_secret_post.
// Required parameters:
//   - client_secret_post: KeyFile
//   - tls_client_auth: IssuerURL, TLSCertFile, TLSKeyFile
type ClientCredentialsFlowOptions struct {
	KeyFile                 string
	ClientID                string
	IssuerURL               string
	AdditionalScopes        []string
	TokenEndpointAuthMethod string
	TLSCertFile             string
	TLSKeyFile              string
	TrustCertsFilePath      string
}

// DefaultGrantProvider provides authorization grants by loading credentials from a key file
type DefaultGrantProvider struct {
	oidcClient *http.Client
}

// GetGrant creates an authorization grant by loading credentials from the key file and
// merging the scopes from both the options and the key file configuration
func (p *DefaultGrantProvider) GetGrant(audience string, options *ClientCredentialsFlowOptions) (
	*AuthorizationGrant, error) {
	if options == nil {
		return nil, errors.New("client credentials flow options cannot be nil")
	}

	authMethod, err := normalizeTokenEndpointAuthMethod(options.TokenEndpointAuthMethod)
	if err != nil {
		return nil, err
	}

	if authMethod == TokenEndpointAuthMethodTLSClientAuth {
		if options.IssuerURL == "" {
			return nil, errors.New("issuer url is required for client credentials flow")
		}

		wellKnownEndpoints, err := GetOIDCWellKnownEndpointsFromIssuerURLWithClient(options.IssuerURL, p.oidcClient)
		if err != nil {
			return nil, err
		}

		clientID := options.ClientID
		if clientID == "" {
			clientID = defaultTLSClientID
		}

		return &AuthorizationGrant{
			Type:          GrantTypeClientCredentials,
			Audience:      audience,
			ClientID:      clientID,
			TokenEndpoint: wellKnownEndpoints.TokenEndpoint,
			Scopes:        normalizeScopes(options.AdditionalScopes),
		}, nil
	}

	credsProvider := NewClientCredentialsProviderFromKeyFile(options.KeyFile)
	keyFile, err := credsProvider.GetClientCredentials()
	if err != nil {
		return nil, errors.Wrap(err, "could not get client credentials")
	}

	issuerURL := options.IssuerURL
	if issuerURL == "" {
		issuerURL = keyFile.IssuerURL
	}
	if issuerURL == "" {
		return nil, errors.New("issuer url is required for client credentials flow")
	}

	wellKnownEndpoints, err := GetOIDCWellKnownEndpointsFromIssuerURLWithClient(issuerURL, p.oidcClient)
	if err != nil {
		return nil, err
	}
	// Merge the scopes of the options AdditionalScopes with the scopes read from the keyFile config
	scopesToAdd := normalizeScopes(options.AdditionalScopes)

	if keyFile.Scope != "" {
		scopesSplit := strings.Fields(keyFile.Scope)
		scopesToAdd = append(scopesToAdd, scopesSplit...)
	}

	return &AuthorizationGrant{
		Type:              GrantTypeClientCredentials,
		Audience:          audience,
		ClientID:          keyFile.ClientID,
		ClientCredentials: keyFile,
		TokenEndpoint:     wellKnownEndpoints.TokenEndpoint,
		Scopes:            scopesToAdd,
	}, nil
}

func newClientCredentialsFlow(
	options ClientCredentialsFlowOptions,
	exchanger ClientCredentialsExchanger,
	grantProvider GrantProvider,
	clock clock.Clock) *ClientCredentialsFlow {
	return &ClientCredentialsFlow{
		options:       options,
		exchanger:     exchanger,
		grantProvider: grantProvider,
		clock:         clock,
	}
}

func normalizeScopes(scopes []string) []string {
	filtered := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		if scope != "" {
			filtered = append(filtered, scope)
		}
	}
	return filtered
}

func normalizeTokenEndpointAuthMethod(method string) (string, error) {
	if method == "" {
		return TokenEndpointAuthMethodClientSecretPost, nil
	}

	switch method {
	case TokenEndpointAuthMethodClientSecretPost, TokenEndpointAuthMethodTLSClientAuth:
		return method, nil
	default:
		return "", errors.Errorf("unsupported token endpoint auth method: %s", method)
	}
}

func newClientCredentialsHTTPClient(options ClientCredentialsFlowOptions) (*http.Client, error) {
	authMethod, err := normalizeTokenEndpointAuthMethod(options.TokenEndpointAuthMethod)
	if err != nil {
		return nil, err
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	} else {
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
	}

	if options.TrustCertsFilePath != "" {
		rootCA, err := os.ReadFile(options.TrustCertsFilePath)
		if err != nil {
			return nil, err
		}
		transport.TLSClientConfig.RootCAs = x509.NewCertPool()
		transport.TLSClientConfig.RootCAs.AppendCertsFromPEM(rootCA)
	}

	hasTLSCertFile := options.TLSCertFile != ""
	hasTLSKeyFile := options.TLSKeyFile != ""
	if hasTLSCertFile != hasTLSKeyFile {
		return nil, errors.New("tlsCertFile and tlsKeyFile must be specified together")
	}

	if authMethod == TokenEndpointAuthMethodTLSClientAuth && !hasTLSCertFile {
		return nil, errors.New("tlsCertFile and tlsKeyFile are required for tls_client_auth")
	}

	if hasTLSCertFile {
		if _, err := tls.LoadX509KeyPair(options.TLSCertFile, options.TLSKeyFile); err != nil {
			return nil, err
		}

		transport.TLSClientConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(options.TLSCertFile, options.TLSKeyFile)
			if err != nil {
				return nil, err
			}
			return &cert, nil
		}
	}

	return &http.Client{Transport: transport}, nil
}

// NewDefaultClientCredentialsFlow provides an easy way to build up a default
// client credentials flow with all the correct configuration.
func NewDefaultClientCredentialsFlow(options ClientCredentialsFlowOptions) (*ClientCredentialsFlow, error) {
	authMethod, err := normalizeTokenEndpointAuthMethod(options.TokenEndpointAuthMethod)
	if err != nil {
		return nil, err
	}
	options.TokenEndpointAuthMethod = authMethod

	httpClient, err := newClientCredentialsHTTPClient(options)
	if err != nil {
		return nil, err
	}
	tokenRetriever := NewTokenRetriever(httpClient)
	return newClientCredentialsFlow(
		options,
		tokenRetriever,
		&DefaultGrantProvider{oidcClient: httpClient},
		clock.RealClock{}), nil
}

var _ Flow = &ClientCredentialsFlow{}

func (c *ClientCredentialsFlow) Authorize(audience string) (*AuthorizationGrant, error) {
	grant, err := c.grantProvider.GetGrant(audience, &c.options)
	if err != nil {
		return nil, err
	}

	// test the credentials and obtain an initial access token
	refresher := &ClientCredentialsGrantRefresher{
		exchanger:  c.exchanger,
		clock:      c.clock,
		authMethod: c.options.TokenEndpointAuthMethod,
	}
	grant, err = refresher.Refresh(grant)
	if err != nil {
		return nil, errors.Wrap(err, "authentication failed using client credentials")
	}
	return grant, nil
}

type ClientCredentialsGrantRefresher struct {
	exchanger  ClientCredentialsExchanger
	clock      clock.Clock
	authMethod string
}

func NewDefaultClientCredentialsGrantRefresher(
	clock clock.Clock,
	options ClientCredentialsFlowOptions,
) (*ClientCredentialsGrantRefresher, error) {
	authMethod, err := normalizeTokenEndpointAuthMethod(options.TokenEndpointAuthMethod)
	if err != nil {
		return nil, err
	}
	options.TokenEndpointAuthMethod = authMethod

	httpClient, err := newClientCredentialsHTTPClient(options)
	if err != nil {
		return nil, err
	}

	tokenRetriever := NewTokenRetriever(httpClient)
	return &ClientCredentialsGrantRefresher{
		exchanger:  tokenRetriever,
		clock:      clock,
		authMethod: authMethod,
	}, nil
}

var _ AuthorizationGrantRefresher = &ClientCredentialsGrantRefresher{}

func (g *ClientCredentialsGrantRefresher) Refresh(grant *AuthorizationGrant) (*AuthorizationGrant, error) {
	if grant.Type != GrantTypeClientCredentials {
		return nil, errors.New("unsupported grant type")
	}
	authMethod := g.authMethod
	if authMethod == "" {
		if grant.ClientCredentials == nil {
			authMethod = TokenEndpointAuthMethodTLSClientAuth
		} else {
			authMethod = TokenEndpointAuthMethodClientSecretPost
		}
	}

	clientID := grant.ClientID
	clientSecret := ""
	if grant.ClientCredentials != nil {
		if clientID == "" {
			clientID = grant.ClientCredentials.ClientID
		}
		clientSecret = grant.ClientCredentials.ClientSecret
	}

	exchangeRequest := ClientCredentialsExchangeRequest{
		TokenEndpoint: grant.TokenEndpoint,
		Audience:      grant.Audience,
		ClientID:      clientID,
		ClientSecret:  clientSecret,
		Scopes:        grant.Scopes,
		AuthMethod:    authMethod,
	}
	tr, err := g.exchanger.ExchangeClientCredentials(exchangeRequest)
	if err != nil {
		return nil, errors.Wrap(err, "could not exchange client credentials")
	}

	token := convertToOAuth2Token(tr, g.clock)
	grant = &AuthorizationGrant{
		Type:              GrantTypeClientCredentials,
		Audience:          grant.Audience,
		ClientID:          grant.ClientID,
		ClientCredentials: grant.ClientCredentials,
		TokenEndpoint:     grant.TokenEndpoint,
		Token:             &token,
		Scopes:            grant.Scopes,
	}
	return grant, nil
}
