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

package auth

import (
	"encoding/json"
	"net/http"

	"github.com/apache/pulsar-client-go/oauth2"
	"github.com/apache/pulsar-client-go/oauth2/cache"
	clock2 "github.com/apache/pulsar-client-go/oauth2/clock"
	xoauth2 "golang.org/x/oauth2"
)

const (
	OAuth2PluginName      = "org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2"
	OAuth2PluginShortName = "oauth2"
)

type OAuth2ClientCredentials struct {
	IssuerURL  string `json:"issuerUrl,omitempty"`
	Audience   string `json:"audience,omitempty"`
	Scope      string `json:"scope,omitempty"`
	PrivateKey string `json:"privateKey,omitempty"`
	ClientID   string `json:"clientId,omitempty"`
}

type OAuth2Provider struct {
	clock            clock2.RealClock
	issuer           oauth2.Issuer
	source           cache.CachingTokenSource
	defaultTransport http.RoundTripper
	tokenTransport   *transport
	flow             *oauth2.ClientCredentialsFlow
}

func NewAuthenticationOAuth2(issuer oauth2.Issuer) (*OAuth2Provider, error) {
	p := &OAuth2Provider{
		clock:  clock2.RealClock{},
		issuer: issuer,
	}

	err := p.initCache()
	if err != nil {
		return nil, err
	}

	return p, nil
}

// NewAuthenticationOAuth2WithDefaultFlow uses memory to save the grant
func NewAuthenticationOAuth2WithDefaultFlow(issuer oauth2.Issuer, keyFile string) (Provider, error) {
	return NewAuthenticationOAuth2WithFlow(issuer, oauth2.ClientCredentialsFlowOptions{
		KeyFile: keyFile,
	})
}

func NewAuthenticationOAuth2WithFlow(
	issuer oauth2.Issuer, flowOptions oauth2.ClientCredentialsFlowOptions) (Provider, error) {
	flow, err := oauth2.NewDefaultClientCredentialsFlow(flowOptions)
	if err != nil {
		return nil, err
	}

	p := &OAuth2Provider{
		clock:  clock2.RealClock{},
		issuer: issuer,
		flow:   flow,
	}

	return p, p.initCache()
}

func NewAuthenticationOAuth2FromAuthParams(encodedAuthParam string,
	transport http.RoundTripper) (*OAuth2Provider, error) {

	var paramsJSON OAuth2ClientCredentials
	err := json.Unmarshal([]byte(encodedAuthParam), &paramsJSON)
	if err != nil {
		return nil, err
	}
	return NewAuthenticationOAuth2WithParams(paramsJSON.IssuerURL, paramsJSON.ClientID, paramsJSON.Audience,
		paramsJSON.Scope, transport)
}

func NewAuthenticationOAuth2WithParams(
	issuerEndpoint,
	clientID,
	audience string,
	_ string,
	transport http.RoundTripper) (*OAuth2Provider, error) {

	issuer := oauth2.Issuer{
		IssuerEndpoint: issuerEndpoint,
		ClientID:       clientID,
		Audience:       audience,
	}

	p := &OAuth2Provider{
		clock:            clock2.RealClock{},
		issuer:           issuer,
		defaultTransport: transport,
	}

	err := p.initCache()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (o *OAuth2Provider) initCache() error {
	source, err := cache.NewDefaultTokenCache(o.issuer.Audience, o.flow)
	if err != nil {
		return err
	}
	o.source = source
	o.tokenTransport = &transport{
		source: o.source,
		wrapped: &xoauth2.Transport{
			Source: o.source,
			Base:   o.defaultTransport,
		},
	}
	return nil
}

func (o *OAuth2Provider) RoundTrip(req *http.Request) (*http.Response, error) {
	return o.tokenTransport.RoundTrip(req)
}

func (o *OAuth2Provider) WithTransport(tripper http.RoundTripper) {
	o.defaultTransport = tripper
}

func (o *OAuth2Provider) Transport() http.RoundTripper {
	return o.tokenTransport
}

func (o *OAuth2Provider) getRefresher(t oauth2.AuthorizationGrantType) (oauth2.AuthorizationGrantRefresher, error) {
	switch t {
	case oauth2.GrantTypeClientCredentials:
		return oauth2.NewDefaultClientCredentialsGrantRefresher(o.clock)
	case oauth2.GrantTypeDeviceCode:
		return oauth2.NewDefaultDeviceAuthorizationGrantRefresher(o.clock)
	default:
		return nil, oauth2.ErrUnsupportedAuthData
	}
}

type transport struct {
	source  cache.CachingTokenSource
	wrapped *xoauth2.Transport
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if len(req.Header.Get("Authorization")) != 0 {
		return t.wrapped.Base.RoundTrip(req)
	}

	res, err := t.wrapped.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode == 401 {
		err := t.source.InvalidateToken()
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (t *transport) WrappedRoundTripper() http.RoundTripper { return t.wrapped.Base }
