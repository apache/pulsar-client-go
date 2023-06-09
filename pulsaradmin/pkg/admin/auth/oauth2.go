// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
	"path/filepath"

	"github.com/99designs/keyring"
	"github.com/apache/pulsar-client-go/oauth2"
	"github.com/apache/pulsar-client-go/oauth2/cache"
	clock2 "github.com/apache/pulsar-client-go/oauth2/clock"
	"github.com/apache/pulsar-client-go/oauth2/store"
	"github.com/pkg/errors"
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
	store            store.Store
	source           cache.CachingTokenSource
	defaultTransport http.RoundTripper
	tokenTransport   *transport
}

func NewAuthenticationOAuth2(issuer oauth2.Issuer, store store.Store) (*OAuth2Provider, error) {
	p := &OAuth2Provider{
		clock:  clock2.RealClock{},
		issuer: issuer,
		store:  store,
	}

	err := p.loadGrant()
	if err != nil {
		return nil, err
	}

	return p, nil
}

// NewAuthenticationOAuth2WithDefaultFlow uses memory to save the grant
func NewAuthenticationOAuth2WithDefaultFlow(issuer oauth2.Issuer, keyFile string) (Provider, error) {
	st := store.NewMemoryStore()
	flow, err := oauth2.NewDefaultClientCredentialsFlow(oauth2.ClientCredentialsFlowOptions{
		KeyFile: keyFile,
	})
	if err != nil {
		return nil, err
	}

	grant, err := flow.Authorize(issuer.Audience)
	if err != nil {
		return nil, err
	}

	err = st.SaveGrant(issuer.Audience, *grant)
	if err != nil {
		return nil, err
	}

	p := &OAuth2Provider{
		clock:  clock2.RealClock{},
		issuer: issuer,
		store:  st,
	}

	return p, p.loadGrant()
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
	scope string,
	transport http.RoundTripper) (*OAuth2Provider, error) {

	issuer := oauth2.Issuer{
		IssuerEndpoint: issuerEndpoint,
		ClientID:       clientID,
		Audience:       audience,
	}

	keyringStore, err := MakeKeyringStore()
	if err != nil {
		return nil, err
	}

	p := &OAuth2Provider{
		clock:            clock2.RealClock{},
		issuer:           issuer,
		store:            keyringStore,
		defaultTransport: transport,
	}

	err = p.loadGrant()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (o *OAuth2Provider) loadGrant() error {
	grant, err := o.store.LoadGrant(o.issuer.Audience)
	if err != nil {
		if err == store.ErrNoAuthenticationData {
			return errors.New("oauth2 login required")
		}
		return err
	}
	return o.initCache(grant)
}

func (o *OAuth2Provider) initCache(grant *oauth2.AuthorizationGrant) error {
	refresher, err := o.getRefresher(grant.Type)
	if err != nil {
		return err
	}

	source, err := cache.NewDefaultTokenCache(o.store, o.issuer.Audience, refresher)
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
		return nil, store.ErrUnsupportedAuthData
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

const (
	serviceName  = "pulsar"
	keyChainName = "pulsarctl"
)

func MakeKeyringStore() (store.Store, error) {
	kr, err := makeKeyring()
	if err != nil {
		return nil, err
	}
	return store.NewKeyringStore(kr)
}

func makeKeyring() (keyring.Keyring, error) {
	return keyring.Open(keyring.Config{
		AllowedBackends:          keyring.AvailableBackends(),
		ServiceName:              serviceName,
		KeychainName:             keyChainName,
		KeychainTrustApplication: true,
		FileDir:                  filepath.Join("~/.config/pulsar", "credentials"),
		FilePasswordFunc:         keyringPrompt,
	})
}

func keyringPrompt(prompt string) (string, error) {
	return "", nil
}
