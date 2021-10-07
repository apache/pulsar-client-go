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
	"net/http"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/99designs/keyring"
	"github.com/apache/pulsar-client-go/oauth2"
	"github.com/apache/pulsar-client-go/oauth2/cache"
	clock2 "github.com/apache/pulsar-client-go/oauth2/clock"
	"github.com/apache/pulsar-client-go/oauth2/store"
	util "github.com/streamnative/pulsar-admin-go/pkg/pulsar/utils"
	xoauth2 "golang.org/x/oauth2"
)

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
	return &transport{
		source: o.source,
		wrapped: &xoauth2.Transport{
			Source: o.source,
			Base:   o.defaultTransport,
		},
	}
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
		FileDir:                  filepath.Join(util.HomeDir(), "~/.config/pulsar", "credentials"),
		FilePasswordFunc:         keyringPrompt,
	})
}

func keyringPrompt(prompt string) (string, error) {
	return "", nil
}
