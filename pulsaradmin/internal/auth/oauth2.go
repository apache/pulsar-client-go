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

	"github.com/99designs/keyring"
	"github.com/pkg/errors"
	xoauth2 "golang.org/x/oauth2"

	"github.com/apache/pulsar-client-go/oauth2"
	"github.com/apache/pulsar-client-go/oauth2/cache"
	clock2 "github.com/apache/pulsar-client-go/oauth2/clock"
	"github.com/apache/pulsar-client-go/oauth2/store"
	"github.com/apache/pulsar-client-go/pulsaradmin/internal/httptools"
)

type Oauth2Transport struct {
	clock            clock2.RealClock
	issuer           oauth2.Issuer
	store            store.Store
	source           cache.CachingTokenSource
	defaultTransport http.RoundTripper
	tokenTransport   *oa2Transport
}

func NewOauth2Provider(
	issuerEndpoint,
	clientID,
	audience,
	scope,
	privateKey string,
	transport *http.Transport,
) (*Oauth2Transport, error) {
	issuer := oauth2.Issuer{
		IssuerEndpoint: issuerEndpoint,
		ClientID:       clientID,
		Audience:       audience,
	}
	// TODO: is it an error here if private key comes in with scope?
	if privateKey != "" {
		return oAuth2WithDefaultFlow(issuer, privateKey)
	}

	keyringStore, err := makeKeyringStore()
	if err != nil {
		return nil, err
	}

	p := &Oauth2Transport{
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

func NewAuthenticationOAuth2(issuer oauth2.Issuer, store store.Store) (*Oauth2Transport, error) {
	p := &Oauth2Transport{
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

// oAuth2WithDefaultFlow uses memory to save the grant
func oAuth2WithDefaultFlow(issuer oauth2.Issuer, keyFile string) (*Oauth2Transport, error) {
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

	p := &Oauth2Transport{
		clock:  clock2.RealClock{},
		issuer: issuer,
		store:  st,
	}

	err = p.loadGrant()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (o *Oauth2Transport) loadGrant() error {
	grant, err := o.store.LoadGrant(o.issuer.Audience)
	if err != nil {
		if err == store.ErrNoAuthenticationData {
			return errors.New("oauth2 login required")
		}
		return err
	}
	return o.initCache(grant)
}

func (o *Oauth2Transport) initCache(grant *oauth2.AuthorizationGrant) error {
	refresher, err := o.getRefresher(grant.Type)
	if err != nil {
		return err
	}

	source, err := cache.NewDefaultTokenCache(o.store, o.issuer.Audience, refresher)
	if err != nil {
		return err
	}
	o.source = source
	o.tokenTransport = &oa2Transport{
		source: o.source,
		wrapped: &xoauth2.Transport{
			Source: o.source,
			Base:   o.defaultTransport,
		},
	}
	return nil
}

func (o *Oauth2Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	return o.tokenTransport.RoundTrip(httptools.CloneReq(req))
}

func (o *Oauth2Transport) getRefresher(t oauth2.AuthorizationGrantType) (oauth2.AuthorizationGrantRefresher, error) {
	switch t {
	case oauth2.GrantTypeClientCredentials:
		return oauth2.NewDefaultClientCredentialsGrantRefresher(o.clock)
	case oauth2.GrantTypeDeviceCode:
		return oauth2.NewDefaultDeviceAuthorizationGrantRefresher(o.clock)
	default:
		return nil, store.ErrUnsupportedAuthData
	}
}

type oa2Transport struct {
	source  cache.CachingTokenSource
	wrapped *xoauth2.Transport
}

func (t *oa2Transport) RoundTrip(req *http.Request) (*http.Response, error) {
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

const (
	serviceName  = "pulsar"
	keyChainName = "pulsar-admin-go"
)

func makeKeyringStore() (store.Store, error) {
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

func keyringPrompt(_ string) (string, error) {
	return "", nil
}
