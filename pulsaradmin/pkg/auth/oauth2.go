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
	"errors"
	"fmt"
	"net/http"

	"github.com/streamnative/pulsar-admin-go/pkg/auth/oauth2"
)

type OAuth2Provider struct {
	issuer  *oauth2.Issuer
	keyFile string
	T       http.RoundTripper
}

func NewAuthenticationOAuth2(
	issueEndpoint,
	clientID,
	audience,
	keyFile string,
	transport http.RoundTripper) (*OAuth2Provider, error) {

	return &OAuth2Provider{
		issuer: &oauth2.Issuer{
			IssuerEndpoint: issueEndpoint,
			ClientID:       clientID,
			Audience:       audience,
		},
		keyFile: keyFile,
		T:       transport,
	}, nil
}

func (o *OAuth2Provider) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := o.getToken(o.issuer)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	return o.T.RoundTrip(req)
}

func (o *OAuth2Provider) Transport() http.RoundTripper {
	return o.T
}

func (o *OAuth2Provider) getFlow(issuer *oauth2.Issuer) (oauth2.Flow, error) {
	// note that these flows don't rely on the user's cache or configuration
	// to produce an ephemeral token that doesn't replace what is generated
	// by `login` or by `activate-service-account`.

	var err error
	var flow oauth2.Flow
	if o.keyFile != "" {
		flow, err = oauth2.NewDefaultClientCredentialsFlow(*issuer, o.keyFile)
		if err != nil {
			return nil, err
		}
		return flow, err
	}
	return flow, errors.New("the key file must be specified")
}

func (o *OAuth2Provider) getToken(issuer *oauth2.Issuer) (string, error) {
	flow, err := o.getFlow(issuer)
	if err != nil {
		return "", err
	}

	_, token, err := flow.Authorize()
	if err != nil {
		return "", err
	}

	return token.AccessToken, nil
}
