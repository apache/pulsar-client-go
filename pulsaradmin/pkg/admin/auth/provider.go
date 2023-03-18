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
	"net/http"

	"github.com/streamnative/pulsar-admin-go/pkg/admin/config"
)

// Provider provide a general method to add auth message
type Provider interface {
	RoundTrip(req *http.Request) (*http.Response, error)
	Transport() http.RoundTripper
	WithTransport(tripper http.RoundTripper)
}

func GetAuthProvider(config *config.Config) (Provider, error) {
	var provider Provider
	defaultTransport, err := NewDefaultTransport(config)
	if err != nil {
		return nil, err
	}
	switch config.AuthPlugin {
	case TLSPluginShortName:
		fallthrough
	case TLSPluginName:
		provider, err = NewAuthenticationTLSFromAuthParams(config.AuthParams, defaultTransport)
	case TokenPluginName:
		fallthrough
	case TokePluginShortName:
		provider, err = NewAuthenticationTokenFromAuthParams(config.AuthParams, defaultTransport)
	case OAuth2PluginName:
		fallthrough
	case OAuth2PluginShortName:
		provider, err = NewAuthenticationOAuth2FromAuthParams(config.AuthParams, defaultTransport)
	default:
		switch {
		case len(config.TLSCertFile) > 0 && len(config.TLSKeyFile) > 0:
			provider, err = NewAuthenticationTLS(config.TLSCertFile, config.TLSKeyFile, defaultTransport)
		case len(config.Token) > 0:
			provider, err = NewAuthenticationToken(config.Token, defaultTransport)
		case len(config.TokenFile) > 0:
			provider, err = NewAuthenticationTokenFromFile(config.TokenFile, defaultTransport)
		case len(config.IssuerEndpoint) > 0 || len(config.ClientID) > 0 || len(config.Audience) > 0 || len(config.Scope) > 0:
			provider, err = NewAuthenticationOAuth2WithParams(
				config.IssuerEndpoint, config.ClientID, config.Audience, config.Scope, defaultTransport)
		}
	}
	return provider, err
}
