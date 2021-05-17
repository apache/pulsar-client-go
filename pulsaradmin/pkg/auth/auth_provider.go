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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common"
)

// Provider provide a general method to add auth message
type Provider interface {
	RoundTrip(req *http.Request) (*http.Response, error)
	Transport() http.RoundTripper
	WithTransport(tripper http.RoundTripper)
}

type Transport struct {
	T http.RoundTripper
}

func GetAuthProvider(config *common.Config) (*Provider, error) {
	var provider Provider
	defaultTransport := GetDefaultTransport(config)
	var err error
	switch config.AuthPlugin {
	case TLSPluginName:
		provider, err = NewAuthenticationTLSFromAuthParams(config.AuthParams, defaultTransport)
	case TokenPluginName:
		provider, err = NewAuthenticationTokenFromAuthParams(config.AuthParams, defaultTransport)
	default:
		switch {
		case len(config.TLSCertFile) > 0 && len(config.TLSKeyFile) > 0:
			provider, err = NewAuthenticationTLS(config.TLSCertFile, config.TLSKeyFile, defaultTransport)
		case len(config.Token) > 0:
			provider, err = NewAuthenticationToken(config.Token, defaultTransport)
		case len(config.TokenFile) > 0:
			provider, err = NewAuthenticationTokenFromFile(config.TokenFile, defaultTransport)
		case len(config.IssuerEndpoint) > 0 || len(config.KeyFile) > 0:
			provider, err = NewAuthenticationOAuth2WithParams(
				config.IssuerEndpoint,
				config.ClientID,
				config.Audience, defaultTransport)
		}
	}
	return &provider, err
}

func GetDefaultTransport(config *common.Config) http.RoundTripper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.TLSAllowInsecureConnection,
	}
	if len(config.TLSTrustCertsFilePath) > 0 {
		rootCA, err := ioutil.ReadFile(config.TLSTrustCertsFilePath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error loading certificate authority:", err)
			os.Exit(1)
		}
		tlsConfig.RootCAs = x509.NewCertPool()
		tlsConfig.RootCAs.AppendCertsFromPEM(rootCA)
	}
	transport.MaxIdleConnsPerHost = 10
	transport.TLSClientConfig = tlsConfig
	return transport
}
