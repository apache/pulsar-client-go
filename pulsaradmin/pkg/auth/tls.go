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
	"net/http"
	"strings"
)

const (
	TLSPluginName = "org.apache.pulsar.client.impl.auth.AuthenticationTls"
)

type TLSAuthProvider struct {
	certificatePath string
	privateKeyPath  string
	T               http.RoundTripper
}

// NewAuthenticationTLS initialize the authentication provider
func NewAuthenticationTLS(certificatePath string, privateKeyPath string,
	transport http.RoundTripper) (*TLSAuthProvider, error) {
	provider := &TLSAuthProvider{
		certificatePath: certificatePath,
		privateKeyPath:  privateKeyPath,
		T:               transport,
	}
	if err := provider.configTLS(); err != nil {
		return nil, err
	}
	return provider, nil
}

func NewAuthenticationTLSFromAuthParams(encodedAuthParams string,
	transport http.RoundTripper) (*TLSAuthProvider, error) {
	var certificatePath string
	var privateKeyPath string
	parts := strings.Split(encodedAuthParams, ",")
	for _, part := range parts {
		kv := strings.Split(part, ":")
		switch kv[0] {
		case "tlsCertFile":
			certificatePath = kv[1]
		case "tlsKeyFile":
			privateKeyPath = kv[1]
		}
	}
	return NewAuthenticationTLS(certificatePath, privateKeyPath, transport)
}

func (p *TLSAuthProvider) GetTLSCertificate() (*tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(p.certificatePath, p.privateKeyPath)
	return &cert, err
}

func (p *TLSAuthProvider) RoundTrip(req *http.Request) (*http.Response, error) {
	return p.T.RoundTrip(req)
}

func (p *TLSAuthProvider) Transport() http.RoundTripper {
	return p.T
}

func (p *TLSAuthProvider) configTLS() error {
	cert, err := p.GetTLSCertificate()
	if err != nil {
		return err
	}
	transport := p.T.(*http.Transport)
	transport.TLSClientConfig.Certificates = []tls.Certificate{*cert}
	return nil
}
