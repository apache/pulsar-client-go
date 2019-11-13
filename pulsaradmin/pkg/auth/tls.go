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
	"io/ioutil"

	"github.com/pkg/errors"
)

type TLSAuthProvider struct {
	certificatePath         string
	privateKeyPath          string
	allowInsecureConnection bool
}

// NewAuthenticationTLS initialize the authentication provider
func NewAuthenticationTLS(certificatePath string, privateKeyPath string,
	allowInsecureConnection bool) *TLSAuthProvider {

	return &TLSAuthProvider{
		certificatePath:         certificatePath,
		privateKeyPath:          privateKeyPath,
		allowInsecureConnection: allowInsecureConnection,
	}
}

func (p *TLSAuthProvider) Init() error {
	// Try to read certificates immediately to provide better error at startup
	_, err := p.GetTLSCertificate()
	return err
}

func (p *TLSAuthProvider) Name() string {
	return "tls"
}

func (p *TLSAuthProvider) GetTLSCertificate() (*tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(p.certificatePath, p.privateKeyPath)
	return &cert, err
}

func (p *TLSAuthProvider) GetTLSConfig(certFile string, allowInsecureConnection bool) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: allowInsecureConnection,
	}

	if certFile != "" {
		caCerts, err := ioutil.ReadFile(certFile)
		if err != nil {
			return nil, err
		}

		tlsConfig.RootCAs = x509.NewCertPool()
		if !tlsConfig.RootCAs.AppendCertsFromPEM(caCerts) {
			return nil, errors.New("failed to parse root CAs certificates")
		}
	}

	cert, err := p.GetTLSCertificate()
	if err != nil {
		return nil, err
	}

	if cert != nil {
		tlsConfig.Certificates = []tls.Certificate{*cert}
	}

	return tlsConfig, nil
}

func (p *TLSAuthProvider) HasDataForHTTP() bool {
	return false
}

func (p *TLSAuthProvider) GetHTTPHeaders() (map[string]string, error) {
	return nil, errors.New("Unsupported operation")
}
