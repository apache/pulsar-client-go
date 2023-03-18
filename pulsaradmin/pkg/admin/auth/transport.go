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
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/http"

	"github.com/streamnative/pulsar-admin-go/pkg/admin/config"
)

type Transport struct {
	T http.RoundTripper
}

// GetDefaultTransport gets a default transport.
// Deprecated: Use NewDefaultTransport instead.
func GetDefaultTransport(config *config.Config) http.RoundTripper {
	transport, err := NewDefaultTransport(config)
	if err != nil {
		panic(err)
	}

	return transport
}

func NewDefaultTransport(config *config.Config) (http.RoundTripper, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.TLSAllowInsecureConnection,
	}
	if len(config.TLSTrustCertsFilePath) > 0 {
		rootCA, err := ioutil.ReadFile(config.TLSTrustCertsFilePath)
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = x509.NewCertPool()
		tlsConfig.RootCAs.AppendCertsFromPEM(rootCA)
	}
	transport.MaxIdleConnsPerHost = 10
	transport.TLSClientConfig = tlsConfig
	return transport, nil
}
