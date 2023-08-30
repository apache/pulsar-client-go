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
	"fmt"
	"net/http"
	"strings"

	"github.com/pkg/errors"
)

type TLSTransport struct {
	T http.RoundTripper
}

// NewTLSTransport initialize the authentication provider
func NewTLSTransport(certificatePath string, privateKeyPath string, transport *http.Transport) (*TLSTransport, error) {
	switch {
	case certificatePath == "":
		return nil, errors.New("certificate path required")
	case privateKeyPath == "":
		return nil, errors.New("private key path required")
	}

	cert, err := tls.LoadX509KeyPair(certificatePath, privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("loading TLS certificates: %w", err)
	}

	newTransport := transport.Clone()
	newTransport.TLSClientConfig.Certificates = append([]tls.Certificate{cert},
		newTransport.TLSClientConfig.Certificates...)

	return &TLSTransport{
		T: newTransport,
	}, nil
}

func NewTLSTransportFromKV(paramString string, transport *http.Transport) (*TLSTransport, error) {
	var (
		certificatePath string
		privateKeyPath  string
	)
	parts := strings.Split(paramString, ",")
	for _, part := range parts {
		kv := strings.Split(part, ":")
		switch kv[0] {
		case "tlsCertFile":
			certificatePath = kv[1]
		case "tlsKeyFile":
			privateKeyPath = kv[1]
		}
	}
	if certificatePath == "" && privateKeyPath == "" {
		return nil, errors.New(`TLS auth params must be in the form of "tlsCertFile:<path>,tlsKeyFile:<path>"`)
	}
	return NewTLSTransport(certificatePath, privateKeyPath, transport)
}

func (p *TLSTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return p.T.RoundTrip(req)
}
