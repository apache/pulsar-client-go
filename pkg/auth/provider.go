//
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
//

package auth

import (
    "crypto/tls"
    "fmt"
    "io"

    "github.com/pkg/errors"
)

type Provider interface {
	Init() error

	Name() string

	// return a client certificate chain, or nil if the data are not available
	GetTlsCertificate() (*tls.Certificate, error)

	//
	GetData() ([]byte, error)

	io.Closer
}

func NewProvider(name string, params string) (Provider, error) {
	m := parseParams(params)

	switch name {
	case "":
		return NewAuthDisabled(), nil

	case "tls", "org.apache.pulsar.client.impl.auth.AuthenticationTls":
		return NewAuthenticationTLSWithParams(m), nil

	case "token", "org.apache.pulsar.client.impl.auth.AuthenticationToken":
		return NewAuthenticationTokenWithParams(m)

	default:
		return nil, errors.New(fmt.Sprintf("invalid auth provider '%s'", name))
	}
}

func parseParams(params string) map[string]string {
	return nil
}
