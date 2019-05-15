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
	"github.com/pkg/errors"
	"io/ioutil"
	"strings"
)

type tokenAuthProvider struct {
	tokenSupplier func() (string, error)
}

func NewAuthenticationTokenWithParams(params map[string]string) (Provider, error) {
	if params["token"] != "" {
		return NewAuthenticationToken(params["token"]), nil
	} else if params["file"] != "" {
		return NewAuthenticationTokenFromFile(params["file"]), nil
	} else {
		return nil, errors.New("missing configuration for token auth")
	}
}

func NewAuthenticationToken(token string) Provider {
	return &tokenAuthProvider{
		tokenSupplier: func() (string, error) {
			if token == "" {
				return "", errors.New("empty token credentials")
			} else {
				return token, nil
			}
		},
	}
}

func NewAuthenticationTokenFromFile(tokenFilePath string) Provider {
	return &tokenAuthProvider{
		tokenSupplier: func() (string, error) {
			data, err := ioutil.ReadFile(tokenFilePath)
			if err != nil {
				return "", err
			}

			token := strings.Trim(string(data), " \n")
			if token == "" {
				return "", errors.New("empty token credentials")
			} else {
				return token, nil
			}
		},
	}
}

func (p *tokenAuthProvider) Init() error {
	// Try to read certificates immediately to provide better error at startup
	_, err := p.GetData()
	return err
}

func (p *tokenAuthProvider) Name() string {
	return "token"
}

func (p *tokenAuthProvider) GetTlsCertificate() (*tls.Certificate, error) {
	return nil, nil
}

func (p *tokenAuthProvider) GetData() ([]byte, error) {
	t, err := p.tokenSupplier()
	if err != nil {
		return nil, err
	} else {
		return []byte(t), nil
	}
}

func (tokenAuthProvider) Close() error {
	return nil
}
