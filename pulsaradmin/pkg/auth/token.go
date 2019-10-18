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
	"io/ioutil"
	"strings"

	"github.com/pkg/errors"
)

type TokenAuthProvider struct {
	tokenSupplier func() (string, error)
}

// NewAuthenticationTokenWithParams return a interface of Provider with string map.
func NewAuthenticationTokenWithParams(params map[string]string) (*TokenAuthProvider, error) {
	switch {
	case params["token"] != "":
		return NewAuthenticationToken(params["token"]), nil
	case params["file"] != "":
		return NewAuthenticationTokenFromFile(params["file"]), nil
	default:
		return nil, errors.New("missing configuration for token auth")
	}
}

// NewAuthenticationToken return a interface of Provider with a string token.
func NewAuthenticationToken(token string) *TokenAuthProvider {
	return &TokenAuthProvider{
		tokenSupplier: func() (string, error) {
			if token == "" {
				return "", errors.New("empty token credentials")
			}
			return token, nil
		},
	}
}

// NewAuthenticationTokenFromFile return a interface of a Provider with a string token file path.
func NewAuthenticationTokenFromFile(tokenFilePath string) *TokenAuthProvider {
	return &TokenAuthProvider{
		tokenSupplier: func() (string, error) {
			data, err := ioutil.ReadFile(tokenFilePath)
			if err != nil {
				return "", err
			}

			token := strings.Trim(string(data), " \n")
			if token == "" {
				return "", errors.New("empty token credentials")
			}
			return token, nil
		},
	}
}

func (p *TokenAuthProvider) Init() error {
	// Try to read certificates immediately to provide better error at startup
	_, err := p.GetData()
	return err
}

func (p *TokenAuthProvider) GetData() ([]byte, error) {
	t, err := p.tokenSupplier()
	if err != nil {
		return nil, err
	}
	return []byte(t), nil
}
