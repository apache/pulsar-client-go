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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/pkg/errors"
)

const (
	tokenPrefix         = "token:"
	filePrefix          = "file:"
	TokenPluginName     = "org.apache.pulsar.client.impl.auth.AuthenticationToken"
	TokePluginShortName = "token"
)

type Token struct {
	Token string `json:"token"`
	File string `json:"file"`
}

type TokenAuthProvider struct {
	T     http.RoundTripper
	token string
}

// NewAuthenticationToken return a interface of Provider with a string token.
func NewAuthenticationToken(token string, transport http.RoundTripper) (*TokenAuthProvider, error) {
	if len(token) == 0 {
		return nil, errors.New("No token provided")
	}
	return &TokenAuthProvider{token: token, T: transport}, nil
}

// NewAuthenticationTokenFromFile return a interface of a Provider with a string token file path.
func NewAuthenticationTokenFromFile(tokenFilePath string, transport http.RoundTripper) (*TokenAuthProvider, error) {
	data, err := os.ReadFile(tokenFilePath)
	if err != nil {
		return nil, err
	}
	token := strings.Trim(string(data), " \n")
	return NewAuthenticationToken(token, transport)
}

func NewAuthenticationTokenFromAuthParams(encodedAuthParam string,
	transport http.RoundTripper) (*TokenAuthProvider, error) {
	var tokenAuthProvider *TokenAuthProvider
	var err error

	var tokenJSON Token
	err = json.Unmarshal([]byte(encodedAuthParam), &tokenJSON)
	if err != nil {
		switch {
		case strings.HasPrefix(encodedAuthParam, tokenPrefix):
			tokenAuthProvider, err = NewAuthenticationToken(strings.TrimPrefix(encodedAuthParam, tokenPrefix), transport)
		case strings.HasPrefix(encodedAuthParam, filePrefix):
			tokenAuthProvider, err = NewAuthenticationTokenFromFile(strings.TrimPrefix(encodedAuthParam, filePrefix), transport)
		default:
			tokenAuthProvider, err = NewAuthenticationToken(encodedAuthParam, transport)
		}
	} else {
		if tokenJSON.File != "" {
			tokenAuthProvider, err = NewAuthenticationTokenFromFile(tokenJSON.File, transport)
		} else if tokenJSON.Token != "" {
			tokenAuthProvider, err = NewAuthenticationToken(tokenJSON.Token, transport)
		} else {
			return nil, errors.New("unsupported token json auth param")
		}
	}
	return tokenAuthProvider, err
}

func (p *TokenAuthProvider) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", p.token))
	return p.T.RoundTrip(req)
}

func (p *TokenAuthProvider) Transport() http.RoundTripper {
	return p.T
}

func (p *TokenAuthProvider) WithTransport(tripper http.RoundTripper) {
	p.T = tripper
}
