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
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/pkg/errors"

	"github.com/apache/pulsar-client-go/pulsaradmin/internal/httptools"
)

const (
	tokenPrefix = "token:"
	filePrefix  = "file:"
)

type TokenTransport struct {
	Token string
	T     http.RoundTripper
}

func NewTokenTransport(tokenData string, transport *http.Transport) (*TokenTransport, error) {
	tokenData = strings.TrimSpace(tokenData)
	if len(tokenData) == 0 {
		return nil, errors.New("empty token")
	}
	return &TokenTransport{
		Token: tokenData,
		T:     transport,
	}, nil
}

func NewTokenTransportFromKV(paramString string, transport *http.Transport) (*TokenTransport, error) {
	switch {
	case strings.HasPrefix(paramString, tokenPrefix):
		token := paramString[len(tokenPrefix):]
		return NewTokenTransport(token, transport)
	case strings.HasPrefix(paramString, filePrefix):
		tokenFile := paramString[len(filePrefix):]
		return NewTokenTransportFromFile(tokenFile, transport)
	default:
		return NewTokenTransport(paramString, transport)
	}
}

func NewTokenTransportFromFile(tokenPath string, transport *http.Transport) (*TokenTransport, error) {
	fileContents, err := os.ReadFile(tokenPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("token file not found")
		}
		return nil, fmt.Errorf("unable to open token file: %v", err)
	}
	return NewTokenTransport(string(fileContents), transport)
}

func (p *TokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = httptools.CloneReq(req)
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", p.Token))
	return p.T.RoundTrip(req)
}
