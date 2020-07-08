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

package oauth2

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAuth(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cloud-cli Auth Suite")
}

type MockTokenExchanger struct {
	CalledWithRequest        interface{}
	ReturnsTokens            *TokenResult
	ReturnsError             error
	RefreshCalledWithRequest *RefreshTokenExchangeRequest
}

func (te *MockTokenExchanger) ExchangeCode(req AuthorizationCodeExchangeRequest) (*TokenResult, error) {
	te.CalledWithRequest = &req
	return te.ReturnsTokens, te.ReturnsError
}

func (te *MockTokenExchanger) ExchangeRefreshToken(req RefreshTokenExchangeRequest) (*TokenResult, error) {
	te.RefreshCalledWithRequest = &req
	return te.ReturnsTokens, te.ReturnsError
}

func (te *MockTokenExchanger) ExchangeClientCredentials(req ClientCredentialsExchangeRequest) (*TokenResult, error) {
	te.CalledWithRequest = &req
	return te.ReturnsTokens, te.ReturnsError
}

func (te *MockTokenExchanger) ExchangeDeviceCode(ctx context.Context,
	req DeviceCodeExchangeRequest) (*TokenResult, error) {
	te.CalledWithRequest = &req
	return te.ReturnsTokens, te.ReturnsError
}
