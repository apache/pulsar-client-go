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
	"bytes"
	"errors"
	"io/ioutil"
	"testing"
	"time"

	zms "github.com/AthenZ/athenz/libs/go/zmssvctoken"
	zts "github.com/AthenZ/athenz/libs/go/ztsroletoken"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

const tlsClientKeyPath = "../../../integration-tests/certs/client-key.pem"

type MockTokenBuilder struct {
	mock.Mock
}

type MockToken struct {
	mock.Mock
}

type MockRoleToken struct {
	mock.Mock
}

func (m *MockTokenBuilder) SetExpiration(t time.Duration) {
}
func (m *MockTokenBuilder) SetHostname(h string) {
}
func (m *MockTokenBuilder) SetIPAddress(ip string) {
}
func (m *MockTokenBuilder) SetKeyService(keyService string) {
}
func (m *MockTokenBuilder) Token() zms.Token {
	result := m.Called()
	return result.Get(0).(zms.Token)
}

func (m *MockToken) Value() (string, error) {
	result := m.Called()
	return result.Get(0).(string), result.Error(1)
}

func (m *MockRoleToken) RoleTokenValue() (string, error) {
	result := m.Called()
	return result.Get(0).(string), result.Error(1)
}

func MockZmsNewTokenBuilder(domain, name string, privateKeyPEM []byte, keyVersion string) (zms.TokenBuilder, error) {
	// assertion
	key, err := ioutil.ReadFile(tlsClientKeyPath)
	if err != nil {
		return nil, err
	}
	if domain != "pulsar.test.tenant" ||
		name != "service" ||
		!bytes.Equal(key, privateKeyPEM) ||
		keyVersion != "0" {
		return nil, errors.New("Assertion error")
	}

	mockToken := new(MockToken)
	mockToken.On("Value").Return("mockPrincipalToken", nil)
	mockTokenBuilder := new(MockTokenBuilder)
	mockTokenBuilder.On("Token").Return(mockToken)
	return mockTokenBuilder, nil
}

func MockZtsNewRoleToken(tok zms.Token, domain string, opts zts.RoleTokenOptions) zts.RoleToken {
	// assertion
	token, err := tok.Value()
	if err != nil {
		return nil
	}
	if token != "mockPrincipalToken" ||
		domain != "pulsar.test.provider" ||
		opts.BaseZTSURL != "http://localhost:9999/zts/v1" ||
		opts.AuthHeader != "" {
		return nil
	}

	mockRoleToken := new(MockRoleToken)
	mockRoleToken.On("RoleTokenValue").Return("mockRoleToken", nil)
	return mockRoleToken
}

func TestAthenzAuth(t *testing.T) {
	privateKey := "file://" + tlsClientKeyPath
	provider := NewAuthenticationAthenz(
		"pulsar.test.provider",
		"pulsar.test.tenant",
		"service",
		privateKey,
		"",
		"",
		"http://localhost:9999")

	// inject mock function
	athenz := provider.(*athenzAuthProvider)
	athenz.zmsNewTokenBuilder = MockZmsNewTokenBuilder
	athenz.ztsNewRoleToken = MockZtsNewRoleToken

	err := athenz.Init()
	assert.NoError(t, err)

	data, err := athenz.GetData()
	assert.Equal(t, []byte("mockRoleToken"), data)
	assert.NoError(t, err)
}
