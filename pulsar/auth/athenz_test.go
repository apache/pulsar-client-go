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
	"os"
	"testing"
	"time"

	zms "github.com/AthenZ/athenz/libs/go/zmssvctoken"
	zts "github.com/AthenZ/athenz/libs/go/ztsroletoken"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	clientKeyPath  = "../../integration-tests/certs/client-key.pem"
	clientCertPath = "../../integration-tests/certs/client-cert.pem"
	caCertPath     = "../../integration-tests/certs/cacert.pem"
)

type MockTokenBuilder struct {
	mock.Mock
}

type MockToken struct {
	mock.Mock
}

type MockRoleToken struct {
	mock.Mock
	isPrefetcherStarted bool
}

func (m *MockTokenBuilder) SetExpiration(_ time.Duration) {
}
func (m *MockTokenBuilder) SetHostname(_ string) {
}
func (m *MockTokenBuilder) SetIPAddress(_ string) {
}
func (m *MockTokenBuilder) SetKeyService(_ string) {
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

func (m *MockRoleToken) StartPrefetcher() error {
	m.isPrefetcherStarted = true
	result := m.Called()
	return result.Error(0)
}

func (m *MockRoleToken) StopPrefetcher() error {
	m.isPrefetcherStarted = false
	result := m.Called()
	return result.Error(0)
}

func MockZmsNewTokenBuilder(domain, name string, privateKeyPEM []byte, keyVersion string) (zms.TokenBuilder, error) {
	// assertion
	key, err := os.ReadFile(clientKeyPath)
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
	if opts.ProxyURL == "" {
		mockRoleToken.On("RoleTokenValue").Return("mockRoleToken", nil)
	} else {
		mockRoleToken.On("RoleTokenValue").Return("mockRoleToken-"+opts.ProxyURL, nil)
	}
	mockRoleToken.On("StartPrefetcher").Return(nil)
	mockRoleToken.On("StopPrefetcher").Return(nil)
	return mockRoleToken
}

func MockZtsNewRoleTokenFromCert(certFile, keyFile, domain string, opts zts.RoleTokenOptions) zts.RoleToken {
	// assertion
	if certFile != clientCertPath ||
		keyFile != clientKeyPath ||
		domain != "pulsar.test.provider" ||
		opts.BaseZTSURL != "http://localhost:9999/zts/v1" ||
		opts.AuthHeader != "" ||
		opts.CACert == nil {
		return nil
	}

	mockRoleToken := new(MockRoleToken)
	if opts.ProxyURL == "" {
		mockRoleToken.On("RoleTokenValue").Return("mockRoleTokenFromCert", nil)
	} else {
		mockRoleToken.On("RoleTokenValue").Return("mockRoleTokenFromCert-"+opts.ProxyURL, nil)
	}
	mockRoleToken.On("StartPrefetcher").Return(nil)
	mockRoleToken.On("StopPrefetcher").Return(nil)
	return mockRoleToken
}

func TestAthenzAuth(t *testing.T) {
	privateKey := "file://" + clientKeyPath
	provider := NewAuthenticationAthenz(
		"pulsar.test.provider",  // providerDomain
		"pulsar.test.tenant",    // tenantDomain
		"service",               // tenantService
		privateKey,              // privateKey
		"",                      // keyID
		"",                      // x509CertChain
		"",                      // caCert
		"",                      // principalHeader
		"",                      // roleHeader
		"http://localhost:9999", // ztsURL
		"")                      // ztsProxyURL

	// inject mock function
	athenz := provider.(*athenzAuthProvider)
	athenz.zmsNewTokenBuilder = MockZmsNewTokenBuilder
	athenz.ztsNewRoleToken = MockZtsNewRoleToken

	err := athenz.Init()
	assert.NoError(t, err)
	assert.True(t, athenz.roleToken.(*MockRoleToken).isPrefetcherStarted)

	data, err := athenz.GetData()
	assert.Equal(t, []byte("mockRoleToken"), data)
	assert.NoError(t, err)

	err = athenz.Close()
	assert.NoError(t, err)
	assert.False(t, athenz.roleToken.(*MockRoleToken).isPrefetcherStarted)
}

func TestAthenzAuthWithProxy(t *testing.T) {
	privateKey := "file://" + clientKeyPath
	provider := NewAuthenticationAthenz(
		"pulsar.test.provider",  // providerDomain
		"pulsar.test.tenant",    // tenantDomain
		"service",               // tenantService
		privateKey,              // privateKey
		"",                      // keyID
		"",                      // x509CertChain
		"",                      // caCert
		"",                      // principalHeader
		"",                      // roleHeader
		"http://localhost:9999", // ztsURL
		"http://localhost:8080") // ztsProxyURL

	// inject mock function
	athenz := provider.(*athenzAuthProvider)
	athenz.zmsNewTokenBuilder = MockZmsNewTokenBuilder
	athenz.ztsNewRoleToken = MockZtsNewRoleToken

	err := athenz.Init()
	assert.NoError(t, err)
	assert.True(t, athenz.roleToken.(*MockRoleToken).isPrefetcherStarted)

	data, err := athenz.GetData()
	assert.Equal(t, []byte("mockRoleToken-http://localhost:8080"), data)
	assert.NoError(t, err)

	err = athenz.Close()
	assert.NoError(t, err)
	assert.False(t, athenz.roleToken.(*MockRoleToken).isPrefetcherStarted)
}

func TestCopperArgos(t *testing.T) {
	privateKey := "file://" + clientKeyPath
	x509CertChain := "file://" + clientCertPath
	caCert := "file://" + caCertPath

	provider := NewAuthenticationAthenz(
		"pulsar.test.provider",  // providerDomain
		"",                      // tenantDomain
		"",                      // tenantService
		privateKey,              // privateKey
		"",                      // keyID
		x509CertChain,           // x509CertChain
		caCert,                  // caCert
		"",                      // principalHeader
		"",                      // roleHeader
		"http://localhost:9999", // ztsURL
		"")                      // ztsProxyURL

	// inject mock function
	athenz := provider.(*athenzAuthProvider)
	athenz.ztsNewRoleTokenFromCert = MockZtsNewRoleTokenFromCert

	err := athenz.Init()
	assert.NoError(t, err)
	assert.True(t, athenz.roleToken.(*MockRoleToken).isPrefetcherStarted)

	data, err := athenz.GetData()
	assert.Equal(t, []byte("mockRoleTokenFromCert"), data)
	assert.NoError(t, err)

	err = athenz.Close()
	assert.NoError(t, err)
	assert.False(t, athenz.roleToken.(*MockRoleToken).isPrefetcherStarted)
}

func TestCopperArgosWithProxy(t *testing.T) {
	privateKey := "file://" + clientKeyPath
	x509CertChain := "file://" + clientCertPath
	caCert := "file://" + caCertPath

	provider := NewAuthenticationAthenz(
		"pulsar.test.provider",  // providerDomain
		"",                      // tenantDomain
		"",                      // tenantService
		privateKey,              // privateKey
		"",                      // keyID
		x509CertChain,           // x509CertChain
		caCert,                  // caCert
		"",                      // principalHeader
		"",                      // roleHeader
		"http://localhost:9999", // ztsURL
		"http://localhost:8080") // ztsProxyURL

	// inject mock function
	athenz := provider.(*athenzAuthProvider)
	athenz.ztsNewRoleTokenFromCert = MockZtsNewRoleTokenFromCert

	err := athenz.Init()
	assert.NoError(t, err)
	assert.True(t, athenz.roleToken.(*MockRoleToken).isPrefetcherStarted)

	data, err := athenz.GetData()
	assert.Equal(t, []byte("mockRoleTokenFromCert-http://localhost:8080"), data)
	assert.NoError(t, err)

	err = athenz.Close()
	assert.NoError(t, err)
	assert.False(t, athenz.roleToken.(*MockRoleToken).isPrefetcherStarted)
}

func TestIllegalParams(t *testing.T) {
	privateKey := "file://" + clientKeyPath
	x509CertChain := "file://" + clientCertPath

	provider := NewAuthenticationAthenz(
		"pulsar.test.provider",  // providerDomain
		"",                      // tenantDomain
		"",                      // tenantService
		"",                      // privateKey
		"",                      // keyID
		"",                      // x509CertChain
		"",                      // caCert
		"",                      // principalHeader
		"",                      // roleHeader
		"http://localhost:9999", // ztsURL
		"")                      // ztsProxyURL
	athenz := provider.(*athenzAuthProvider)

	err := athenz.Init()
	assert.Error(t, err, "Should fail due to missing privateKey parameter")
	assert.Equal(t, "missing required parameters", err.Error())

	provider = NewAuthenticationAthenz(
		"pulsar.test.provider",  // providerDomain
		"pulsar.test.tenant",    // tenantDomain
		"",                      // tenantService
		privateKey,              // privateKey
		"",                      // keyID
		"",                      // x509CertChain
		"",                      // caCert
		"",                      // principalHeader
		"",                      // roleHeader
		"http://localhost:9999", // ztsURL
		"")                      // ztsProxyURL
	athenz = provider.(*athenzAuthProvider)

	err = athenz.Init()
	assert.Error(t, err, "Should fail due to missing tenantService parameter")
	assert.Equal(t, "missing required parameters", err.Error())

	provider = NewAuthenticationAthenz(
		"pulsar.test.provider",  // providerDomain
		"",                      // tenantDomain
		"",                      // tenantService
		privateKey,              // privateKey
		"",                      // keyID
		"data:foo",              // x509CertChain
		"",                      // caCert
		"",                      // principalHeader
		"",                      // roleHeader
		"http://localhost:9999", // ztsURL
		"")                      // ztsProxyURL
	athenz = provider.(*athenzAuthProvider)

	err = athenz.Init()
	assert.Error(t, err, "Should fail due to incorrect x509CertChain scheme")
	assert.Equal(t, "x509CertChain and privateKey must be specified as file paths", err.Error())

	provider = NewAuthenticationAthenz(
		"pulsar.test.provider",  // providerDomain
		"",                      // tenantDomain
		"",                      // tenantService
		"data:bar",              // privateKey
		"",                      // keyID
		x509CertChain,           // x509CertChain
		"",                      // caCert
		"",                      // principalHeader
		"",                      // roleHeader
		"http://localhost:9999", // ztsURL
		"")                      // ztsProxyURL
	athenz = provider.(*athenzAuthProvider)

	err = athenz.Init()
	assert.Error(t, err, "Should fail due to incorrect privateKey scheme")
	assert.Equal(t, "x509CertChain and privateKey must be specified as file paths", err.Error())
}
