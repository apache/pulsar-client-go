// Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.

package auth

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
