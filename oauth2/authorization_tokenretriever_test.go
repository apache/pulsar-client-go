// Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.

package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type MockTransport struct {
	Responses   []*http.Response
	ReturnError error
}

var _ HTTPAuthTransport = &MockTransport{}

func (t *MockTransport) Do(req *http.Request) (*http.Response, error) {
	if len(t.Responses) > 0 {
		r := t.Responses[0]
		t.Responses = t.Responses[1:]
		return r, nil
	}
	return nil, t.ReturnError
}

var _ = Describe("CodetokenExchanger", func() {
	Describe("newExchangeCodeRequest", func() {
		It("creates the request", func() {
			tokenRetriever := TokenRetriever{
				oidcWellKnownEndpoints: OIDCWellKnownEndpoints{TokenEndpoint: "https://issuer/oauth/token"}}
			exchangeRequest := AuthorizationCodeExchangeRequest{
				ClientID:     "clientID",
				CodeVerifier: "Verifier",
				Code:         "code",
				RedirectURI:  "https://redirect",
			}

			result, err := tokenRetriever.newExchangeCodeRequest(exchangeRequest)

			result.ParseForm()

			Expect(err).To(BeNil())
			Expect(result.FormValue("grant_type")).To(Equal("authorization_code"))
			Expect(result.FormValue("client_id")).To(Equal("clientID"))
			Expect(result.FormValue("code_verifier")).To(Equal("Verifier"))
			Expect(result.FormValue("code")).To(Equal("code"))
			Expect(result.FormValue("redirect_uri")).To(Equal("https://redirect"))
			Expect(result.URL.String()).To(Equal("https://issuer/oauth/token"))

			Expect(result.Header.Get("Content-Type")).To(Equal("application/x-www-form-urlencoded"))
			Expect(result.Header.Get("Content-Length")).To(Equal("117"))
		})

		It("returns an error when NewRequest returns an error", func() {
			tokenRetriever := TokenRetriever{
				oidcWellKnownEndpoints: OIDCWellKnownEndpoints{TokenEndpoint: "://issuer/oauth/token"}}

			result, err := tokenRetriever.newExchangeCodeRequest(AuthorizationCodeExchangeRequest{})

			Expect(result).To(BeNil())
			Expect(err.Error()).To(Equal("parse ://issuer/oauth/token: missing protocol scheme"))
		})
	})

	Describe("handleAuthTokensResponse", func() {
		It("handles the response", func() {
			tokenRetriever := TokenRetriever{}
			response := buildResponse(200, AuthorizationTokenResponse{
				ExpiresIn:    1,
				AccessToken:  "myAccessToken",
				RefreshToken: "myRefreshToken",
			})

			result, err := tokenRetriever.handleAuthTokensResponse(response)

			Expect(err).To(BeNil())
			Expect(result).To(Equal(&TokenResult{
				ExpiresIn:    1,
				AccessToken:  "myAccessToken",
				RefreshToken: "myRefreshToken",
			}))
		})

		It("returns error when status code is not successful", func() {
			tokenRetriever := TokenRetriever{}
			response := buildResponse(500, nil)

			result, err := tokenRetriever.handleAuthTokensResponse(response)

			Expect(result).To(BeNil())
			Expect(err.Error()).To(Equal("a non-success status code was received: 500"))
		})

		It("returns typed error when response body contains error information", func() {
			errorBody := TokenErrorResponse{Error: "test", ErrorDescription: "test description"}
			tokenRetriever := TokenRetriever{}
			response := buildResponse(400, errorBody)

			result, err := tokenRetriever.handleAuthTokensResponse(response)

			Expect(result).To(BeNil())
			Expect(err).To(Equal(&TokenError{ErrorCode: "test", ErrorDescription: "test description"}))
			Expect(err.Error()).To(Equal("test description (test)"))
		})

		It("returns error when deserialization fails", func() {
			tokenRetriever := TokenRetriever{}
			response := buildResponse(200, "")

			result, err := tokenRetriever.handleAuthTokensResponse(response)
			Expect(result).To(BeNil())
			Expect(err.Error()).To(Equal(
				"json: cannot unmarshal string into Go value of type auth.AuthorizationTokenResponse"))
		})
	})

	Describe("newRefreshTokenRequest", func() {
		It("creates the request", func() {
			tokenRetriever := TokenRetriever{
				oidcWellKnownEndpoints: OIDCWellKnownEndpoints{TokenEndpoint: "https://issuer/oauth/token"}}
			exchangeRequest := RefreshTokenExchangeRequest{
				ClientID:     "clientID",
				RefreshToken: "refreshToken",
			}

			result, err := tokenRetriever.newRefreshTokenRequest(exchangeRequest)

			result.ParseForm()

			Expect(err).To(BeNil())
			Expect(result.FormValue("grant_type")).To(Equal("refresh_token"))
			Expect(result.FormValue("client_id")).To(Equal("clientID"))
			Expect(result.FormValue("refresh_token")).To(Equal("refreshToken"))
			Expect(result.URL.String()).To(Equal("https://issuer/oauth/token"))

			Expect(result.Header.Get("Content-Type")).To(Equal("application/x-www-form-urlencoded"))
			Expect(result.Header.Get("Content-Length")).To(Equal("70"))
		})

		It("returns an error when NewRequest returns an error", func() {
			tokenRetriever := TokenRetriever{
				oidcWellKnownEndpoints: OIDCWellKnownEndpoints{TokenEndpoint: "://issuer/oauth/token"}}

			result, err := tokenRetriever.newRefreshTokenRequest(RefreshTokenExchangeRequest{})

			Expect(result).To(BeNil())
			Expect(err.Error()).To(Equal("parse ://issuer/oauth/token: missing protocol scheme"))
		})
	})

	Describe("newClientCredentialsRequest", func() {
		It("creates the request", func() {
			tokenRetriever := TokenRetriever{
				oidcWellKnownEndpoints: OIDCWellKnownEndpoints{TokenEndpoint: "https://issuer/oauth/token"}}
			exchangeRequest := ClientCredentialsExchangeRequest{
				ClientID:     "clientID",
				ClientSecret: "clientSecret",
				Audience:     "audience",
			}

			result, err := tokenRetriever.newClientCredentialsRequest(exchangeRequest)

			result.ParseForm()

			Expect(err).To(BeNil())
			Expect(result.FormValue("grant_type")).To(Equal("client_credentials"))
			Expect(result.FormValue("client_id")).To(Equal("clientID"))
			Expect(result.FormValue("client_secret")).To(Equal("clientSecret"))
			Expect(result.FormValue("audience")).To(Equal("audience"))
			Expect(result.URL.String()).To(Equal("https://issuer/oauth/token"))

			Expect(result.Header.Get("Content-Type")).To(Equal("application/x-www-form-urlencoded"))
			Expect(result.Header.Get("Content-Length")).To(Equal("93"))
		})

		It("returns an error when NewRequest returns an error", func() {
			tokenRetriever := TokenRetriever{
				oidcWellKnownEndpoints: OIDCWellKnownEndpoints{TokenEndpoint: "://issuer/oauth/token"}}

			result, err := tokenRetriever.newClientCredentialsRequest(ClientCredentialsExchangeRequest{})

			Expect(result).To(BeNil())
			Expect(err.Error()).To(Equal("parse ://issuer/oauth/token: missing protocol scheme"))
		})
	})

	Describe("newDeviceCodeExchangeRequest", func() {
		It("creates the request", func() {
			tokenRetriever := TokenRetriever{
				oidcWellKnownEndpoints: OIDCWellKnownEndpoints{TokenEndpoint: "https://issuer/oauth/token"}}
			exchangeRequest := DeviceCodeExchangeRequest{
				ClientID:     "clientID",
				DeviceCode:   "deviceCode",
				PollInterval: time.Duration(5) * time.Second,
			}

			result, err := tokenRetriever.newDeviceCodeExchangeRequest(exchangeRequest)

			result.ParseForm()

			Expect(err).To(BeNil())
			Expect(result.FormValue("grant_type")).To(Equal("urn:ietf:params:oauth:grant-type:device_code"))
			Expect(result.FormValue("client_id")).To(Equal("clientID"))
			Expect(result.FormValue("device_code")).To(Equal("deviceCode"))
			Expect(result.URL.String()).To(Equal("https://issuer/oauth/token"))

			Expect(result.Header.Get("Content-Type")).To(Equal("application/x-www-form-urlencoded"))
			Expect(result.Header.Get("Content-Length")).To(Equal("107"))
		})

		It("returns an error when NewRequest returns an error", func() {
			tokenRetriever := TokenRetriever{
				oidcWellKnownEndpoints: OIDCWellKnownEndpoints{TokenEndpoint: "://issuer/oauth/token"}}

			result, err := tokenRetriever.newClientCredentialsRequest(ClientCredentialsExchangeRequest{})

			Expect(result).To(BeNil())
			Expect(err.Error()).To(Equal("parse ://issuer/oauth/token: missing protocol scheme"))
		})
	})

	Describe("ExchangeDeviceCode", func() {
		var mockTransport *MockTransport
		var tokenRetriever *TokenRetriever
		var exchangeRequest DeviceCodeExchangeRequest
		var tokenResult TokenResult

		BeforeEach(func() {
			mockTransport = &MockTransport{}
			tokenRetriever = &TokenRetriever{
				oidcWellKnownEndpoints: OIDCWellKnownEndpoints{TokenEndpoint: "https://issuer/oauth/token"},
				transport:              mockTransport,
			}
			exchangeRequest = DeviceCodeExchangeRequest{
				ClientID:     "clientID",
				DeviceCode:   "deviceCode",
				PollInterval: time.Duration(1) * time.Second,
			}
			tokenResult = TokenResult{
				ExpiresIn:    1,
				AccessToken:  "myAccessToken",
				RefreshToken: "myRefreshToken",
			}
		})

		It("returns a token", func() {
		})

		It("supports cancellation", func() {
			mockTransport.Responses = []*http.Response{
				buildResponse(400, &TokenErrorResponse{"authorization_pending", ""}),
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := tokenRetriever.ExchangeDeviceCode(ctx, exchangeRequest)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(Equal("cancelled"))
		})

		It("implements authorization_pending and slow_down", func() {
			startTime := time.Now()
			mockTransport.Responses = []*http.Response{
				buildResponse(400, &TokenErrorResponse{"authorization_pending", ""}),
				buildResponse(400, &TokenErrorResponse{"authorization_pending", ""}),
				buildResponse(400, &TokenErrorResponse{"slow_down", ""}),
				buildResponse(200, &tokenResult),
			}
			token, err := tokenRetriever.ExchangeDeviceCode(context.Background(), exchangeRequest)
			Expect(err).To(BeNil())
			Expect(token).To(Equal(&tokenResult))
			endTime := time.Now()
			Expect(endTime.Sub(startTime)).To(BeNumerically(">", exchangeRequest.PollInterval*3))
		})

		It("implements expired_token", func() {
			mockTransport.Responses = []*http.Response{
				buildResponse(400, &TokenErrorResponse{"expired_token", ""}),
			}
			_, err := tokenRetriever.ExchangeDeviceCode(context.Background(), exchangeRequest)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(Equal("the device code has expired"))
		})

		It("implements access_denied", func() {
			mockTransport.Responses = []*http.Response{
				buildResponse(400, &TokenErrorResponse{"access_denied", ""}),
			}
			_, err := tokenRetriever.ExchangeDeviceCode(context.Background(), exchangeRequest)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(Equal("the device was not authorized"))
		})
	})
})

func buildResponse(statusCode int, body interface{}) *http.Response {
	b, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	resp := &http.Response{
		StatusCode: statusCode,
		Header:     map[string][]string{},
		Body:       ioutil.NopCloser(bytes.NewReader(b)),
	}
	if strings.HasPrefix(string(b), "{") {
		resp.Header.Add("Content-Type", "application/json")
	}

	return resp
}
