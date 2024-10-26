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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

type MockTransport struct {
	Responses   []*http.Response
	ReturnError error
}

var _ HTTPAuthTransport = &MockTransport{}

func (t *MockTransport) Do(_ *http.Request) (*http.Response, error) {
	if len(t.Responses) > 0 {
		r := t.Responses[0]
		t.Responses = t.Responses[1:]
		return r, nil
	}
	return nil, t.ReturnError
}

var _ = ginkgo.Describe("CodetokenExchanger", func() {
	ginkgo.Describe("newExchangeCodeRequest", func() {
		ginkgo.It("creates the request", func() {
			tokenRetriever := TokenRetriever{}
			exchangeRequest := AuthorizationCodeExchangeRequest{
				TokenEndpoint: "https://issuer/oauth/token",
				ClientID:      "clientID",
				CodeVerifier:  "Verifier",
				Code:          "code",
				RedirectURI:   "https://redirect",
			}

			result, err := tokenRetriever.newExchangeCodeRequest(exchangeRequest)

			result.ParseForm()

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(result.FormValue("grant_type")).To(gomega.Equal("authorization_code"))
			gomega.Expect(result.FormValue("client_id")).To(gomega.Equal("clientID"))
			gomega.Expect(result.FormValue("code_verifier")).To(gomega.Equal("Verifier"))
			gomega.Expect(result.FormValue("code")).To(gomega.Equal("code"))
			gomega.Expect(result.FormValue("redirect_uri")).To(gomega.Equal("https://redirect"))
			gomega.Expect(result.URL.String()).To(gomega.Equal("https://issuer/oauth/token"))

			gomega.Expect(result.Header.Get("Content-Type")).To(gomega.Equal("application/x-www-form-urlencoded"))
			gomega.Expect(result.Header.Get("Content-Length")).To(gomega.Equal("117"))
		})

		ginkgo.It("returns an error when NewRequest returns an error", func() {
			tokenRetriever := TokenRetriever{}

			result, err := tokenRetriever.newExchangeCodeRequest(AuthorizationCodeExchangeRequest{
				TokenEndpoint: "://issuer/oauth/token",
			})

			gomega.Expect(result).To(gomega.BeNil())
			gomega.Expect(err.Error()).To(gomega.Equal("parse \"://issuer/oauth/token\": missing protocol scheme"))
		})
	})

	ginkgo.Describe("handleAuthTokensResponse", func() {
		ginkgo.It("handles the response", func() {
			tokenRetriever := TokenRetriever{}
			response := buildResponse(200, AuthorizationTokenResponse{
				ExpiresIn:    1,
				AccessToken:  "myAccessToken",
				RefreshToken: "myRefreshToken",
			})

			result, err := tokenRetriever.handleAuthTokensResponse(response)

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(result).To(gomega.Equal(&TokenResult{
				ExpiresIn:    1,
				AccessToken:  "myAccessToken",
				RefreshToken: "myRefreshToken",
			}))
		})

		ginkgo.It("returns error when status code is not successful", func() {
			tokenRetriever := TokenRetriever{}
			response := buildResponse(500, nil)

			result, err := tokenRetriever.handleAuthTokensResponse(response)

			gomega.Expect(result).To(gomega.BeNil())
			gomega.Expect(err.Error()).To(gomega.Not(gomega.BeNil()))
		})

		ginkgo.It("returns typed error when response body contains error information", func() {
			errorBody := TokenErrorResponse{Error: "test", ErrorDescription: "test description"}
			tokenRetriever := TokenRetriever{}
			response := buildResponse(400, errorBody)

			result, err := tokenRetriever.handleAuthTokensResponse(response)

			gomega.Expect(result).To(gomega.BeNil())
			gomega.Expect(err).To(gomega.Equal(&TokenError{ErrorCode: "test", ErrorDescription: "test description"}))
			gomega.Expect(err.Error()).To(gomega.Equal("test description (test)"))
		})

		ginkgo.It("returns error when deserialization fails", func() {
			tokenRetriever := TokenRetriever{}
			response := buildResponse(200, "")

			result, err := tokenRetriever.handleAuthTokensResponse(response)
			gomega.Expect(result).To(gomega.BeNil())
			gomega.Expect(err.Error()).To(gomega.Equal(
				"json: cannot unmarshal string into Go value of type oauth2.AuthorizationTokenResponse"))
		})
	})

	ginkgo.Describe("newRefreshTokenRequest", func() {
		ginkgo.It("creates the request", func() {
			tokenRetriever := TokenRetriever{}
			exchangeRequest := RefreshTokenExchangeRequest{
				TokenEndpoint: "https://issuer/oauth/token",
				ClientID:      "clientID",
				RefreshToken:  "refreshToken",
			}

			result, err := tokenRetriever.newRefreshTokenRequest(exchangeRequest)

			result.ParseForm()

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(result.FormValue("grant_type")).To(gomega.Equal("refresh_token"))
			gomega.Expect(result.FormValue("client_id")).To(gomega.Equal("clientID"))
			gomega.Expect(result.FormValue("refresh_token")).To(gomega.Equal("refreshToken"))
			gomega.Expect(result.URL.String()).To(gomega.Equal("https://issuer/oauth/token"))

			gomega.Expect(result.Header.Get("Content-Type")).To(gomega.Equal("application/x-www-form-urlencoded"))
			gomega.Expect(result.Header.Get("Content-Length")).To(gomega.Equal("70"))
		})

		ginkgo.It("returns an error when NewRequest returns an error", func() {
			tokenRetriever := TokenRetriever{}

			result, err := tokenRetriever.newRefreshTokenRequest(RefreshTokenExchangeRequest{
				TokenEndpoint: "://issuer/oauth/token",
			})

			gomega.Expect(result).To(gomega.BeNil())
			gomega.Expect(err.Error()).To(gomega.Equal("parse \"://issuer/oauth/token\": missing protocol scheme"))
		})
	})

	ginkgo.Describe("newClientCredentialsRequest", func() {
		ginkgo.It("creates the request", func() {
			tokenRetriever := TokenRetriever{}
			exchangeRequest := ClientCredentialsExchangeRequest{
				TokenEndpoint: "https://issuer/oauth/token",
				ClientID:      "clientID",
				ClientSecret:  "clientSecret",
				Audience:      "audience",
			}

			result, err := tokenRetriever.newClientCredentialsRequest(exchangeRequest)

			result.ParseForm()

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(result.FormValue("grant_type")).To(gomega.Equal("client_credentials"))
			gomega.Expect(result.FormValue("client_id")).To(gomega.Equal("clientID"))
			gomega.Expect(result.FormValue("client_secret")).To(gomega.Equal("clientSecret"))
			gomega.Expect(result.FormValue("audience")).To(gomega.Equal("audience"))
			gomega.Expect(result.URL.String()).To(gomega.Equal("https://issuer/oauth/token"))

			gomega.Expect(result.Header.Get("Content-Type")).To(gomega.Equal("application/x-www-form-urlencoded"))
			gomega.Expect(result.Header.Get("Content-Length")).To(gomega.Equal("93"))
		})

		ginkgo.It("returns an error when NewRequest returns an error", func() {
			tokenRetriever := TokenRetriever{}

			result, err := tokenRetriever.newClientCredentialsRequest(ClientCredentialsExchangeRequest{
				TokenEndpoint: "://issuer/oauth/token",
			})

			gomega.Expect(result).To(gomega.BeNil())
			gomega.Expect(err.Error()).To(gomega.Equal("parse \"://issuer/oauth/token\": missing protocol scheme"))
		})
	})

	ginkgo.Describe("newDeviceCodeExchangeRequest", func() {
		ginkgo.It("creates the request", func() {
			tokenRetriever := TokenRetriever{}
			exchangeRequest := DeviceCodeExchangeRequest{
				TokenEndpoint: "https://issuer/oauth/token",
				ClientID:      "clientID",
				DeviceCode:    "deviceCode",
				PollInterval:  time.Duration(5) * time.Second,
			}

			result, err := tokenRetriever.newDeviceCodeExchangeRequest(exchangeRequest)

			result.ParseForm()

			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(result.FormValue("grant_type")).To(gomega.Equal("urn:ietf:params:oauth:grant-type:device_code"))
			gomega.Expect(result.FormValue("client_id")).To(gomega.Equal("clientID"))
			gomega.Expect(result.FormValue("device_code")).To(gomega.Equal("deviceCode"))
			gomega.Expect(result.URL.String()).To(gomega.Equal("https://issuer/oauth/token"))

			gomega.Expect(result.Header.Get("Content-Type")).To(gomega.Equal("application/x-www-form-urlencoded"))
			gomega.Expect(result.Header.Get("Content-Length")).To(gomega.Equal("107"))
		})

		ginkgo.It("returns an error when NewRequest returns an error", func() {
			tokenRetriever := TokenRetriever{}

			result, err := tokenRetriever.newClientCredentialsRequest(ClientCredentialsExchangeRequest{
				TokenEndpoint: "://issuer/oauth/token",
			})

			gomega.Expect(result).To(gomega.BeNil())
			gomega.Expect(err.Error()).To(gomega.Equal("parse \"://issuer/oauth/token\": missing protocol scheme"))
		})
	})

	ginkgo.Describe("ExchangeDeviceCode", func() {
		var mockTransport *MockTransport
		var tokenRetriever *TokenRetriever
		var exchangeRequest DeviceCodeExchangeRequest
		var tokenResult TokenResult

		ginkgo.BeforeEach(func() {
			mockTransport = &MockTransport{}
			tokenRetriever = &TokenRetriever{
				transport: mockTransport,
			}
			exchangeRequest = DeviceCodeExchangeRequest{
				TokenEndpoint: "https://issuer/oauth/token",
				ClientID:      "clientID",
				DeviceCode:    "deviceCode",
				PollInterval:  time.Duration(1) * time.Second,
			}
			tokenResult = TokenResult{
				ExpiresIn:    1,
				AccessToken:  "myAccessToken",
				RefreshToken: "myRefreshToken",
			}
		})

		ginkgo.It("returns a token", func() {
		})

		ginkgo.It("supports cancellation", func() {
			mockTransport.Responses = []*http.Response{
				buildResponse(400, &TokenErrorResponse{"authorization_pending", ""}),
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := tokenRetriever.ExchangeDeviceCode(ctx, exchangeRequest)
			gomega.Expect(err).ToNot(gomega.BeNil())
			gomega.Expect(err.Error()).To(gomega.Equal("cancelled"))
		})

		ginkgo.It("implements authorization_pending and slow_down", func() {
			startTime := time.Now()
			mockTransport.Responses = []*http.Response{
				buildResponse(400, &TokenErrorResponse{"authorization_pending", ""}),
				buildResponse(400, &TokenErrorResponse{"authorization_pending", ""}),
				buildResponse(400, &TokenErrorResponse{"slow_down", ""}),
				buildResponse(200, &tokenResult),
			}
			token, err := tokenRetriever.ExchangeDeviceCode(context.Background(), exchangeRequest)
			gomega.Expect(err).To(gomega.BeNil())
			gomega.Expect(token).To(gomega.Equal(&tokenResult))
			endTime := time.Now()
			gomega.Expect(endTime.Sub(startTime)).To(gomega.BeNumerically(">", exchangeRequest.PollInterval*3))
		})

		ginkgo.It("implements expired_token", func() {
			mockTransport.Responses = []*http.Response{
				buildResponse(400, &TokenErrorResponse{"expired_token", ""}),
			}
			_, err := tokenRetriever.ExchangeDeviceCode(context.Background(), exchangeRequest)
			gomega.Expect(err).ToNot(gomega.BeNil())
			gomega.Expect(err.Error()).To(gomega.Equal("the device code has expired"))
		})

		ginkgo.It("implements access_denied", func() {
			mockTransport.Responses = []*http.Response{
				buildResponse(400, &TokenErrorResponse{"access_denied", ""}),
			}
			_, err := tokenRetriever.ExchangeDeviceCode(context.Background(), exchangeRequest)
			gomega.Expect(err).ToNot(gomega.BeNil())
			gomega.Expect(err.Error()).To(gomega.Equal("the device was not authorized"))
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
		Body:       io.NopCloser(bytes.NewReader(b)),
	}
	if strings.HasPrefix(string(b), "{") {
		resp.Header.Add("Content-Type", "application/json")
	}

	return resp
}
