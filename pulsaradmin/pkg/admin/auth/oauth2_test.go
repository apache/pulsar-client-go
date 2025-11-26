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
	"net/http/httptest"
	"os"
	"testing"

	"github.com/apache/pulsar-client-go/oauth2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

// mockOAuthServer will mock a oauth service for the tests
func mockOAuthServer() *httptest.Server {
	// prepare a port for the mocked server
	server := httptest.NewUnstartedServer(http.DefaultServeMux)

	// mock the used REST path for the tests
	mockedHandler := http.NewServeMux()
	mockedHandler.HandleFunc("/.well-known/openid-configuration", func(writer http.ResponseWriter, _ *http.Request) {
		s := fmt.Sprintf(`{
    "issuer":"%s",
    "authorization_endpoint":"%s/authorize",
    "token_endpoint":"%s/oauth/token",
    "device_authorization_endpoint":"%s/oauth/device/code"
}`, server.URL, server.URL, server.URL, server.URL)
		fmt.Fprintln(writer, s)
	})
	mockedHandler.HandleFunc("/oauth/token", func(writer http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(writer, "{\n  \"access_token\": \"token-content\",\n  \"token_type\": \"Bearer\"\n}")
	})
	mockedHandler.HandleFunc("/authorize", func(writer http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(writer, "true")
	})

	server.Config.Handler = mockedHandler
	server.Start()

	return server
}

// mockKeyFile will mock a temp key file for testing.
func mockKeyFile(server string) (string, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	kf, err := os.CreateTemp(pwd, "test_oauth2")
	if err != nil {
		return "", err
	}
	_, err = kf.WriteString(fmt.Sprintf(`{
  "type":"sn_service_account",
  "client_id":"client-id",
  "client_secret":"client-secret",
  "client_email":"oauth@test.org",
  "issuer_url":"%s"
}`, server))

	if err != nil {
		return "", err
	}

	return kf.Name(), nil
}

func TestOauth2(t *testing.T) {
	server := mockOAuthServer()
	defer server.Close()
	kf, err := mockKeyFile(server.URL)
	defer os.Remove(kf)
	if err != nil {
		t.Fatal(errors.Wrap(err, "create mocked key file failed"))
	}

	issuer := oauth2.Issuer{
		IssuerEndpoint: server.URL,
		ClientID:       "client-id",
		Audience:       server.URL,
	}

	auth, err := NewAuthenticationOAuth2(issuer)
	if err != nil {
		t.Fatal(err)
	}

	token, err := auth.source.Token()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "token-content", token.AccessToken)
}
