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
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewAuthenticationBasicWithParams(t *testing.T) {
	username := "admin"
	password := "123456"

	provider, err := NewAuthenticationBasic(username, password)
	require.NoError(t, err)
	require.NotNil(t, provider)

	data, err := provider.GetData()
	require.NoError(t, err)
	require.Equal(t, []byte(username+":"+password), data)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.Header.Get("Authorization")))
	}))

	client := s.Client()
	err = provider.WithTransport(client.Transport)
	require.NoError(t, err)
	client.Transport = provider

	resp, err := client.Get(s.URL)
	require.NoError(t, err)

	body, err := ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, []byte("Basic YWRtaW46MTIzNDU2"), body)
}

func TestNewAuthenticationBasicWithInvalidParams(t *testing.T) {
	username := "admin"
	password := "123456"
	provider, err := NewAuthenticationBasic("", password)
	require.Equal(t, errors.New("username cannot be empty"), err)
	require.Nil(t, provider)

	provider, err = NewAuthenticationBasic(username, "")
	require.Equal(t, errors.New("password cannot be empty"), err)
	require.Nil(t, provider)
}
