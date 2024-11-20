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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClientCredentialsProviderFromKeyFile(t *testing.T) {
	oauthType := "TYPE"
	clientID := "CLIENT_ID"
	ClientSecret := "CLIENT_SECRET"
	ClientEmail := "CLIENT_EMAIL"
	IssuerURL := "ISSUER_URL"
	Scope := "SCOPE"
	keyFile := &KeyFile{
		Type:         oauthType,
		ClientID:     clientID,
		ClientSecret: ClientSecret,
		ClientEmail:  ClientEmail,
		IssuerURL:    IssuerURL,
		Scope:        Scope,
	}

	b, err := json.Marshal(keyFile)
	require.NoError(t, err)
	tmpFile, err := os.CreateTemp("", "key-file")
	require.NoError(t, err)
	defer func(name string) {
		_ = os.Remove(name)
	}(tmpFile.Name())
	_, err = tmpFile.Write(b)
	require.NoError(t, err)

	jsonData := string(b)
	base64Data := base64.StdEncoding.EncodeToString(b)

	assertCredentials(t, fmt.Sprintf("file://%s", tmpFile.Name()), keyFile)
	assertCredentials(t, fmt.Sprintf("data://%s", jsonData), keyFile)
	assertCredentials(t, fmt.Sprintf("data:,%s", jsonData), keyFile)
	assertCredentials(t, fmt.Sprintf("data:application/json,%s", jsonData), keyFile)
	assertCredentials(t, fmt.Sprintf("data:;base64,%s", base64Data), keyFile)
	assertCredentials(t, fmt.Sprintf("data:application/json;base64,%s", base64Data), keyFile)
}

func TestNewInvalidClientCredentialsProviderFromKeyFile(t *testing.T) {
	p := NewClientCredentialsProviderFromKeyFile("data:application/data,hi")
	_, err := p.GetClientCredentials()
	require.Error(t, err)
}

func assertCredentials(t *testing.T, keyfile string, expected *KeyFile) {
	p := NewClientCredentialsProviderFromKeyFile(keyfile)
	clientCredentials, err := p.GetClientCredentials()
	require.NoError(t, err)
	assert.Equal(t, expected, clientCredentials)
}
