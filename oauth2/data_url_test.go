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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDataURL(t *testing.T) {
	rawURL := "data:,test"
	url, err := newDataURL(rawURL)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", url.Mimetype)
	assert.Equal(t, "test", string(url.Data))

	rawURL = "data:;base64," + base64.StdEncoding.EncodeToString([]byte("test"))
	url, err = newDataURL(rawURL)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", url.Mimetype)
	assert.Equal(t, "test", string(url.Data))

	rawURL = "data:application/json,test"
	url, err = newDataURL(rawURL)
	require.NoError(t, err)
	assert.Equal(t, "application/json", url.Mimetype)
	assert.Equal(t, "test", string(url.Data))

	rawURL = "data:application/json;base64," + base64.StdEncoding.EncodeToString([]byte("test"))
	url, err = newDataURL(rawURL)
	require.NoError(t, err)
	assert.Equal(t, "application/json", url.Mimetype)
	assert.Equal(t, "test", string(url.Data))

	rawURL = "data://test"
	url, err = newDataURL(rawURL)
	require.Nil(t, url)
	assert.Error(t, err)
	assert.EqualError(t, errDataURLInvalid, err.Error())
}
