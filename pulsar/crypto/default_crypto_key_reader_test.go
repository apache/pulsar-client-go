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

package crypto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPublicKey(t *testing.T) {
	keyReader := NewFileKeyReader("../crypto/testdata/pub_key_rsa.pem", "")
	keyInfo, err := keyReader.PublicKey("test-key", map[string]string{"key": "value"})

	assert.Nil(t, err)
	assert.NotNil(t, keyInfo)
	assert.NotEmpty(t, keyInfo.Metadata())
	assert.NotEmpty(t, keyInfo.Name())
	assert.NotEmpty(t, keyInfo.Key())
	assert.Equal(t, "value", keyInfo.metadata["key"])
}

func TestGetPrivateKey(t *testing.T) {
	keyReader := NewFileKeyReader("", "../crypto/testdata/pri_key_rsa.pem")
	keyInfo, err := keyReader.PrivateKey("test-key", map[string]string{"key": "value"})

	assert.Nil(t, err)
	assert.NotNil(t, keyInfo)
	assert.NotEmpty(t, keyInfo.Metadata())
	assert.NotEmpty(t, keyInfo.Name())
	assert.NotEmpty(t, keyInfo.Key())
	assert.Equal(t, "value", keyInfo.metadata["key"])
}

func TestInvalidKeyPath(t *testing.T) {
	keyReader := NewFileKeyReader("../crypto/testdata/no_pub_key_rsa.pem", "../crypto/testdata/no_pri_key_rsa.pem")

	// try to read public key
	keyInfo, err := keyReader.PublicKey("test-pub-key", nil)
	assert.Nil(t, keyInfo)
	assert.NotNil(t, err)

	// try to read private key
	keyInfo, err = keyReader.PrivateKey("test-pri-key", nil)
	assert.Nil(t, keyInfo)
	assert.NotNil(t, err)
}
