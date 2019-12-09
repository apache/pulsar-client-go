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

package algorithm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testData = []struct {
	name      string
	algorithm Algorithm
}{
	{"RSA", RS256},
	{"RSA", RS384},
	{"RSA", RS512},
	{"ECDSA", ES256},
	{"ECDSA", ES384},
	{"ECDSA", ES512},
	{"INVALID", Algorithm("INVALID")},
}

func TestGetSignatureAlgorithm(t *testing.T) {
	for _, data := range testData {
		t.Logf("test case: %+v", data)
		switch data.name {
		case "RSA":
			testRSA(t, data.algorithm)
		case "ECDSA":
			testECDSA(t, data.algorithm)
		default:
			sa, err := GetSignatureAlgorithm(data.algorithm)
			assert.Nil(t, sa)
			assert.NotNil(t, err)
			assert.Equal(t,
				fmt.Sprintf("the signature algorithm '%s' is invalid. Valid options are: "+
					"'RS256', 'RS384', 'RS512', 'ES256', 'ES384', 'ES512'\n", data.algorithm),
				err.Error())
		}
	}
}

func testRSA(t *testing.T, algorithm Algorithm) {
	sa, err := GetSignatureAlgorithm(algorithm)
	assert.Nil(t, err)

	kp, err := sa.GenerateKeyPair()
	assert.Nil(t, err)
	assert.NotNil(t, kp)
	_, err = kp.EncodedPrivateKey()
	assert.Nil(t, err)
	_, err = kp.EncodedPublicKey()
	assert.Nil(t, err)

	rsaPrivateKey, err := kp.GetRsaPrivateKey()
	assert.Nil(t, err)
	assert.NotNil(t, rsaPrivateKey)

	ecdsaPrivateKey, err := kp.GetEcdsaPrivateKey()
	assert.Nil(t, ecdsaPrivateKey)
	assert.NotNil(t, err)
	assert.Equal(t,
		"the private key is not generated using ECDSA signature algorithm",
		err.Error())
}

func testECDSA(t *testing.T, algorithm Algorithm) {
	sa, err := GetSignatureAlgorithm(algorithm)
	assert.Nil(t, err)

	kp, err := sa.GenerateKeyPair()
	assert.Nil(t, err)
	assert.NotNil(t, kp)
	_, err = kp.EncodedPrivateKey()
	assert.Nil(t, err)
	_, err = kp.EncodedPublicKey()
	assert.Nil(t, err)

	ecdsaPrivateKey, err := kp.GetEcdsaPrivateKey()
	assert.Nil(t, err)
	assert.NotNil(t, ecdsaPrivateKey)

	rsaPrivateKey, err := kp.GetRsaPrivateKey()
	assert.Nil(t, rsaPrivateKey)
	assert.NotNil(t, err)
	assert.Equal(t,
		"the private key is not generated using RSA signature algorithm",
		err.Error())
}
