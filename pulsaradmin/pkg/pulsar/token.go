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

package pulsar

import (
	"strings"

	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common/algorithm/algorithm"
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common/algorithm/keypair"

	"github.com/dgrijalva/jwt-go"
	"github.com/pkg/errors"
)

type Token interface {
	// CreateKeyPair is used to create public and private key pair using the given signature algorithm
	CreateKeyPair(algorithm.Algorithm) (*keypair.KeyPair, error)

	// CreateSecretKey is used for creating a secret key
	CreateSecretKey(algorithm.Algorithm) ([]byte, error)

	// Create creates a token object using the specified signature algorithm, private key,
	// object and the expire time
	Create(algorithm.Algorithm, interface{}, string, int64) (string, error)

	// Validate a token is valid or not
	Validate(algorithm.Algorithm, string, interface{}) (string, int64, error)

	// GetAlgorithm gets which algorithm the token used
	GetAlgorithm(string) (string, error)

	// GetSubject gets the subject of a token
	GetSubject(string) (string, error)
}

type token struct {
	pulsar *pulsarClient
}

func (c *pulsarClient) Token() Token {
	return &token{
		pulsar: c,
	}
}

func (t *token) CreateKeyPair(signatureAlgorithm algorithm.Algorithm) (*keypair.KeyPair, error) {
	sa, err := algorithm.GetSignatureAlgorithm(signatureAlgorithm)
	if err != nil {
		return nil, err
	}
	return sa.GenerateKeyPair()
}

func (t *token) CreateSecretKey(signatureAlgorithm algorithm.Algorithm) ([]byte, error) {
	sa, err := algorithm.GetSignatureAlgorithm(signatureAlgorithm)
	if err != nil {
		return nil, err
	}
	return sa.GenerateSecret()
}

func (t *token) Create(algorithm algorithm.Algorithm, signKey interface{}, subject string,
	expireTime int64) (string, error) {

	claims := &jwt.StandardClaims{
		Subject:   subject,
		ExpiresAt: expireTime,
	}
	signMethod := parseAlgorithmToJwtSignMethod(algorithm)
	tokenString := jwt.NewWithClaims(signMethod, claims)

	return tokenString.SignedString(signKey)
}

func (t *token) Validate(algorithm algorithm.Algorithm, tokenString string,
	signKey interface{}) (string, int64, error) {

	// verify the signature algorithm
	parsedToken, err := jwt.ParseWithClaims(tokenString, &jwt.StandardClaims{},
		func(jt *jwt.Token) (i interface{}, e error) {
			signMethod := parseAlgorithmToJwtSignMethod(algorithm)
			if jt.Method != signMethod {
				return nil, errors.Errorf("unexpected signing method: %s", algorithm)
			}
			return signKey, nil
		})

	// get the subject and the expire time
	if claim, ok := parsedToken.Claims.(*jwt.StandardClaims); parsedToken.Valid && ok {
		return claim.Subject, claim.ExpiresAt, nil
	}

	return "", 0, err
}

func (t *token) GetAlgorithm(tokenString string) (string, error) {
	parts := strings.Split(tokenString, ".")
	algorithm, err := jwt.DecodeSegment(parts[0])
	if err != nil {
		return "", err
	}
	return string(algorithm), nil
}

func (t *token) GetSubject(tokenString string) (string, error) {
	parts := strings.Split(tokenString, ".")
	algorithm, err := jwt.DecodeSegment(parts[1])
	if err != nil {
		return "", err
	}
	return string(algorithm), nil
}

func parseAlgorithmToJwtSignMethod(a algorithm.Algorithm) jwt.SigningMethod {
	switch a {
	case algorithm.HS256:
		return jwt.SigningMethodHS256
	case algorithm.HS384:
		return jwt.SigningMethodHS384
	case algorithm.HS512:
		return jwt.SigningMethodHS512
	case algorithm.RS256:
		return jwt.SigningMethodRS256
	case algorithm.RS384:
		return jwt.SigningMethodRS384
	case algorithm.RS512:
		return jwt.SigningMethodRS512
	case algorithm.ES256:
		return jwt.SigningMethodES256
	case algorithm.ES384:
		return jwt.SigningMethodES384
	case algorithm.ES512:
		return jwt.SigningMethodES512
	case algorithm.PS256:
		return jwt.SigningMethodPS256
	case algorithm.PS384:
		return jwt.SigningMethodPS384
	case algorithm.PS512:
		return jwt.SigningMethodPS512
	default:
		return jwt.SigningMethodRS256
	}
}
