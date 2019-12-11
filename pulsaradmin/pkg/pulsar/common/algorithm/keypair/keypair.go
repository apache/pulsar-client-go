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

package keypair

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"

	"github.com/pkg/errors"
)

type KeyType int

const (
	RSA KeyType = iota
	ECDSA
)

// KeyPair saves the ecdsa private key or the rsa private key and provides
// a get public/private encoded bytes array method
type KeyPair struct {
	keyType    KeyType
	privateKey interface{}
}

func New(keyType KeyType, privateKey interface{}) *KeyPair {
	return &KeyPair{
		keyType:    keyType,
		privateKey: privateKey,
	}
}

// EncodedPrivateKey gets the encoded private key
func (k *KeyPair) EncodedPrivateKey() ([]byte, error) {
	switch k.keyType {
	case RSA:
		key, err := k.GetRsaPrivateKey()
		if err != nil {
			return nil, err
		}
		return x509.MarshalPKCS1PrivateKey(key), err
	case ECDSA:
		key, err := k.GetEcdsaPrivateKey()
		if err != nil {
			return nil, err
		}
		return x509.MarshalECPrivateKey(key)
	}
	return nil, errors.New("unknown error")
}

// DecodePrivateKey parses the private key to a KeyPair
func DecodePrivateKey(keyType KeyType, privateKey []byte) (*KeyPair, error) {
	switch keyType {
	case RSA:
		key, err := x509.ParsePKCS1PrivateKey(privateKey)
		if err != nil {
			k, e := x509.ParsePKCS8PrivateKey(privateKey)
			return New(keyType, k), e
		}
		return New(keyType, key), nil
	case ECDSA:
		key, err := x509.ParseECPrivateKey(privateKey)
		if err != nil {
			k, e := x509.ParsePKCS8PrivateKey(privateKey)
			return New(keyType, k), e
		}
		return New(ECDSA, key), nil
	}
	return nil, errors.New("unknown error")
}

// EncodedPublicKey gets the encoded public key
func (k *KeyPair) EncodedPublicKey() ([]byte, error) {
	switch k.keyType {
	case RSA:
		key, err := k.GetRsaPrivateKey()
		return x509.MarshalPKCS1PublicKey(&key.PublicKey), err
	case ECDSA:
		key, _ := k.GetEcdsaPrivateKey()
		return x509.MarshalPKIXPublicKey(&key.PublicKey)
	}
	return nil, errors.New("unknown error")
}

// GetRsaPrivateKey gets the rsa private key if you are using rsa signature
// algorithm to generate the private key
func (k *KeyPair) GetRsaPrivateKey() (*rsa.PrivateKey, error) {
	if k.keyType != RSA {
		return nil, errors.New("the private key is not generated using RSA signature algorithm")
	}
	if rsaKey, ok := k.privateKey.(*rsa.PrivateKey); ok {
		return rsaKey, nil
	}
	return nil, errors.New("the private key is not generated using RSA signature algorithm")
}

// GetEcdsaPrivateKey gets the ecdsa private key if you are using ecdsa signature
// algorithm to generate the private key
func (k *KeyPair) GetEcdsaPrivateKey() (*ecdsa.PrivateKey, error) {
	if k.keyType != ECDSA {
		return nil, errors.New("the private key is not generated using ECDSA signature algorithm")
	}
	if ecdsaKey, ok := k.privateKey.(*ecdsa.PrivateKey); ok {
		return ecdsaKey, nil
	}
	return nil, errors.New("the private key is not generated using ecdsa signature algorithm")
}
