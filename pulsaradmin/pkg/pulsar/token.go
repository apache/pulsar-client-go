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
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common/algorithm/algorithm"
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common/algorithm/keypair"
)

type Token interface {
	// CreateKeyPair is used to create public and private key pair using the given signature algorithm
	CreateKeyPair(algorithm.Algorithm) (*keypair.KeyPair, error)
}

type token struct {
	pulsar *pulsarClient
}

func (c *pulsarClient) Token() Token {
	return &token{
		pulsar: c,
	}
}

func (c *token) CreateKeyPair(signatureAlgorithm algorithm.Algorithm) (*keypair.KeyPair, error) {
	sa, err := algorithm.GetSignatureAlgorithm(signatureAlgorithm)
	if err != nil {
		return nil, err
	}
	return sa.GenerateKeyPair()
}
