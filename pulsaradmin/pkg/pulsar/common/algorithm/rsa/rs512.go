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

package rsa

import (
	"crypto/rand"
	"crypto/rsa"

	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common/algorithm/keypair"

	"github.com/pkg/errors"
)

type RS512 struct{}

func (p *RS512) GenerateSecret() ([]byte, error) {
	return nil, errors.New("unsupported operation")
}

func (p *RS512) GenerateKeyPair() (*keypair.KeyPair, error) {
	pri, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, err
	}
	return keypair.New(keypair.RSA, pri), nil
}
