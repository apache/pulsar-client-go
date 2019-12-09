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
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common/algorithm/ecdsa"
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common/algorithm/keypair"
	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common/algorithm/rsa"

	"github.com/pkg/errors"
)

type Algorithm string

const (
	RS256 = "RS256"
	RS384 = "RS384"
	RS512 = "RS512"
	ES256 = "ES256"
	ES384 = "ES384"
	ES512 = "ES512"
)

var algorithmMap = map[Algorithm]SignatureAlgorithm{
	RS256: new(rsa.RS256),
	RS384: new(rsa.RS384),
	RS512: new(rsa.RS512),
	ES256: new(ecdsa.ES256),
	ES384: new(ecdsa.ES384),
	ES512: new(ecdsa.ES512),
}

// SignatureAlgorithm is a collection of all signature algorithm and it provides
// some basic method to use
type SignatureAlgorithm interface {
	// GenerateKeyPair generates public and private key
	GenerateKeyPair() (*keypair.KeyPair, error)
}

func GetSignatureAlgorithm(algorithm Algorithm) (SignatureAlgorithm, error) {
	sa := algorithmMap[algorithm]
	if sa == nil {
		return nil, errors.Errorf("the signature algorithm '%s' is invalid. Valid options are: "+
			"'RS256', 'RS384', 'RS512', 'ES256', 'ES384', 'ES512'\n", algorithm)
	}
	return sa, nil
}
