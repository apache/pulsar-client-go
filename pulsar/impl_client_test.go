//
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
//

package pulsar

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	client, err := NewClient(ClientOptions{})
	assert.Nil(t, client)
	assert.NotNil(t, err)
	assert.Equal(t, Result(ResultInvalidConfiguration), err.(*Error).Result())
}

func TestTLSConnectionCAError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURLTLS,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	// The client should fail because it wouldn't trust the
	// broker certificate
	assert.Error(t, err)
	assert.Nil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTLSInsecureConnection(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                        serviceURLTLS,
		TLSAllowInsecureConnection: true,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTLSConnection(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                   serviceURLTLS,
		TLSTrustCertsFilePath: caCertsPath,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTLSConnectionHostNameVerification(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                   serviceURLTLS,
		TLSTrustCertsFilePath: caCertsPath,
		TLSValidateHostname:   true,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTLSConnectionHostNameVerificationError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                   "pulsar+ssl://127.0.0.1:6651",
		TLSTrustCertsFilePath: caCertsPath,
		TLSValidateHostname:   true,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	assert.Error(t, err)
	assert.Nil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTLSAuthError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                   serviceURLTLS,
		TLSTrustCertsFilePath: caCertsPath,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})

	assert.Error(t, err)
	assert.Nil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTLSAuth(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                   serviceURLTLS,
		TLSTrustCertsFilePath: caCertsPath,
		Authentication:        NewAuthenticationTLS(tlsClientCertPath, tlsClientKeyPath),
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTokenAuth(t *testing.T) {
	token, err := ioutil.ReadFile(tokenFilePath)
	assert.NoError(t, err)

	client, err := NewClient(ClientOptions{
		URL:            serviceURL,
		Authentication: NewAuthenticationToken(string(token)),
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTokenAuthFromFile(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:            serviceURL,
		Authentication: NewAuthenticationTokenFromFile(tokenFilePath),
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}
