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
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/pulsar-client-go/pkg/auth"
)

func TestClient(t *testing.T) {
	client, err := NewClient("")
	assert.Nil(t, client)
	assert.NotNil(t, err)
	assert.Equal(t, ResultInvalidConfiguration, err.(*Error).Result())
}

func TestTLSConnectionCAError(t *testing.T) {
	client, err := NewClient(serviceURLTLS, WithTLS("", false, false))
	assert.NoError(t, err)

	producer, err := client.CreateProducer(newTopicName())

	// The client should fail because it wouldn't trust the
	// broker certificate
	assert.Error(t, err)
	assert.Nil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTLSInsecureConnection(t *testing.T) {
	client, err := NewClient(serviceURLTLS, WithTLS("", true, false))
	assert.NoError(t, err)

	producer, err := client.CreateProducer(newTopicName())

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTLSConnection(t *testing.T) {
	client, err := NewClient(serviceURLTLS, WithTLS(caCertsPath, false, false))
	assert.NoError(t, err)

	producer, err := client.CreateProducer(newTopicName())

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTLSConnectionHostNameVerification(t *testing.T) {
	client, err := NewClient(serviceURLTLS, WithTLS(caCertsPath, false, true))
	assert.NoError(t, err)

	producer, err := client.CreateProducer(newTopicName())
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTLSConnectionHostNameVerificationError(t *testing.T) {
	client, err := NewClient("pulsar+ssl://127.0.0.1:6651", WithTLS(caCertsPath, false, true))
	assert.NoError(t, err)

	producer, err := client.CreateProducer(newTopicName())

	assert.Error(t, err)
	assert.Nil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTLSAuthError(t *testing.T) {
	client, err := NewClient(serviceURLTLS, WithTLS(caCertsPath, false, false))
	assert.NoError(t, err)

	producer, err := client.CreateProducer(newAuthTopicName())

	assert.Error(t, err)
	assert.Nil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTLSAuth(t *testing.T) {
	client, err := NewClient(serviceURLTLS, WithTLS(caCertsPath, false, false),
		WithAuthentication(NewAuthenticationTLS(tlsClientCertPath, tlsClientKeyPath).(auth.Provider)))
	assert.NoError(t, err)

	producer, err := client.CreateProducer(newAuthTopicName())

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTokenAuth(t *testing.T) {
	token, err := ioutil.ReadFile(tokenFilePath)
	assert.NoError(t, err)

	client, err := NewClient(serviceURL, WithAuthentication(NewAuthenticationToken(string(token)).(auth.Provider)))
	assert.NoError(t, err)

	producer, err := client.CreateProducer(newAuthTopicName())

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTokenAuthFromFile(t *testing.T) {
	client, err := NewClient(serviceURL, WithAuthentication(NewAuthenticationTokenFromFile(tokenFilePath).(auth.Provider)))
	assert.NoError(t, err)

	producer, err := client.CreateProducer(newAuthTopicName())

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	err = client.Close()
	assert.NoError(t, err)
}

func TestTopicPartitions(t *testing.T) {
	client, err := NewClient("pulsar://localhost:6650")

	assert.Nil(t, err)
	defer client.Close()

	// Create topic with 5 partitions
	httpPut("http://localhost:8080/admin/v2/persistent/public/default/TestGetTopicPartitions/partitions",
		5)

	partitionedTopic := "persistent://public/default/TestGetTopicPartitions"

	partitions, err := client.TopicPartitions(partitionedTopic)
	assert.Nil(t, err)
	assert.Equal(t, len(partitions), 5)
	for i := 0; i < 5; i++ {
		assert.Equal(t, partitions[i],
			fmt.Sprintf("%s-partition-%d", partitionedTopic, i))
	}

	// Non-Partitioned topic
	topic := "persistent://public/default/TestGetTopicPartitions-nopartitions"

	partitions, err = client.TopicPartitions(topic)
	assert.Nil(t, err)
	assert.Equal(t, len(partitions), 1)
	assert.Equal(t, partitions[0], topic)
}
