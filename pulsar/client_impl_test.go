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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	client, err := NewClient(ClientOptions{})
	assert.Nil(t, client)
	assert.NotNil(t, err)
	assert.Equal(t, ResultInvalidConfiguration, err.(*Error).Result())
}

func TestTLSConnectionCAError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:              serviceURLTLS,
		OperationTimeout: 5 * time.Second,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	// The client should fail because it wouldn't trust the
	// broker certificate
	assert.Error(t, err)
	assert.Nil(t, producer)

	client.Close()
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

	client.Close()
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

	client.Close()
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

	client.Close()
}

func TestTLSConnectionHostNameVerificationError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                   "pulsar+ssl://127.0.0.1:6651",
		OperationTimeout:      5 * time.Second,
		TLSTrustCertsFilePath: caCertsPath,
		TLSValidateHostname:   true,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newTopicName(),
	})

	assert.Error(t, err)
	assert.Nil(t, producer)

	client.Close()
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

	client.Close()
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

	client.Close()
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

	client.Close()
}

func TestTokenAuthWithSupplier(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
		Authentication: NewAuthenticationTokenFromSupplier(func() (s string, err error) {
			token, err := ioutil.ReadFile(tokenFilePath)
			if err != nil {
				return "", err
			}

			return string(token), nil
		}),
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	client.Close()
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

	client.Close()
}

func TestTopicPartitions(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	assert.Nil(t, err)
	defer client.Close()

	// Create topic with 5 partitions
	err = httpPut("admin/v2/persistent/public/default/TestGetTopicPartitions/partitions", 5)
	assert.Nil(t, err)

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

func TestNamespaceTopicsNamespaceDoesNotExit(t *testing.T) {
	c, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	if err != nil {
		t.Errorf("failed to create client error: %+v", err)
		return
	}
	defer c.Close()
	ci := c.(*client)

	// fetch from namespace that does not exist
	name := generateRandomName()
	topics, err := ci.namespaceTopics(fmt.Sprintf("%s/%s", name, name))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(topics))
}

func TestNamespaceTopics(t *testing.T) {
	name := generateRandomName()
	namespace := fmt.Sprintf("public/%s", name)
	namespaceURL := fmt.Sprintf("admin/v2/namespaces/%s", namespace)
	err := httpPut(namespaceURL, anonymousNamespacePolicy())
	if err != nil {
		t.Fatal()
	}
	defer func() {
		_ = httpDelete(fmt.Sprintf("admin/v2/namespaces/%s", namespace))
	}()

	// create topics
	topic1 := fmt.Sprintf("%s/topic-1", namespace)
	if err := httpPut("admin/v2/persistent/"+topic1, nil); err != nil {
		t.Fatal(err)
	}
	topic2 := fmt.Sprintf("%s/topic-2", namespace)
	if err := httpPut("admin/v2/persistent/"+topic2, namespace); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = httpDelete("admin/v2/persistent/"+topic1, "admin/v2/persistent/"+topic2)
	}()

	c, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	if err != nil {
		t.Errorf("failed to create client error: %+v", err)
		return
	}
	defer c.Close()
	ci := c.(*client)

	topics, err := ci.namespaceTopics(namespace)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(topics))

	// add a non-persistent topic
	topicName := fmt.Sprintf("non-persistent://%s/testNonPersistentTopic", namespace)
	client, err := NewClient(ClientOptions{
		URL: serviceURL,
	})
	assert.Nil(t, err)
	defer client.Close()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topicName,
	})

	assert.Nil(t, err)
	defer producer.Close()

	topics, err = ci.namespaceTopics(namespace)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(topics))
}

func anonymousNamespacePolicy() map[string]interface{} {
	return map[string]interface{}{
		"auth_policies": map[string]interface{}{
			"namespace_auth": map[string]interface{}{
				"anonymous": []string{"produce", "consume"},
			},
		},
		"replication_clusters": []string{"standalone"},
	}
}
