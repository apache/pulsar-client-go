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
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/pulsar-client-go/pulsar/internal"

	"github.com/apache/pulsar-client-go/pulsar/internal/auth"
	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	client, err := NewClient(ClientOptions{})
	assert.Nil(t, client)
	assert.NotNil(t, err)
	assert.Equal(t, InvalidConfiguration, err.(*Error).Result())
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

func TestTLSAuthWithCertSupplier(t *testing.T) {
	supplier := func() (*tls.Certificate, error) {
		cert, err := tls.LoadX509KeyPair(tlsClientCertPath, tlsClientKeyPath)
		return &cert, err
	}
	client, err := NewClient(ClientOptions{
		URL:                   serviceURLTLS,
		TLSTrustCertsFilePath: caCertsPath,
		Authentication:        NewAuthenticationFromTLSCertSupplier(supplier),
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

// mockOAuthServer will mock a oauth service for the tests
func mockOAuthServer() *httptest.Server {
	// prepare a port for the mocked server
	server := httptest.NewUnstartedServer(http.DefaultServeMux)

	// mock the used REST path for the tests
	mockedHandler := http.NewServeMux()
	mockedHandler.HandleFunc("/.well-known/openid-configuration", func(writer http.ResponseWriter, request *http.Request) {
		s := fmt.Sprintf(`{
    "issuer":"%s",
    "authorization_endpoint":"%s/authorize",
    "token_endpoint":"%s/oauth/token",
    "device_authorization_endpoint":"%s/oauth/device/code"
}`, server.URL, server.URL, server.URL, server.URL)
		fmt.Fprintln(writer, s)
	})
	mockedHandler.HandleFunc("/oauth/token", func(writer http.ResponseWriter, request *http.Request) {
		fmt.Fprintln(writer, "{\n"+
			"  \"access_token\": \"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0b2tlbi1wcmluY2lwYWwifQ."+
			"tSfgR8l7dKC6LoWCxQgNkuSB8our7xV_nAM7wpgCbG4\",\n"+
			"  \"token_type\": \"Bearer\"\n}")
	})
	mockedHandler.HandleFunc("/authorize", func(writer http.ResponseWriter, request *http.Request) {
		fmt.Fprintln(writer, "true")
	})

	server.Config.Handler = mockedHandler
	server.Start()

	return server
}

// mockKeyFile will mock a temp key file for testing.
func mockKeyFile(server string) (string, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	kf, err := ioutil.TempFile(pwd, "test_oauth2")
	if err != nil {
		return "", err
	}
	_, err = kf.WriteString(fmt.Sprintf(`{
  "type":"resource",
  "client_id":"client-id",
  "client_secret":"client-secret",
  "client_email":"oauth@test.org",
  "issuer_url":"%s"
}`, server))
	if err != nil {
		return "", err
	}

	return kf.Name(), nil
}

func TestOAuth2Auth(t *testing.T) {
	server := mockOAuthServer()
	defer server.Close()
	kf, err := mockKeyFile(server.URL)
	defer os.Remove(kf)
	if err != nil {
		t.Fatal(err)
	}

	params := map[string]string{
		auth.ConfigParamType:      auth.ConfigParamTypeClientCredentials,
		auth.ConfigParamIssuerURL: server.URL,
		auth.ConfigParamClientID:  "client-id",
		auth.ConfigParamAudience:  "audience",
		auth.ConfigParamKeyFile:   kf,
	}

	oauth := NewAuthenticationOAuth2(params)
	client, err := NewClient(ClientOptions{
		URL:            serviceURL,
		Authentication: oauth,
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
	topicAdminURL := "admin/v2/persistent/public/default/TestGetTopicPartitions/partitions"
	err = httpPut(topicAdminURL, 5)
	defer httpDelete(topicAdminURL)
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
	topics, err := ci.lookupService.GetTopicsOfNamespace(fmt.Sprintf("public/%s", name), internal.Persistent)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(topics))
}

func TestNamespaceTopicsNamespaceDoesNotExitWebURL(t *testing.T) {
	c, err := NewClient(ClientOptions{
		URL: webServiceURL,
	})
	if err != nil {
		t.Errorf("failed to create client error: %+v", err)
		return
	}
	defer c.Close()
	ci := c.(*client)

	// fetch from namespace that does not exist
	name := generateRandomName()
	topics, err := ci.lookupService.GetTopicsOfNamespace(fmt.Sprintf("public/%s", name), internal.Persistent)
	assert.NotNil(t, err)
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

	topics, err := ci.lookupService.GetTopicsOfNamespace(namespace, internal.Persistent)
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

	topics, err = ci.lookupService.GetTopicsOfNamespace(namespace, internal.Persistent)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(topics))
}

func TestNamespaceTopicsWebURL(t *testing.T) {
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
		URL: webServiceURL,
	})
	if err != nil {
		t.Errorf("failed to create client error: %+v", err)
		return
	}
	defer c.Close()
	ci := c.(*client)

	topics, err := ci.lookupService.GetTopicsOfNamespace(namespace, internal.Persistent)
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

	topics, err = ci.lookupService.GetTopicsOfNamespace(namespace, internal.Persistent)
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

func TestRetryWithMultipleHosts(t *testing.T) {
	// Multi hosts included an unreached port and the actual port for verify retry logic
	client, err := NewClient(ClientOptions{
		URL: "pulsar://localhost:6600,localhost:6650",
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/retry-multiple-hosts-" + generateRandomName()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})

	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()
	var msgIDs [][]byte

	for i := 0; i < 10; i++ {
		if msgID, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, msgID)
			msgIDs = append(msgIDs, msgID.Serialize())
		}
	}

	assert.Equal(t, 10, len(msgIDs))

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            "retry-multi-hosts-sub",
		Type:                        Shared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		assert.Nil(t, err)
		assert.Contains(t, msgIDs, msg.ID().Serialize())
		consumer.Ack(msg)
	}

	err = consumer.Unsubscribe()
	assert.Nil(t, err)

}

func TestHTTPSConnectionCAError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:              webServiceURLTLS,
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

func TestHTTPSInsecureConnection(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                        webServiceURLTLS,
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

func TestHTTPSConnection(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                   webServiceURLTLS,
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

func TestHTTPSConnectionHostNameVerification(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                   webServiceURLTLS,
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

func TestHTTPSConnectionHostNameVerificationError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                   "https://127.0.0.1:8443",
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

func TestHTTPTopicPartitions(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: webServiceURL,
	})

	assert.Nil(t, err)
	defer client.Close()

	// Create topic with 5 partitions
	topicAdminURL := "admin/v2/persistent/public/default/TestHTTPTopicPartitions/partitions"
	err = httpPut(topicAdminURL, 5)
	defer httpDelete(topicAdminURL)
	assert.Nil(t, err)

	partitionedTopic := "persistent://public/default/TestHTTPTopicPartitions"

	partitions, err := client.TopicPartitions(partitionedTopic)
	assert.Nil(t, err)
	assert.Equal(t, len(partitions), 5)
	for i := 0; i < 5; i++ {
		assert.Equal(t, partitions[i],
			fmt.Sprintf("%s-partition-%d", partitionedTopic, i))
	}

	// Non-Partitioned topic
	topic := "persistent://public/default/TestHTTPTopicPartitions-nopartitions"

	partitions, err = client.TopicPartitions(topic)
	assert.Nil(t, err)
	assert.Equal(t, len(partitions), 1)
	assert.Equal(t, partitions[0], topic)
}

func TestRetryWithMultipleHttpHosts(t *testing.T) {
	// Multi hosts included an unreached port and the actual port for verify retry logic
	client, err := NewClient(ClientOptions{
		URL: "http://localhost:8081,localhost:8080",
	})

	assert.Nil(t, err)
	defer client.Close()

	topic := "persistent://public/default/retry-multiple-hosts-" + generateRandomName()

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: topic,
	})

	assert.Nil(t, err)
	defer producer.Close()

	ctx := context.Background()
	var msgIDs [][]byte

	for i := 0; i < 10; i++ {
		if msgID, err := producer.Send(ctx, &ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, msgID)
			msgIDs = append(msgIDs, msgID.Serialize())
		}
	}

	assert.Equal(t, 10, len(msgIDs))

	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            "retry-multi-hosts-sub",
		Type:                        Shared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	defer consumer.Close()

	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		assert.Nil(t, err)
		assert.Contains(t, msgIDs, msg.ID().Serialize())
		consumer.Ack(msg)
	}

	err = consumer.Unsubscribe()
	assert.Nil(t, err)

}

func TestHTTPSAuthError(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                   webServiceURLTLS,
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

func TestHTTPSAuth(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                   webServiceURLTLS,
		TLSTrustCertsFilePath: caCertsPath,
		Authentication:        NewAuthenticationTLS(tlsClientCertPath, tlsClientKeyPath),
	})
	assert.NoError(t, err)
	t.Logf("TestHTTPSAuth client %v", client)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})
	t.Logf("TestHTTPSAuth err %v", err)
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	client.Close()
}

func TestHTTPSAuthWithCertSupplier(t *testing.T) {
	supplier := func() (*tls.Certificate, error) {
		cert, err := tls.LoadX509KeyPair(tlsClientCertPath, tlsClientKeyPath)
		return &cert, err
	}
	client, err := NewClient(ClientOptions{
		URL:                   webServiceURLTLS,
		TLSTrustCertsFilePath: caCertsPath,
		Authentication:        NewAuthenticationFromTLSCertSupplier(supplier),
	})
	assert.NoError(t, err)
	t.Logf("TestHTTPSAuthWithCertSupplier client %v", client)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})
	t.Logf("TestHTTPSAuthWithCertSupplier err %v", err)
	assert.NoError(t, err)
	assert.NotNil(t, producer)

	client.Close()
}

func TestHTTPTokenAuth(t *testing.T) {
	token, err := ioutil.ReadFile(tokenFilePath)
	assert.NoError(t, err)

	client, err := NewClient(ClientOptions{
		URL:            webServiceURL,
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

func TestHTTPTokenAuthWithSupplier(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL: webServiceURL,
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

func TestHTTPTokenAuthFromFile(t *testing.T) {
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

func TestHTTPSTokenAuthFromFile(t *testing.T) {
	client, err := NewClient(ClientOptions{
		URL:                   webServiceURLTLS,
		TLSTrustCertsFilePath: caCertsPath,
		TLSValidateHostname:   true,
		Authentication:        NewAuthenticationTokenFromFile(tokenFilePath),
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	client.Close()
}

func TestHTTPOAuth2Auth(t *testing.T) {
	server := mockOAuthServer()
	defer server.Close()
	kf, err := mockKeyFile(server.URL)
	defer os.Remove(kf)
	if err != nil {
		t.Fatal(err)
	}

	params := map[string]string{
		auth.ConfigParamType:      auth.ConfigParamTypeClientCredentials,
		auth.ConfigParamIssuerURL: server.URL,
		auth.ConfigParamClientID:  "client-id",
		auth.ConfigParamAudience:  "audience",
		auth.ConfigParamKeyFile:   kf,
	}

	oauth := NewAuthenticationOAuth2(params)
	client, err := NewClient(ClientOptions{
		URL:            webServiceURL,
		Authentication: oauth,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	client.Close()
}

func TestHTTPSOAuth2Auth(t *testing.T) {
	server := mockOAuthServer()
	defer server.Close()
	kf, err := mockKeyFile(server.URL)
	defer os.Remove(kf)
	if err != nil {
		t.Fatal(err)
	}

	params := map[string]string{
		auth.ConfigParamType:      auth.ConfigParamTypeClientCredentials,
		auth.ConfigParamIssuerURL: server.URL,
		auth.ConfigParamClientID:  "client-id",
		auth.ConfigParamAudience:  "audience",
		auth.ConfigParamKeyFile:   kf,
	}

	oauth := NewAuthenticationOAuth2(params)
	client, err := NewClient(ClientOptions{
		URL:                   webServiceURLTLS,
		TLSTrustCertsFilePath: caCertsPath,
		TLSValidateHostname:   true,
		Authentication:        oauth,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	client.Close()
}

func TestHTTPOAuth2AuthFailed(t *testing.T) {
	server := mockOAuthServer()
	defer server.Close()
	kf, err := mockKeyFile(server.URL)
	defer os.Remove(kf)
	if err != nil {
		t.Fatal(err)
	}

	params := map[string]string{
		auth.ConfigParamType:      auth.ConfigParamTypeClientCredentials,
		auth.ConfigParamIssuerURL: "error-url",
		auth.ConfigParamClientID:  "client-id",
		auth.ConfigParamAudience:  "audience",
	}

	oauth := NewAuthenticationOAuth2(params)
	client, err := NewClient(ClientOptions{
		URL:            webServiceURL,
		Authentication: oauth,
	})
	assert.NoError(t, err)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})

	assert.Error(t, err)
	assert.Nil(t, producer)

	client.Close()
}

func TestHTTPBasicAuth(t *testing.T) {
	basicAuth, err := NewAuthenticationBasic("admin", "123456")
	require.NoError(t, err)
	require.NotNil(t, basicAuth)

	client, err := NewClient(ClientOptions{
		URL:            webServiceURL,
		Authentication: basicAuth,
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})

	require.NoError(t, err)
	require.NotNil(t, producer)

	client.Close()
}

func TestHTTPSBasicAuth(t *testing.T) {
	basicAuth, err := NewAuthenticationBasic("admin", "123456")
	require.NoError(t, err)
	require.NotNil(t, basicAuth)

	client, err := NewClient(ClientOptions{
		URL:                   webServiceURLTLS,
		TLSTrustCertsFilePath: caCertsPath,
		TLSValidateHostname:   true,
		Authentication:        basicAuth,
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	producer, err := client.CreateProducer(ProducerOptions{
		Topic: newAuthTopicName(),
	})
	require.NoError(t, err)
	require.NotNil(t, producer)

	client.Close()
}
