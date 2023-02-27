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
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/stretchr/testify/assert"

	"testing"
	"time"
)

func TestTCClient(t *testing.T) {
	//1. Prepare: create PulsarClient and init transaction coordinator client.
	topic := newTopicName()
	sub := "my-sub"
	tc, client := createTcClient(t)
	//2. Prepare: create Topic and Subscription.
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
	})
	assert.NoError(t, err)
	//3. Test newTransaction, addSubscriptionToTxn, addPublishPartitionToTxn
	//Create a transaction1 and add subscription and publish topic to the transaction.
	id1, err := tc.newTransaction(3 * time.Minute)
	assert.NoError(t, err)
	err = tc.addSubscriptionToTxn(id1, topic, sub)
	assert.NoError(t, err)
	err = tc.addPublishPartitionToTxn(id1, []string{topic})
	assert.NoError(t, err)
	//4. Verify the transaction1 stats
	stats, err := transactionStats(id1)
	assert.NoError(t, err)
	assert.Equal(t, "OPEN", stats["status"])
	producedPartitions := stats["producedPartitions"].(map[string]interface{})
	ackedPartitions := stats["ackedPartitions"].(map[string]interface{})
	_, ok := producedPartitions[topic]
	assert.True(t, ok)
	temp, ok := ackedPartitions[topic]
	assert.True(t, ok)
	subscriptions := temp.(map[string]interface{})
	_, ok = subscriptions[sub]
	assert.True(t, ok)
	//5. Test End transaction
	//Create transaction2 and Commit the transaction.
	id2, err := tc.newTransaction(3 * time.Minute)
	assert.NoError(t, err)
	//6. Verify the transaction2 stats
	stats2, err := transactionStats(id2)
	assert.NoError(t, err)
	assert.Equal(t, "OPEN", stats2["status"])
	err = tc.endTxn(id2, pb.TxnAction_COMMIT)
	assert.NoError(t, err)
	stats2, err = transactionStats(id2)
	//The transaction will be removed from txnMeta. Therefore, it is expected that stats2 is zero
	if err == nil {
		assert.Equal(t, "COMMITTED", stats2["status"])
	} else {
		assert.Equal(t, err.Error(), "http error status code: 404")
	}
	defer consumer.Close()
	defer tc.close()
	defer client.Close()
}

// createTcClient Create a transaction coordinator client to send request
func createTcClient(t *testing.T) (*transactionCoordinatorClient, *client) {
	c, err := NewClient(ClientOptions{
		URL:                   webServiceURLTLS,
		TLSTrustCertsFilePath: caCertsPath,
		Authentication:        NewAuthenticationTLS(tlsClientCertPath, tlsClientKeyPath),
	})
	if err != nil {
		t.Fatalf("Failed to create client due to %s", err.Error())
	}
	tcClient := newTransactionCoordinatorClientImpl(c.(*client))
	if err = tcClient.start(); err != nil {
		t.Fatalf("Failed to start transaction coordinator due to %s", err.Error())
	}

	return tcClient, c.(*client)
}
