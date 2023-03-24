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
	"errors"
	"fmt"
	"testing"
	"time"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

//Test points:
//	1. Abort and commit txn.
//		1. Do nothing, just open a transaction and then commit it or abort it.
//		The operations of committing/aborting txn should success at the first time and fail at the second time.
//	2. The internal API, registerSendOrAckOp and endSendOrAckOp
//		1. Register 4 operation but only end 3 operations, the transaction can not be committed or aborted.
//		2. Register 3 operation and end 3 operation the transaction can be committed and aborted.
//		3. Register an operation and end the operation with an error,
//		and then the state of the transaction should be replaced to errored.
//	3. The internal API, registerAckTopic and registerProducerTopic
//		1. Register ack topic and send topic, and call http request to get the stats of the transaction
//		to do verification.

// TestTxnImplCommitOrAbort Test abort and commit txn
func TestTxnImplCommitOrAbort(t *testing.T) {
	tc, _ := createTcClient(t)
	//1. Open a transaction and then commit it.
	//The operations of committing txn1 should success at the first time and fail at the second time.
	txn1 := createTxn(tc, t)
	err := txn1.Commit(context.Background())
	require.Nil(t, err, fmt.Sprintf("Failed to commit the transaction %d:%d\n", txn1.txnID.mostSigBits,
		txn1.txnID.leastSigBits))
	txn1.state = TxnOpen
	txn1.opsFlow <- true
	err = txn1.Commit(context.Background())
	assert.Equal(t, err.(*Error).Result(), TransactionNoFoundError)
	assert.Equal(t, txn1.GetState(), TxnError)
	//2. Open a transaction and then abort it.
	//The operations of aborting txn2 should success at the first time and fail at the second time.
	id2, err := tc.newTransaction(time.Hour)
	require.Nil(t, err, "Failed to new a transaction")
	txn2 := newTransaction(*id2, tc, time.Hour)
	err = txn2.Abort(context.Background())
	require.Nil(t, err, fmt.Sprintf("Failed to abort the transaction %d:%d\n",
		id2.mostSigBits, id2.leastSigBits))
	txn2.state = TxnOpen
	txn2.opsFlow <- true
	err = txn2.Abort(context.Background())
	assert.Equal(t, err.(*Error).Result(), TransactionNoFoundError)
	assert.Equal(t, txn1.GetState(), TxnError)
	err = txn2.registerSendOrAckOp()
	assert.Equal(t, err.(*Error).Result(), InvalidStatus)
	err = txn1.registerSendOrAckOp()
	assert.Equal(t, err.(*Error).Result(), InvalidStatus)
}

// TestRegisterOpAndEndOp Test the internal API including the registerSendOrAckOp and endSendOrAckOp.
func TestRegisterOpAndEndOp(t *testing.T) {
	tc, _ := createTcClient(t)
	//1. Register 4 operation but only end 3 operations, the transaction can not be committed or aborted.
	res := registerOpAndEndOp(t, tc, 4, 3, nil, true)
	assert.Equal(t, res.(*Error).Result(), TimeoutError)
	res = registerOpAndEndOp(t, tc, 4, 3, nil, false)
	assert.Equal(t, res.(*Error).Result(), TimeoutError)

	//2. Register 3 operation and end 3 operation the transaction can be committed and aborted.
	res = registerOpAndEndOp(t, tc, 3, 3, nil, true)
	assert.Nil(t, res)
	res = registerOpAndEndOp(t, tc, 3, 3, nil, false)
	assert.Nil(t, res)
	//3. Register an operation and end the operation with an error,
	// and then the state of the transaction should be replaced to errored.
	res = registerOpAndEndOp(t, tc, 4, 4, errors.New(""), true)
	assert.Equal(t, res.(*Error).Result(), InvalidStatus)
	res = registerOpAndEndOp(t, tc, 4, 4, errors.New(""), false)
	assert.Equal(t, res.(*Error).Result(), InvalidStatus)
}

// TestRegisterTopic Test the internal API, registerAckTopic and registerProducerTopic
func TestRegisterTopic(t *testing.T) {
	//1. Prepare: create PulsarClient and init transaction coordinator client.
	topic := newTopicName()
	sub := "my-sub"
	tc, client := createTcClient(t)
	//2. Prepare: create Topic and Subscription.
	_, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
	})
	assert.NoError(t, err)
	txn := createTxn(tc, t)
	//3. Create a topic and register topic and subscription.
	err = txn.registerAckTopic(topic, sub)
	require.Nil(t, err, "Failed to register ack topic.")
	err = txn.registerProducerTopic(topic)
	require.Nil(t, err, "Failed to register ack topic.")
	//4. Call http request to get the stats of the transaction to do verification.
	stats2, err := transactionStats(&txn.txnID)
	assert.NoError(t, err)
	topics := stats2["producedPartitions"].(map[string]interface{})
	subTopics := stats2["ackedPartitions"].(map[string]interface{})
	assert.NotNil(t, topics[topic])
	assert.NotNil(t, subTopics[topic])
	subs := subTopics[topic].(map[string]interface{})
	assert.NotNil(t, subs[sub])
}

func registerOpAndEndOp(t *testing.T, tc *transactionCoordinatorClient, rp int, ep int, err error, commit bool) error {
	txn := createTxn(tc, t)
	for i := 0; i < rp; i++ {
		err := txn.registerSendOrAckOp()
		assert.Nil(t, err)
	}
	for i := 0; i < ep; i++ {
		txn.endSendOrAckOp(err)
	}
	if commit {
		err = txn.Commit(context.Background())
	} else {
		err = txn.Abort(context.Background())
	}
	return err
}

func createTxn(tc *transactionCoordinatorClient, t *testing.T) *transaction {
	id, err := tc.newTransaction(time.Hour)
	require.Nil(t, err, "Failed to new a transaction.")
	return newTransaction(*id, tc, time.Hour)
}

// createTcClient Create a transaction coordinator client to send request
func createTcClient(t *testing.T) (*transactionCoordinatorClient, *client) {
	c, err := NewClient(ClientOptions{
		URL:                   webServiceURLTLS,
		TLSTrustCertsFilePath: caCertsPath,
		Authentication:        NewAuthenticationTLS(tlsClientCertPath, tlsClientKeyPath),
	})
	require.Nil(t, err, "Failed to create client.")
	tcClient := newTransactionCoordinatorClientImpl(c.(*client))
	err = tcClient.start()
	require.Nil(t, err, "Failed to start transaction coordinator.")

	return tcClient, c.(*client)
}
