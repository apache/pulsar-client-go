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
	require.Nil(t, err, fmt.Sprintf("Failed to commit the transaction %d:%d\n", txn1.txnID.MostSigBits,
		txn1.txnID.LeastSigBits))
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
		id2.MostSigBits, id2.LeastSigBits))
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
		EnableTransaction:     true,
	})
	require.Nil(t, err, "Failed to create client.")

	return c.(*client).tcClient, c.(*client)
}

// TestConsumeAndProduceWithTxn is a test function that validates the behavior of producing and consuming
// messages with and without transactions. It consists of the following steps:
//
// 1. Prepare: Create a PulsarClient and initialize the transaction coordinator client.
// 2. Prepare: Create a topic and a subscription.
// 3. Produce 10 messages with a transaction and 10 messages without a transaction.
// - Expectation: The consumer should be able to receive the 10 messages sent without a transaction,
// but not the 10 messages sent with the transaction.
// 4. Commit the transaction and receive the remaining 10 messages.
// - Expectation: The consumer should be able to receive the 10 messages sent with the transaction.
// 5. Clean up: Close the consumer and producer instances.
//
// The test ensures that the consumer can only receive messages sent with a transaction after it is committed,
// and that it can always receive messages sent without a transaction.
func TestConsumeAndProduceWithTxn(t *testing.T) {
	// Step 1: Prepare - Create PulsarClient and initialize the transaction coordinator client.
	topic := newTopicName()
	sub := "my-sub"
	_, client := createTcClient(t)
	// Step 2: Prepare - Create Topic and Subscription.
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
	})
	assert.NoError(t, err)
	producer, err := client.CreateProducer(ProducerOptions{
		Topic:       topic,
		SendTimeout: 0,
	})
	assert.NoError(t, err)
	// Step 3: Open a transaction, send 10 messages with the transaction and 10 messages without the transaction.
	// Expectation: We can receive the 10 messages sent without a transaction and
	// cannot receive the 10 messages sent with the transaction.
	txn, err := client.NewTransaction(time.Hour)
	require.Nil(t, err)
	for i := 0; i < 10; i++ {
		_, err = producer.Send(context.Background(), &ProducerMessage{
			Payload: make([]byte, 1024),
		})
		require.Nil(t, err)
	}
	for i := 0; i < 10; i++ {
		_, err := producer.Send(context.Background(), &ProducerMessage{
			Transaction: txn,
			Payload:     make([]byte, 1024),
		})
		require.Nil(t, err)
	}
	// Attempt to receive and acknowledge the 10 messages sent without a transaction.
	for i := 0; i < 10; i++ {
		msg, _ := consumer.Receive(context.Background())
		assert.NotNil(t, msg)
		err = consumer.Ack(msg)
		assert.Nil(t, err)
	}
	// Create a goroutine to attempt receiving a message and send it to the 'done1' channel.
	done1 := make(chan Message)
	go func() {
		msg, _ := consumer.Receive(context.Background())
		err := consumer.AckID(msg.ID())
		require.Nil(t, err)
		close(done1)
	}()
	// Expectation: The consumer should not receive uncommitted messages.
	select {
	case <-done1:
		require.Fail(t, "The consumer should not receive uncommitted message")
	case <-time.After(time.Second):
	}
	// Step 4: After committing the transaction, we should be able to receive the remaining 10 messages.
	// Acknowledge the rest of the 10 messages with the transaction.
	// Expectation: After committing the transaction, all messages of the subscription will be acknowledged.
	_ = txn.Commit(context.Background())
	txn, err = client.NewTransaction(time.Hour)
	require.Nil(t, err)
	for i := 0; i < 9; i++ {
		msg, _ := consumer.Receive(context.Background())
		require.NotNil(t, msg)
		err = consumer.AckWithTxn(msg, txn)
		require.Nil(t, err)
	}
	_ = txn.Commit(context.Background())
	<-done1
	consumer.Close()
	consumer, _ = client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
	})
	// Create a goroutine to attempt receiving a message and send it to the 'done1' channel.
	done2 := make(chan Message)
	go func() {
		consumer.Receive(context.Background())
		close(done2)
	}()

	// Expectation: The consumer should not receive uncommitted messages.
	select {
	case <-done2:
		require.Fail(t, "The consumer should not receive messages")
	case <-time.After(time.Second):
	}

	// Step 5: Clean up - Close the consumer and producer instances.
	consumer.Close()
	producer.Close()
}

func TestAckAndSendWithTxn(t *testing.T) {
	// Prepare: Create PulsarClient and initialize the transaction coordinator client.
	sourceTopic := newTopicName()
	sinkTopic := newTopicName()
	sub := "my-sub"
	_, client := createTcClient(t)

	// Prepare: Create source and sink topics and subscriptions.
	sourceConsumer, _ := client.Subscribe(ConsumerOptions{
		Topic:            sourceTopic,
		SubscriptionName: sub,
	})
	sourceProducer, _ := client.CreateProducer(ProducerOptions{
		Topic:       sourceTopic,
		SendTimeout: 0,
	})
	sinkConsumer, _ := client.Subscribe(ConsumerOptions{
		Topic:            sinkTopic,
		SubscriptionName: sub,
	})
	sinkProducer, _ := client.CreateProducer(ProducerOptions{
		Topic:       sinkTopic,
		SendTimeout: 0,
	})

	// Produce 10 messages to the source topic.
	for i := 0; i < 10; i++ {
		_, err := sourceProducer.Send(context.Background(), &ProducerMessage{
			Payload: make([]byte, 1024),
		})
		require.Nil(t, err)
	}

	// Open a transaction and consume messages from the source topic while sending messages to the sink topic.
	txn, err := client.NewTransaction(time.Hour)
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		msg, _ := sourceConsumer.Receive(context.Background())
		require.NotNil(t, msg)

		payload := msg.Payload()
		_, err := sinkProducer.Send(context.Background(), &ProducerMessage{
			Transaction: txn,
			Payload:     payload,
		})
		require.Nil(t, err)

		err = sourceConsumer.AckWithTxn(msg, txn)
		require.Nil(t, err)
	}

	// Commit the transaction.
	_ = txn.Commit(context.Background())

	// Verify that the messages are available in the sink topic.
	for i := 0; i < 10; i++ {
		msg, _ := sinkConsumer.Receive(context.Background())
		require.NotNil(t, msg)
		err = sinkConsumer.Ack(msg)
		require.Nil(t, err)
	}

	// Clean up: Close consumers and producers.
	sourceConsumer.Close()
	sourceProducer.Close()
	sinkConsumer.Close()
	sinkProducer.Close()
}

func TestTransactionAbort(t *testing.T) {
	// Prepare: Create PulsarClient and initialize the transaction coordinator client.
	topic := newTopicName()
	sub := "my-sub"
	_, client := createTcClient(t)

	// Prepare: Create Topic and Subscription.
	consumer, _ := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
	})
	producer, _ := client.CreateProducer(ProducerOptions{
		Topic:       topic,
		SendTimeout: 0,
	})

	// Open a transaction and send 10 messages with the transaction.
	txn, err := client.NewTransaction(time.Hour)
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		_, err = producer.Send(context.Background(), &ProducerMessage{
			Transaction: txn,
			Payload:     make([]byte, 1024),
		})
		require.Nil(t, err)
	}

	// Abort the transaction.
	_ = txn.Abort(context.Background())

	consumerShouldNotReceiveMessage(t, consumer)

	// Clean up: Close the consumer and producer instances.
	consumer.Close()
	producer.Close()
}

func consumerShouldNotReceiveMessage(t *testing.T, consumer Consumer) {
	// Expectation: The consumer should not receive any messages.
	done := make(chan struct{})
	go func() {
		_, err := consumer.Receive(context.Background())
		if err != nil {
			close(done)
		}
	}()

	select {
	case <-done:
		// The consumer should not receive any messages.
		require.Fail(t, "The consumer should not receive any messages")
	case <-time.After(time.Second):
	}
}

func TestTransactionAckChunkMessage(t *testing.T) {
	topic := newTopicName()
	sub := "my-sub"

	// Prepare: Create PulsarClient and initialize the transaction coordinator client.
	_, client := createTcClient(t)

	// Create transaction and register the send operation.
	txn, err := client.NewTransaction(time.Hour)
	require.Nil(t, err)

	// Create a producer with chunking enabled to send a large message that will be split into chunks.
	producer, err := client.CreateProducer(ProducerOptions{
		Name:            "test",
		Topic:           topic,
		EnableChunking:  true,
		DisableBatching: true,
	})
	require.NoError(t, err)
	require.NotNil(t, producer)
	defer producer.Close()

	// Subscribe to the consumer.
	consumer, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		Type:             Exclusive,
		SubscriptionName: sub,
	})
	require.NoError(t, err)
	defer consumer.Close()

	// Send a large message that will be split into chunks.
	msgID, err := producer.Send(context.Background(), &ProducerMessage{
		Transaction: txn,
		Payload:     createTestMessagePayload(_brokerMaxMessageSize),
	})
	require.NoError(t, err)
	_, ok := msgID.(*chunkMessageID)
	require.True(t, ok)

	err = txn.Commit(context.Background())
	require.Nil(t, err)

	// Receive the message using a new transaction and ack it.
	txn2, err := client.NewTransaction(time.Hour)
	require.Nil(t, err)
	message, err := consumer.Receive(context.Background())
	require.Nil(t, err)

	err = consumer.AckWithTxn(message, txn2)
	require.Nil(t, err)

	txn2.Abort(context.Background())

	// Close the consumer to simulate reconnection and receive the same message again.
	consumer.Close()

	// Subscribe to the consumer again.
	consumer, err = client.Subscribe(ConsumerOptions{
		Topic:            topic,
		Type:             Exclusive,
		SubscriptionName: sub,
	})
	require.Nil(t, err)
	message, err = consumer.Receive(context.Background())
	require.Nil(t, err)
	require.NotNil(t, message)

	// Create a new transaction and ack the message again.
	txn3, err := client.NewTransaction(time.Hour)
	require.Nil(t, err)

	err = consumer.AckWithTxn(message, txn3)
	require.Nil(t, err)

	// Commit the third transaction.
	err = txn3.Commit(context.Background())
	require.Nil(t, err)

	// Close the consumer again.
	consumer.Close()

	// Subscribe to the consumer again and verify that no message is received.
	consumer, err = client.Subscribe(ConsumerOptions{
		Topic:            topic,
		Type:             Exclusive,
		SubscriptionName: sub,
	})
	require.Nil(t, err)
	consumerShouldNotReceiveMessage(t, consumer)
}

func TestTxnConnReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := newTopicName()
	_, cli := createTcClient(t)

	txn, err := cli.NewTransaction(5 * time.Minute)
	assert.NoError(t, err)

	connections := cli.cnxPool.GetConnections()
	for _, conn := range connections {
		conn.Close()
	}

	err = txn.Commit(ctx)
	assert.NoError(t, err)

	txn, err = cli.NewTransaction(5 * time.Minute)
	assert.NoError(t, err) // Assert that the transaction can be opened after the connections are reconnected

	// Start a goroutine to periodically close connections
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				connections := cli.cnxPool.GetConnections()
				for _, conn := range connections {
					conn.Close()
				}
			}
		}
	}()

	producer, err := cli.CreateProducer(ProducerOptions{
		Topic:       topic,
		SendTimeout: 0,
	})
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err := producer.Send(context.Background(), &ProducerMessage{
			Transaction: txn,
			Payload:     make([]byte, 1024),
		})
		require.Nil(t, err)
		time.Sleep(500 * time.Millisecond)
	}
	err = txn.Commit(context.Background())
	assert.NoError(t, err)
}
