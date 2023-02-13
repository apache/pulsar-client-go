package pulsar

import (
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTCClient(t *testing.T) {
	//1. Prepare: create PulsarClient and init transaction coordinator client.
	topic := "my-topic"
	sub := "my-sub"
	tc, client := createTcClient(t)
	defer client.Close()
	defer tc.close()
	//2. Prepare: create Topic and Subscription.
	_, err := client.Subscribe(ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
	})
	assert.Nil(t, err)
	//3. Test newTransaction, addSubscriptionToTxn, addPublishPartitionToTxn
	//Create a transaction1 and add subscription and publish topic to the transaction.
	id1, err := tc.newTransaction(3 * time.Minute)
	err = tc.addSubscriptionToTxn(id1, topic, sub)
	err = tc.addPublishPartitionToTxn(id1, []string{topic})
	assert.Nil(t, err)
	//4. Verify the transaction1 stats
	stats, err := transactionStats(id1)
	assert.Nil(t, err)
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
	assert.Nil(t, err)
	//6. Verify the transaction2 stats
	stats2, err := transactionStats(id2)
	assert.Equal(t, "OPEN", stats2["status"])
	err = tc.endTxn(id2, pb.TxnAction_COMMIT)
	stats2, err = transactionStats(id2)
	//The transaction will be removed from txnMeta. Therefore, it is expected that stats2 is zero
	if err == nil {
		assert.Equal(t, "COMMITTED", stats2["status"])
	} else {
		assert.Equal(t, err.Error(), "http error status code: 404")
	}
}

/*
Create a transaction coordinator client to send request
*/
func createTcClient(t *testing.T) (*transactionCoordinatorClient, *client) {
	c, err := NewClient(ClientOptions{
		URL:                 "pulsar://localhost:6650",
		IsEnableTransaction: true,
	})
	assert.Nil(t, err)
	tcClient := newTransactionCoordinatorClientImpl(c.(*client))
	err = tcClient.start()
	assert.Nil(t, err)

	return tcClient, c.(*client)
}
