package pulsar

import (
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewTxn(t *testing.T) {
	topic := "my-topic"
	sub := "my-sub"

	tc := createTcClient(t)
	id, err := tc.newTransaction(time.Duration(300000000000))
	err = tc.addSubscriptionToTxn(id, topic, sub)
	err = tc.addPublishPartitionToTxn(id, []string{topic})
	assert.Nil(t, err)
	id2, err := tc.newTransaction(time.Duration(300000000000))
	err = tc.endTxn(id2, pb.TxnAction_ABORT)
	assert.Nil(t, err)
}

func Test(t *testing.T) {
	assert.NotNil(t, TxnID{})
	id := TxnID{}
	println("[{}, {}] ", id.mostSigBits, id.leastSigBits)
}

/*
*
Create a transaction coordinator client to send request
*/
func createTcClient(t *testing.T) *transactionCoordinatorClient {
	c, err := NewClient(ClientOptions{
		URL:                 "pulsar://localhost:6650",
		IsEnableTransaction: true,
	})
	assert.Nil(t, err)
	tcClient := newTransactionCoordinatorClientImpl(c.(*client))
	err = tcClient.start()
	assert.Nil(t, err)
	return tcClient
}
