package pulsar

import (
	"context"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	uAtomic "go.uber.org/atomic"
	"sync"
	"sync/atomic"
	"time"
)

type State int32
type void struct{}
type subscription struct {
	topic        string
	subscription string
}

type transaction struct {
	sync.Mutex
	txnID                    TxnID
	state                    State
	tcClient                 *transactionCoordinatorClient
	registerPartitions       map[string]void
	registerAckSubscriptions map[subscription]void
	opsFlow                  chan struct{}
	opCount                  uAtomic.Int32
}

func newTransaction(id TxnID, tcClient *transactionCoordinatorClient, timeout time.Duration) *transaction {
	transaction := &transaction{
		txnID:                    id,
		state:                    Open,
		registerPartitions:       make(map[string]void),
		registerAckSubscriptions: make(map[subscription]void),
		opsFlow:                  make(chan struct{}, 20),
		tcClient:                 tcClient,
	}
	//This means there are not pending requests with this transaction. The transaction can be committed or aborted.
	transaction.opsFlow <- struct{}{}
	go func() {
		//Set the state of the transaction to timeout after timeout
		<-time.After(timeout)
		atomic.CompareAndSwapInt32((*int32)(&transaction.state), Open, TimeOut)
	}()
	return transaction
}
func (txn *transaction) GetState() State {
	return txn.state
}

func (txn *transaction) Commit(ctx context.Context) error {
	if !(atomic.CompareAndSwapInt32((*int32)(&txn.state), Open, Committing) || txn.state == Committing) {
		return newError(InvalidStatus, "Expect transaction state is Open but "+txn.state.string())
	}

	//Wait for all operations to complete
	<-txn.opsFlow
	//Send commit transaction command to transaction coordinator
	err := txn.tcClient.endTxn(txn.txnID, pb.TxnAction_COMMIT)
	if err != nil {
		atomic.StoreInt32((*int32)(&txn.state), Committed)
		return err
	}
	return nil
}

func (txn *transaction) Abort(ctx context.Context) error {
	if !(atomic.CompareAndSwapInt32((*int32)(&txn.state), Open, Aborting) || txn.state == Aborting) {
		return newError(InvalidStatus, "Expect transaction state is Open but "+txn.state.string())
	}

	//Wait for all operations to complete
	<-txn.opsFlow
	//Send abort transaction command to transaction coordinator
	err := txn.tcClient.endTxn(txn.txnID, pb.TxnAction_ABORT)
	if err != nil {
		atomic.StoreInt32((*int32)(&txn.state), Aborted)
		return err
	}
	return nil
}

func (txn *transaction) registerSendOrAckOp() {
	if txn.opCount.Inc() == 1 {
		//There are new operations that not completed
		<-txn.opsFlow
	}
}

func (txn *transaction) endSendOrAckOp(err error) {
	if err != nil {
		atomic.StoreInt32((*int32)(&txn.state), Errored)
	}
	if txn.opCount.Dec() == 0 {
		//This means there are not pending send/ack requests
		txn.opsFlow <- struct{}{}
	}
}

func (txn *transaction) registerProducedTopicAsync(topic string, callback func(err error)) {
	go func() {
		err := txn.registerProducerTopic(topic)
		callback(err)
	}()
}

func (txn *transaction) registerAckTopicAsync(topic string, subName string,
	callback func(err error)) {
	go func() {
		err := txn.registerAckTopic(topic, subName)
		callback(err)
	}()
}

func (txn *transaction) registerProducerTopic(topic string) error {
	isOpen, err := txn.checkIfOpen()
	if !isOpen {
		return err
	}
	_, ok := txn.registerPartitions[topic]
	if !ok {
		txn.Lock()
		defer txn.Unlock()
		err := txn.tcClient.addPublishPartitionToTxn(txn.txnID, []string{topic})
		if err != nil {
			return err
		}
		txn.registerPartitions[topic] = struct{}{}
		return nil
	} else {
		return nil
	}
}

func (txn *transaction) registerAckTopic(topic string, subName string) error {
	isOpen, err := txn.checkIfOpen()
	if !isOpen {
		return err
	}
	sub := subscription{
		topic:        topic,
		subscription: subName,
	}
	_, ok := txn.registerAckSubscriptions[sub]
	if !ok {
		txn.Lock()
		defer txn.Unlock()
		err := txn.tcClient.addSubscriptionToTxn(txn.txnID, topic, subName)
		if err != nil {
			return err
		}
		txn.registerAckSubscriptions[sub] = struct{}{}
		return nil
	} else {
		return nil
	}
}

func (txn *transaction) GetTxnID() TxnID {
	return txn.txnID
}

func (txn *transaction) checkIfOpen() (bool, error) {
	if txn.state == Open {
		return true, nil
	} else {
		return false, newError(InvalidStatus, "Expect transaction state is Open but "+txn.state.string())
	}
}

func (txn *transaction) checkIfOpenOrAborting() (bool, error) {
	if atomic.CompareAndSwapInt32((*int32)(&txn.state), Open, Aborting) || txn.state == Aborted {
		return true, nil
	} else {
		return false, newError(InvalidStatus, "Expect transaction state is Open but "+txn.state.string())
	}
}

func (state State) string() string {
	switch state {
	case Open:
		return "Open"
	case Committing:
		return "Committing"
	case Aborting:
		return "Aborting"
	case Committed:
		return "Committed"
	case Aborted:
		return "Aborted"
	case TimeOut:
		return "TimeOut"
	default:
		return "Unknown"
	}
}
