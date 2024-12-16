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
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

type subscription struct {
	topic        string
	subscription string
}

type transaction struct {
	mu                       sync.Mutex
	txnID                    TxnID
	state                    atomic.Int32
	tcClient                 *transactionCoordinatorClient
	registerPartitions       map[string]bool
	registerAckSubscriptions map[subscription]bool
	// opsFlow It has two effects:
	// 1. Wait all the operations of sending and acking messages with the transaction complete
	// by reading msg from the chan.
	// 2. Prevent sending or acking messages with a committed or aborted transaction.
	// opsCount is record the number of the uncompleted operations.
	// opsFlow
	//   Write:
	//     1. When the transaction is created, a bool will be written to opsFlow chan.
	//     2. When the opsCount decrement from 1 to 0, a new bool will be written to opsFlow chan.
	//     3. When get a retryable error after committing or aborting the transaction,
	//        a bool will be written to opsFlow chan.
	//   Read:
	//     1. When the transaction is committed or aborted, a bool will be read from opsFlow chan.
	//     2. When the opsCount increment from 0 to 1, a bool will be read from opsFlow chan.
	opsFlow   chan bool
	opsCount  atomic.Int32
	opTimeout time.Duration
	log       log.Logger
}

func newTransaction(id TxnID, tcClient *transactionCoordinatorClient, timeout time.Duration) *transaction {
	transaction := &transaction{
		txnID:                    id,
		registerPartitions:       make(map[string]bool),
		registerAckSubscriptions: make(map[subscription]bool),
		opsFlow:                  make(chan bool, 1),
		opTimeout:                tcClient.client.operationTimeout,
		tcClient:                 tcClient,
	}
	transaction.state.Store(int32(TxnOpen))
	// This means there are not pending requests with this transaction. The transaction can be committed or aborted.
	transaction.opsFlow <- true
	go func() {
		// Set the state of the transaction to timeout after timeout
		<-time.After(timeout)
		transaction.state.CompareAndSwap(int32(TxnOpen), int32(TxnTimeout))
	}()
	transaction.log = tcClient.log.SubLogger(log.Fields{})
	return transaction
}

func (txn *transaction) GetState() TxnState {
	return TxnState(txn.state.Load())
}

func (txn *transaction) Commit(_ context.Context) error {
	if !(txn.state.CompareAndSwap(int32(TxnOpen), int32(TxnCommitting))) {
		txnState := txn.state.Load()
		return newError(InvalidStatus, txnStateErrorMessage(TxnOpen, TxnState(txnState)))
	}

	// Wait for all operations to complete
	select {
	case <-txn.opsFlow:
	case <-ctx.Done():
		txn.state.Store(int32(TxnOpen))
		return ctx.Err()
	case <-time.After(txn.opTimeout):
		txn.state.Store(int32(TxnTimeout))
		return newError(TimeoutError, "There are some operations that are not completed after the timeout.")
	}
	// Send commit transaction command to transaction coordinator
	err := txn.tcClient.endTxn(&txn.txnID, pb.TxnAction_COMMIT)
	if err == nil {
		txn.state.Store(int32(TxnCommitted))
	} else {
		var e *Error
		if errors.As(err, &e) && (e.Result() == TransactionNoFoundError || e.Result() == InvalidStatus) {
			txn.state.Store(int32(TxnError))
			return err
		}
		txn.opsFlow <- true
	}
	return err
}

func (txn *transaction) Abort(_ context.Context) error {
	if !(txn.state.CompareAndSwap(int32(TxnOpen), int32(TxnAborting))) {
		txnState := txn.state.Load()
		return newError(InvalidStatus, txnStateErrorMessage(TxnOpen, TxnState(txnState)))
	}

	// Wait for all operations to complete
	select {
	case <-txn.opsFlow:
	case <-ctx.Done():
		txn.state.Store(int32(TxnOpen))
		return ctx.Err()
	case <-time.After(txn.opTimeout):
		txn.state.Store(int32(TxnTimeout))
		return newError(TimeoutError, "There are some operations that are not completed after the timeout.")
	}
	// Send abort transaction command to transaction coordinator
	err := txn.tcClient.endTxn(&txn.txnID, pb.TxnAction_ABORT)
	if err == nil {
		txn.state.Store(int32(TxnAborted))
	} else {
		var e *Error
		if errors.As(err, &e) && (e.Result() == TransactionNoFoundError || e.Result() == InvalidStatus) {
			txn.state.Store(int32(TxnError))
			return err
		}
		txn.opsFlow <- true
	}
	return err
}

func (txn *transaction) registerSendOrAckOp() error {
	if txn.opsCount.Add(1) == 1 {
		// There are new operations that were not completed
		select {
		case <-txn.opsFlow:
			return nil
		case <-time.After(txn.opTimeout):
			if err := txn.verifyOpen(); err != nil {
				return err
			}
			return newError(TimeoutError, "Failed to get the semaphore to register the send/ack operation")
		}
	}
	return nil
}

func (txn *transaction) endSendOrAckOp(err error) {
	if err != nil {
		txn.state.Store(int32(TxnError))
	}
	if txn.opsCount.Add(-1) == 0 {
		// This means there are no pending send/ack requests
		txn.opsFlow <- true
	}
}

func (txn *transaction) registerProducerTopic(topic string) error {
	if err := txn.verifyOpen(); err != nil {
		return err
	}
	_, ok := txn.registerPartitions[topic]
	if !ok {
		txn.mu.Lock()
		defer txn.mu.Unlock()
		if _, ok = txn.registerPartitions[topic]; !ok {
			err := txn.tcClient.addPublishPartitionToTxn(&txn.txnID, []string{topic})
			if err != nil {
				return err
			}
			txn.registerPartitions[topic] = true
		}
	}
	return nil
}

func (txn *transaction) registerAckTopic(topic string, subName string) error {
	if err := txn.verifyOpen(); err != nil {
		return err
	}
	sub := subscription{
		topic:        topic,
		subscription: subName,
	}
	_, ok := txn.registerAckSubscriptions[sub]
	if !ok {
		txn.mu.Lock()
		defer txn.mu.Unlock()
		if _, ok = txn.registerAckSubscriptions[sub]; !ok {
			err := txn.tcClient.addSubscriptionToTxn(&txn.txnID, topic, subName)
			if err != nil {
				return err
			}
			txn.registerAckSubscriptions[sub] = true
		}
	}
	return nil
}

func (txn *transaction) GetTxnID() TxnID {
	return txn.txnID
}

func (txn *transaction) verifyOpen() error {
	txnState := txn.state.Load()
	if txnState != int32(TxnOpen) {
		return newError(InvalidStatus, txnStateErrorMessage(TxnOpen, TxnState(txnState)))
	}
	return nil
}

func (state TxnState) String() string {
	switch state {
	case TxnOpen:
		return "TxnOpen"
	case TxnCommitting:
		return "TxnCommitting"
	case TxnAborting:
		return "TxnAborting"
	case TxnCommitted:
		return "TxnCommitted"
	case TxnAborted:
		return "TxnAborted"
	case TxnTimeout:
		return "TxnTimeout"
	case TxnError:
		return "TxnError"
	default:
		return "Unknown"
	}
}

//nolint:unparam
func txnStateErrorMessage(expected, actual TxnState) string {
	return fmt.Sprintf("Expected transaction state: %s, actual: %s", expected, actual)
}
