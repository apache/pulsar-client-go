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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/pkg/errors"
	uAtomic "go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
)

type transactionCoordinatorClient struct {
	client    *client
	handlers  []*transactionHandler
	epoch     uint64
	semaphore internal.Semaphore
	log       log.Logger
}

type transactionHandler struct {
	tc              *transactionCoordinatorClient
	state           uAtomic.Int32
	conn            uAtomic.Value
	partition       uint64
	closeCh         chan any
	requestCh       chan any
	connectClosedCh chan *connectionClosed
	log             log.Logger
}

type txnHandlerState int

const (
	txnHandlerReady = iota
	txnHandlerClosed
)

var ErrMaxConcurrentOpsReached = newError(MaxConcurrentOperationsReached, "Max concurrent operations reached")
var ErrTransactionCoordinatorNotEnabled = newError(TransactionCoordinatorNotEnabled, "The broker doesn't enable "+
	"the transaction coordinator, or the transaction coordinator has not initialized")

func (t *transactionHandler) getState() txnHandlerState {
	return txnHandlerState(t.state.Load())
}

func (t *transactionHandler) setState(state txnHandlerState) {
	t.state.Store(int32(state))
}

func (tc *transactionCoordinatorClient) newTransactionHandler(partition uint64) (*transactionHandler, error) {
	handler := &transactionHandler{
		tc:              tc,
		partition:       partition,
		closeCh:         make(chan any),
		requestCh:       make(chan any),
		connectClosedCh: make(chan *connectionClosed),
		log:             tc.log.SubLogger(log.Fields{"txn handler partition": partition}),
	}
	err := handler.grabConn()
	if err != nil {
		return nil, err
	}
	go handler.runEventsLoop()
	return handler, nil
}

func (t *transactionHandler) grabConn() error {
	lr, err := t.tc.client.lookupService.Lookup(getTCAssignTopicName(t.partition))
	if err != nil {
		t.log.WithError(err).Warn("Failed to lookup the transaction_impl " +
			"coordinator assign topic [" + strconv.FormatUint(t.partition, 10) + "]")
		return err
	}

	requestID := t.tc.client.rpcClient.NewRequestID()
	cmdTCConnect := pb.CommandTcClientConnectRequest{
		RequestId: proto.Uint64(requestID),
		TcId:      proto.Uint64(t.partition),
	}

	res, err := t.tc.client.rpcClient.Request(lr.LogicalAddr, lr.PhysicalAddr, requestID,
		pb.BaseCommand_TC_CLIENT_CONNECT_REQUEST, &cmdTCConnect)

	if err != nil {
		t.log.WithError(err).Error("Failed to connect transaction_impl coordinator " +
			strconv.FormatUint(t.partition, 10))
		return err
	}

	go func() {
		select {
		case <-t.closeCh:
			return
		case <-res.Cnx.WaitForClose():
			t.connectClosedCh <- &connectionClosed{}
		}
	}()
	t.conn.Store(res.Cnx)
	t.log.Infof("Transaction handler with transaction coordinator id %d connected", t.partition)
	return nil
}

func (t *transactionHandler) getConn() internal.Connection {
	return t.conn.Load().(internal.Connection)
}

func (t *transactionHandler) runEventsLoop() {
	for {
		select {
		case <-t.closeCh:
			return
		case req := <-t.requestCh:
			switch r := req.(type) {
			case *newTxnOp:
				t.newTransaction(r)
			case *addPublishPartitionOp:
				t.addPublishPartitionToTxn(r)
			case *addSubscriptionOp:
				t.addSubscriptionToTxn(r)
			case *endTxnOp:
				t.endTxn(r)
			}
		case <-t.connectClosedCh:
			t.log.Infof("Transaction handler %d will reconnect to the transaction coordinator", t.partition)
			t.reconnectToBroker()
		}
	}
}

func (t *transactionHandler) reconnectToBroker() {
	var delayReconnectTime time.Duration
	var defaultBackoff = internal.DefaultBackoff{}

	for {
		if t.getState() == txnHandlerClosed {
			// The handler is already closing
			t.log.Info("transaction handler is closed, exit reconnect")
			return
		}

		delayReconnectTime = defaultBackoff.Next()

		t.log.WithFields(log.Fields{
			"delayReconnectTime": delayReconnectTime,
		}).Info("Reconnecting to broker")
		time.Sleep(delayReconnectTime)

		// double check
		if t.getState() == txnHandlerClosed {
			// Txn handler is already closing
			t.log.Info("transaction handler is closed, exit reconnect")
			return
		}

		err := t.grabConn()
		if err == nil {
			// Successfully reconnected
			t.log.Info("Reconnected transaction handler to broker")
			return
		}
		t.log.WithError(err).Error("Failed to create transaction handler at reconnect")
		errMsg := err.Error()
		if strings.Contains(errMsg, errMsgTopicNotFound) {
			// when topic is deleted, we should give up reconnection.
			t.log.Warn("Topic Not Found.")
			break
		}
	}
}

func (t *transactionHandler) checkRetriableError(err error, op any) bool {
	if err != nil && errors.Is(err, internal.ErrConnectionClosed) {
		// We are in the EventLoop here, so we need to insert the request back to the requestCh asynchronously.
		go func() {
			t.requestCh <- op
		}()
		return true
	}
	return false
}

type newTxnOp struct {
	// Request
	timeout time.Duration

	// Response
	done  chan any
	err   error
	txnID *TxnID
}

func (t *transactionHandler) newTransaction(op *newTxnOp) {
	requestID := t.tc.client.rpcClient.NewRequestID()
	nextTcID := t.tc.nextTCNumber()
	cmdNewTxn := &pb.CommandNewTxn{
		RequestId:     proto.Uint64(requestID),
		TcId:          proto.Uint64(nextTcID),
		TxnTtlSeconds: proto.Uint64(uint64(op.timeout.Milliseconds())),
	}
	res, err := t.tc.client.rpcClient.RequestOnCnx(t.getConn(), requestID, pb.BaseCommand_NEW_TXN, cmdNewTxn)
	if t.checkRetriableError(err, op) {
		return
	}
	defer close(op.done)
	defer t.tc.semaphore.Release()
	if err != nil {
		op.err = err
	} else if res.Response.NewTxnResponse.Error != nil {
		op.err = getErrorFromServerError(res.Response.NewTxnResponse.Error)
	} else {
		op.txnID = &TxnID{*res.Response.NewTxnResponse.TxnidMostBits,
			*res.Response.NewTxnResponse.TxnidLeastBits}
	}
}

type addPublishPartitionOp struct {
	// Request
	id         *TxnID
	partitions []string

	// Response
	done chan any
	err  error
}

func (t *transactionHandler) addPublishPartitionToTxn(op *addPublishPartitionOp) {
	requestID := t.tc.client.rpcClient.NewRequestID()
	cmdAddPartitions := &pb.CommandAddPartitionToTxn{
		RequestId:      proto.Uint64(requestID),
		TxnidMostBits:  proto.Uint64(op.id.MostSigBits),
		TxnidLeastBits: proto.Uint64(op.id.LeastSigBits),
		Partitions:     op.partitions,
	}
	res, err := t.tc.client.rpcClient.RequestOnCnx(t.getConn(), requestID,
		pb.BaseCommand_ADD_PARTITION_TO_TXN, cmdAddPartitions)
	if t.checkRetriableError(err, op) {
		return
	}
	defer close(op.done)
	defer t.tc.semaphore.Release()
	if err != nil {
		op.err = err
	} else if res.Response.AddPartitionToTxnResponse.Error != nil {
		op.err = getErrorFromServerError(res.Response.AddPartitionToTxnResponse.Error)
	}
}

type addSubscriptionOp struct {
	// Request
	id           *TxnID
	topic        string
	subscription string

	// Response
	done chan any
	err  error
}

func (t *transactionHandler) addSubscriptionToTxn(op *addSubscriptionOp) {
	requestID := t.tc.client.rpcClient.NewRequestID()
	sub := &pb.Subscription{
		Topic:        &op.topic,
		Subscription: &op.subscription,
	}
	cmdAddSubscription := &pb.CommandAddSubscriptionToTxn{
		RequestId:      proto.Uint64(requestID),
		TxnidMostBits:  proto.Uint64(op.id.MostSigBits),
		TxnidLeastBits: proto.Uint64(op.id.LeastSigBits),
		Subscription:   []*pb.Subscription{sub},
	}
	res, err := t.tc.client.rpcClient.RequestOnCnx(t.getConn(), requestID,
		pb.BaseCommand_ADD_SUBSCRIPTION_TO_TXN, cmdAddSubscription)
	if t.checkRetriableError(err, op) {
		return
	}
	defer close(op.done)
	defer t.tc.semaphore.Release()
	if err != nil {
		op.err = err
	} else if res.Response.AddSubscriptionToTxnResponse.Error != nil {
		op.err = getErrorFromServerError(res.Response.AddSubscriptionToTxnResponse.Error)
	}
}

type endTxnOp struct {
	// Request
	id     *TxnID
	action pb.TxnAction

	// Response
	done chan any
	err  error
}

func (t *transactionHandler) endTxn(op *endTxnOp) {
	requestID := t.tc.client.rpcClient.NewRequestID()
	cmdEndTxn := &pb.CommandEndTxn{
		RequestId:      proto.Uint64(requestID),
		TxnAction:      &op.action,
		TxnidMostBits:  proto.Uint64(op.id.MostSigBits),
		TxnidLeastBits: proto.Uint64(op.id.LeastSigBits),
	}
	res, err := t.tc.client.rpcClient.RequestOnCnx(t.getConn(), requestID, pb.BaseCommand_END_TXN, cmdEndTxn)
	if t.checkRetriableError(err, op) {
		return
	}
	defer close(op.done)
	defer t.tc.semaphore.Release()
	if err != nil {
		op.err = err
	} else if res.Response.EndTxnResponse.Error != nil {
		op.err = getErrorFromServerError(res.Response.EndTxnResponse.Error)
	}
}

func (t *transactionHandler) close() {
	if t.getState() != txnHandlerReady {
		return
	}
	close(t.closeCh)
	t.setState(txnHandlerClosed)
}

// TransactionCoordinatorAssign is the transaction_impl coordinator topic which is used to look up the broker
// where the TC located.
const TransactionCoordinatorAssign = "persistent://pulsar/system/transaction_coordinator_assign"

// newTransactionCoordinatorClientImpl init a transactionImpl coordinator client and
// acquire connections with all transactionImpl coordinators.
func newTransactionCoordinatorClientImpl(client *client) *transactionCoordinatorClient {
	tc := &transactionCoordinatorClient{
		client:    client,
		semaphore: internal.NewSemaphore(1000),
	}
	tc.log = client.log.SubLogger(log.Fields{})
	return tc
}

func (tc *transactionCoordinatorClient) start() error {
	r, err := tc.client.lookupService.GetPartitionedTopicMetadata(TransactionCoordinatorAssign)
	if err != nil {
		return err
	}
	tc.handlers = make([]*transactionHandler, r.Partitions)
	//Get connections with all transaction_impl coordinators which is synchronized
	if r.Partitions <= 0 {
		return ErrTransactionCoordinatorNotEnabled
	}
	for i := 0; i < r.Partitions; i++ {
		handler, err := tc.newTransactionHandler(uint64(i))
		if err != nil {
			tc.log.WithError(err).Errorf("Failed to create transaction handler %d", i)
			return err
		}
		tc.handlers[uint64(i)] = handler
	}
	return nil
}

func (tc *transactionCoordinatorClient) close() {
	for _, h := range tc.handlers {
		h.close()
	}
}

// newTransaction new a transactionImpl which can be used to guarantee exactly-once semantics.
func (tc *transactionCoordinatorClient) newTransaction(timeout time.Duration) (*TxnID, error) {
	if err := tc.canSendRequest(); err != nil {
		return nil, err
	}
	defer tc.semaphore.Release()
	op := &newTxnOp{
		timeout: timeout,
		done:    make(chan any),
	}
	tc.handlers[tc.nextTCNumber()].requestCh <- op
	<-op.done
	if op.err != nil {
		return nil, op.err
	}
	return op.txnID, nil
}

// addPublishPartitionToTxn register the partitions which published messages with the transactionImpl.
// And this can be used when ending the transactionImpl.
func (tc *transactionCoordinatorClient) addPublishPartitionToTxn(id *TxnID, partitions []string) error {
	if err := tc.canSendRequest(); err != nil {
		return err
	}
	defer tc.semaphore.Release()
	op := &addPublishPartitionOp{
		id:         id,
		partitions: partitions,
		done:       make(chan any),
	}
	tc.handlers[id.MostSigBits].requestCh <- op
	<-op.done
	if op.err != nil {
		return op.err
	}
	return nil
}

// addSubscriptionToTxn register the subscription which acked messages with the transactionImpl.
// And this can be used when ending the transactionImpl.
func (tc *transactionCoordinatorClient) addSubscriptionToTxn(id *TxnID, topic string, subscription string) error {
	if err := tc.canSendRequest(); err != nil {
		return err
	}
	defer tc.semaphore.Release()
	op := &addSubscriptionOp{
		id:           id,
		topic:        topic,
		subscription: subscription,
		done:         make(chan any),
	}
	tc.handlers[id.MostSigBits].requestCh <- op
	<-op.done
	if op.err != nil {
		return op.err
	}
	return nil
}

// endTxn commit or abort the transactionImpl.
func (tc *transactionCoordinatorClient) endTxn(id *TxnID, action pb.TxnAction) error {
	if err := tc.canSendRequest(); err != nil {
		return err
	}
	defer tc.semaphore.Release()
	op := &endTxnOp{
		id:     id,
		action: action,
		done:   make(chan any),
	}
	tc.handlers[id.MostSigBits].requestCh <- op
	<-op.done
	if op.err != nil {
		return op.err
	}
	return nil
}

func getTCAssignTopicName(partition uint64) string {
	return TransactionCoordinatorAssign + "-partition-" + strconv.FormatUint(partition, 10)
}

func (tc *transactionCoordinatorClient) canSendRequest() error {
	if !tc.semaphore.Acquire(context.Background()) {
		return ErrMaxConcurrentOpsReached
	}
	return nil
}

func (tc *transactionCoordinatorClient) nextTCNumber() uint64 {
	return atomic.AddUint64(&tc.epoch, 1) % uint64(len(tc.handlers))
}
