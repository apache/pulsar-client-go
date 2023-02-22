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
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"google.golang.org/protobuf/proto"
)

type transactionCoordinatorClient struct {
	client                    *client
	cons                      []internal.Connection
	epoch                     uint64
	semaphore                 internal.Semaphore
	blockIfReachMaxPendingOps bool
	//The number of transactionImpl coordinators
	tcNum uint64
	log   log.Logger
}

// TransactionCoordinatorAssign is the transaction_impl coordinator topic which is used to look up the broker
// where the TC located.
const TransactionCoordinatorAssign = "persistent://pulsar/system/transaction_coordinator_assign"

/*
*
Init a transactionImpl coordinator client and acquire connections with all transactionImpl coordinators.
*/
func newTransactionCoordinatorClientImpl(client *client) *transactionCoordinatorClient {
	tc := &transactionCoordinatorClient{
		client:                    client,
		blockIfReachMaxPendingOps: true,
		semaphore:                 internal.NewSemaphore(1000),
	}
	tc.log = client.log.SubLogger(log.Fields{})
	return tc
}

func (tc *transactionCoordinatorClient) start() error {
	r, err := tc.client.lookupService.GetPartitionedTopicMetadata(TransactionCoordinatorAssign)
	if err != nil {
		return err
	}
	tc.tcNum = uint64(r.Partitions)
	tc.cons = make([]internal.Connection, tc.tcNum)

	//Get connections with all transaction_impl coordinators which is synchronized
	for i := uint64(0); i < tc.tcNum; i++ {
		err := tc.grabConn(i)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tc *transactionCoordinatorClient) grabConn(partition uint64) error {
	lr, err := tc.client.lookupService.Lookup(getTCAssignTopicName(partition))
	if err != nil {
		tc.log.WithError(err).Warn("Failed to lookup the transaction_impl " +
			"coordinator assign topic [" + strconv.FormatUint(partition, 10) + "]")
		return err
	}

	requestID := tc.client.rpcClient.NewRequestID()
	cmdTCConnect := pb.CommandTcClientConnectRequest{
		RequestId: proto.Uint64(requestID),
		TcId:      proto.Uint64(partition),
	}

	res, err := tc.client.rpcClient.Request(lr.LogicalAddr, lr.PhysicalAddr, requestID,
		pb.BaseCommand_TC_CLIENT_CONNECT_REQUEST, &cmdTCConnect)

	if err != nil {
		tc.log.WithError(err).Error("Failed to connect transaction_impl coordinator " +
			strconv.FormatUint(partition, 10))
		return err
	}
	tc.cons[partition] = res.Cnx
	return nil
}

func (tc *transactionCoordinatorClient) close() {
	for _, con := range tc.cons {
		con.Close()
	}
}

/*
*
New a transactionImpl which can be used to guarantee exactly-once semantics.
*/
func (tc *transactionCoordinatorClient) newTransaction(timeout time.Duration) (*TxnID, error) {
	err := tc.canSendRequest()
	if err != nil {
		return &TxnID{}, err
	}
	requestID := tc.client.rpcClient.NewRequestID()
	nextTcID := tc.nextTCNumber()
	cmdNewTxn := &pb.CommandNewTxn{
		RequestId:     proto.Uint64(requestID),
		TcId:          proto.Uint64(nextTcID),
		TxnTtlSeconds: proto.Uint64(uint64(timeout.Milliseconds())),
	}

	cnx, err := tc.client.rpcClient.RequestOnCnx(tc.cons[nextTcID], requestID, pb.BaseCommand_NEW_TXN, cmdNewTxn)
	if err != nil {
		return &TxnID{}, err
	}

	return &TxnID{*cnx.Response.NewTxnResponse.TxnidMostBits,
		*cnx.Response.NewTxnResponse.TxnidLeastBits}, nil
}

/*
*
Register the partitions which published messages with the transactionImpl.
And this can be used when ending the transactionImpl.
*/
func (tc *transactionCoordinatorClient) addPublishPartitionToTxn(id *TxnID, partitions []string) error {
	err := tc.canSendRequest()
	if err != nil {
		return err
	}
	requestID := tc.client.rpcClient.NewRequestID()
	cmdAddPartitions := &pb.CommandAddPartitionToTxn{
		RequestId:      proto.Uint64(requestID),
		TxnidMostBits:  proto.Uint64(id.mostSigBits),
		TxnidLeastBits: proto.Uint64(id.leastSigBits),
		Partitions:     partitions,
	}
	_, err = tc.client.rpcClient.RequestOnCnx(tc.cons[id.mostSigBits], requestID,
		pb.BaseCommand_ADD_PARTITION_TO_TXN, cmdAddPartitions)
	return err
}

/*
Register the subscription which acked messages with the transactionImpl.
And this can be used when ending the transactionImpl.
*/
func (tc *transactionCoordinatorClient) addSubscriptionToTxn(id *TxnID, topic string, subscription string) error {
	err := tc.canSendRequest()
	if err != nil {
		return err
	}
	requestID := tc.client.rpcClient.NewRequestID()
	sub := &pb.Subscription{
		Topic:        &topic,
		Subscription: &subscription,
	}
	cmdAddSubscription := &pb.CommandAddSubscriptionToTxn{
		RequestId:      proto.Uint64(requestID),
		TxnidMostBits:  proto.Uint64(id.mostSigBits),
		TxnidLeastBits: proto.Uint64(id.leastSigBits),
		Subscription:   []*pb.Subscription{sub},
	}
	_, err = tc.client.rpcClient.RequestOnCnx(tc.cons[id.mostSigBits], requestID,
		pb.BaseCommand_ADD_SUBSCRIPTION_TO_TXN, cmdAddSubscription)
	return err
}

/*
*
Commit or abort the transactionImpl.
*/
func (tc *transactionCoordinatorClient) endTxn(id *TxnID, action pb.TxnAction) error {
	err := tc.canSendRequest()
	if err != nil {
		return err
	}
	requestID := tc.client.rpcClient.NewRequestID()
	cmdEndTxn := &pb.CommandEndTxn{
		RequestId:      proto.Uint64(requestID),
		TxnAction:      &action,
		TxnidMostBits:  proto.Uint64(id.mostSigBits),
		TxnidLeastBits: proto.Uint64(id.leastSigBits),
	}
	_, err = tc.client.rpcClient.RequestOnCnx(tc.cons[id.mostSigBits], requestID, pb.BaseCommand_END_TXN, cmdEndTxn)
	return err
}

func getTCAssignTopicName(partition uint64) string {
	return TransactionCoordinatorAssign + "-partition-" + strconv.FormatUint(partition, 10)
}

func (tc *transactionCoordinatorClient) canSendRequest() error {
	if tc.blockIfReachMaxPendingOps {
		if !tc.semaphore.Acquire(context.Background()) {
			return newError(UnknownError, "Failed to acquire semaphore")
		}
	} else {
		if !tc.semaphore.TryAcquire() {
			return newError(ReachMaxPendingOps, "transaction_impl coordinator reach max pending ops")
		}
	}
	return nil
}

func (tc *transactionCoordinatorClient) nextTCNumber() uint64 {
	return atomic.AddUint64(&tc.epoch, 1) % tc.tcNum
}
