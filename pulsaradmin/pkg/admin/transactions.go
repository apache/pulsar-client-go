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

package admin

import (
	"strconv"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

// Transactions is admin interface for transaction coordinator management
type Transactions interface {
	// GetCoordinatorStats returns the stats of transaction coordinators
	GetCoordinatorStats() (map[string]utils.TransactionCoordinatorStats, error)

	// GetCoordinatorStatsWithCoordinatorID returns the stats of a specific transaction coordinator
	GetCoordinatorStatsWithCoordinatorID(coordinatorID int) (*utils.TransactionCoordinatorStats, error)

	// GetCoordinatorInternalStats returns internal stats of transaction coordinators
	GetCoordinatorInternalStats(coordinatorID int, metadata bool) (map[string]interface{}, error)

	// GetPendingAckStats returns pending ack stats for a subscription
	GetPendingAckStats(topic utils.TopicName, subName string, metadata bool) (map[string]interface{}, error)

	// GetPendingAckInternalStats returns internal pending ack stats for a subscription  
	GetPendingAckInternalStats(topic utils.TopicName, subName string, metadata bool) (map[string]interface{}, error)

	// GetTransactionInBufferStats returns transaction in buffer stats for a topic
	GetTransactionInBufferStats(topic utils.TopicName, mostSigBits, leastSigBits int64, metadata bool) (map[string]interface{}, error)

	// GetSlowTransactions returns slow transactions
	GetSlowTransactions(coordinatorID int, timeout int64) (map[string]interface{}, error)
}

type transactions struct {
	pulsar   *pulsarClient
	basePath string
}

// Transactions is used to access the transactions endpoints
func (c *pulsarClient) Transactions() Transactions {
	return &transactions{
		pulsar:   c,
		basePath: "/transactions",
	}
}

func (t *transactions) GetCoordinatorStats() (map[string]utils.TransactionCoordinatorStats, error) {
	var stats map[string]utils.TransactionCoordinatorStats
	endpoint := t.pulsar.endpoint(t.basePath, "coordinatorStats")
	err := t.pulsar.Client.Get(endpoint, &stats)
	return stats, err
}

func (t *transactions) GetCoordinatorStatsWithCoordinatorID(coordinatorID int) (*utils.TransactionCoordinatorStats, error) {
	var stats utils.TransactionCoordinatorStats
	endpoint := t.pulsar.endpoint(t.basePath, "coordinatorStats", strconv.Itoa(coordinatorID))
	err := t.pulsar.Client.Get(endpoint, &stats)
	return &stats, err
}

func (t *transactions) GetCoordinatorInternalStats(coordinatorID int, metadata bool) (map[string]interface{}, error) {
	var stats map[string]interface{}
	endpoint := t.pulsar.endpoint(t.basePath, "coordinatorInternalStats", strconv.Itoa(coordinatorID))
	params := map[string]string{
		"metadata": strconv.FormatBool(metadata),
	}
	_, err := t.pulsar.Client.GetWithQueryParams(endpoint, &stats, params, false)
	return stats, err
}

func (t *transactions) GetPendingAckStats(topic utils.TopicName, subName string, metadata bool) (map[string]interface{}, error) {
	var stats map[string]interface{}
	endpoint := t.pulsar.endpoint(t.basePath, "pendingAckStats", topic.GetRestPath(), "subscription", subName)
	params := map[string]string{
		"metadata": strconv.FormatBool(metadata),
	}
	_, err := t.pulsar.Client.GetWithQueryParams(endpoint, &stats, params, false)
	return stats, err
}

func (t *transactions) GetPendingAckInternalStats(topic utils.TopicName, subName string, metadata bool) (map[string]interface{}, error) {
	var stats map[string]interface{}
	endpoint := t.pulsar.endpoint(t.basePath, "pendingAckInternalStats", topic.GetRestPath(), "subscription", subName)
	params := map[string]string{
		"metadata": strconv.FormatBool(metadata),
	}
	_, err := t.pulsar.Client.GetWithQueryParams(endpoint, &stats, params, false)
	return stats, err
}

func (t *transactions) GetTransactionInBufferStats(topic utils.TopicName, mostSigBits, leastSigBits int64, metadata bool) (map[string]interface{}, error) {
	var stats map[string]interface{}
	endpoint := t.pulsar.endpoint(t.basePath, "transactionInBufferStats", topic.GetRestPath(), strconv.FormatInt(mostSigBits, 10), strconv.FormatInt(leastSigBits, 10))
	params := map[string]string{
		"metadata": strconv.FormatBool(metadata),
	}
	_, err := t.pulsar.Client.GetWithQueryParams(endpoint, &stats, params, false)
	return stats, err
}

func (t *transactions) GetSlowTransactions(coordinatorID int, timeout int64) (map[string]interface{}, error) {
	var stats map[string]interface{}
	endpoint := t.pulsar.endpoint(t.basePath, "slowTransactions", strconv.Itoa(coordinatorID))
	params := map[string]string{
		"timeout": strconv.FormatInt(timeout, 10),
	}
	_, err := t.pulsar.Client.GetWithQueryParams(endpoint, &stats, params, false)
	return stats, err
}