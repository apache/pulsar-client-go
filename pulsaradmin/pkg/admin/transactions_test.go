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
	"testing"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactions_GetCoordinatorStats(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	// get coordinator stats
	stats, err := admin.Transactions().GetCoordinatorStats()
	assert.NoError(t, err)
	assert.NotNil(t, stats)
}

func TestTransactions_GetCoordinatorStatsWithID(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	// get coordinator stats for coordinator ID 0
	stats, err := admin.Transactions().GetCoordinatorStatsWithCoordinatorID(0)
	assert.NoError(t, err)
	assert.NotNil(t, stats)
}

func TestTransactions_GetSlowTransactions(t *testing.T) {
	config := &config.Config{}
	admin, err := New(config)
	require.NoError(t, err)
	require.NotNil(t, admin)

	// get slow transactions
	stats, err := admin.Transactions().GetSlowTransactions(0, 1000)
	assert.NoError(t, err)
	assert.NotNil(t, stats)
}