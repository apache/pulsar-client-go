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

package padmin

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespaces(t *testing.T) {
	broker := startTestBroker(t)
	defer broker.Close()
	admin := NewTestPulsarAdmin(t, broker.webPort)
	testTenant := RandStr(8)
	err := admin.Tenants.Create(testTenant, TenantInfo{
		AllowedClusters: []string{"standalone"},
	})
	require.Nil(t, err)
	testNs := RandStr(8)
	err = admin.Namespaces.Create(testTenant, testNs)
	require.Nil(t, err)
	namespaces, err := admin.Namespaces.List(testTenant)
	require.Nil(t, err)
	assert.Contains(t, namespaces, fmt.Sprintf("%s/%s", testTenant, testNs))
	err = admin.Namespaces.Delete(testTenant, testNs)
	require.Nil(t, err)
}

func TestNamespacesImpl_OperateNamespaceRetention(t *testing.T) {
	broker := startTestBroker(t)
	defer broker.Close()
	admin := NewTestPulsarAdmin(t, broker.webPort)
	testTenant := RandStr(8)
	err := admin.Tenants.Create(testTenant, TenantInfo{
		AllowedClusters: []string{"standalone"},
	})
	require.Nil(t, err)
	testNs := RandStr(8)
	err = admin.Namespaces.Create(testTenant, testNs)
	require.Nil(t, err)
	err = admin.Namespaces.SetNamespaceRetention(testTenant, testNs, &RetentionConfiguration{
		RetentionSizeInMB:      100,
		RetentionTimeInMinutes: 10,
	})
	require.Nil(t, err)
	cfg, err := admin.Namespaces.GetNamespaceRetention(testTenant, testNs)
	require.Nil(t, err)
	require.EqualValues(t, 100, cfg.RetentionSizeInMB)
	require.EqualValues(t, 10, cfg.RetentionTimeInMinutes)
	err = admin.Namespaces.RemoveNamespaceRetention(testTenant, testNs)
	require.Nil(t, err)
	cfg, err = admin.Namespaces.GetNamespaceRetention(testTenant, testNs)
	require.Nil(t, err)
	require.EqualValues(t, 0, cfg.RetentionSizeInMB)
	require.EqualValues(t, 0, cfg.RetentionTimeInMinutes)
}

func TestNamespacesImpl_GetBacklogQuota(t *testing.T) {
	broker := startTestBroker(t)
	defer broker.Close()
	admin := NewTestPulsarAdmin(t, broker.webPort)
	testTenant := RandStr(8)
	err := admin.Tenants.Create(testTenant, TenantInfo{
		AllowedClusters: []string{"standalone"},
	})
	require.Nil(t, err)
	testNs := RandStr(8)
	err = admin.Namespaces.Create(testTenant, testNs)
	require.Nil(t, err)
	info, err := admin.Namespaces.GetNamespaceBacklogQuota(testTenant, testNs)
	require.Nil(t, err)
	t.Logf("get quota info: %+v", info)
}

func TestNamespacesImpl_SetNamespaceBacklogQuota(t *testing.T) {
	broker := startTestBroker(t)
	defer broker.Close()
	admin := NewTestPulsarAdmin(t, broker.webPort)
	testTenant := RandStr(8)
	err := admin.Tenants.Create(testTenant, TenantInfo{
		AllowedClusters: []string{"standalone"},
	})
	require.Nil(t, err)
	testNs := RandStr(8)
	err = admin.Namespaces.Create(testTenant, testNs)
	require.Nil(t, err)
	err = admin.Namespaces.SetNamespaceBacklogQuota(testTenant, testNs, &BacklogQuota{
		Limit:     100,
		LimitSize: 100,
		LimitTime: 30,
		Policy:    ProducerRequestHold,
	})
	require.Nil(t, err)
}

func TestNamespacesImpl_RemoveNamespaceBacklogQuota(t *testing.T) {
	broker := startTestBroker(t)
	defer broker.Close()
	admin := NewTestPulsarAdmin(t, broker.webPort)
	testTenant := RandStr(8)
	err := admin.Tenants.Create(testTenant, TenantInfo{
		AllowedClusters: []string{"standalone"},
	})
	require.Nil(t, err)
	testNs := RandStr(8)
	err = admin.Namespaces.Create(testTenant, testNs)
	require.Nil(t, err)
	err = admin.Namespaces.RemoveNamespaceBacklogQuota(testTenant, testNs)
	require.Nil(t, err)
}

func TestNamespacesImpl_ClearNamespaceAllTopicsBacklog(t *testing.T) {
	broker := startTestBroker(t)
	defer broker.Close()
	admin := NewTestPulsarAdmin(t, broker.webPort)
	testTenant := RandStr(8)
	err := admin.Tenants.Create(testTenant, TenantInfo{
		AllowedClusters: []string{"standalone"},
	})
	require.Nil(t, err)
	testNs := RandStr(8)
	err = admin.Namespaces.Create(testTenant, testNs)
	require.Nil(t, err)
	err = admin.Namespaces.ClearNamespaceAllTopicsBacklog(testTenant, testNs)
	require.Nil(t, err)
}

func TestNamespacesImpl_OperateMessageTTL(t *testing.T) {
	broker := startTestBroker(t)
	defer broker.Close()
	admin := NewTestPulsarAdmin(t, broker.webPort)
	testTenant := RandStr(8)
	err := admin.Tenants.Create(testTenant, TenantInfo{
		AllowedClusters: []string{"standalone"},
	})
	require.Nil(t, err)
	testNs := RandStr(8)
	err = admin.Namespaces.Create(testTenant, testNs)
	require.Nil(t, err)
	namespaces, err := admin.Namespaces.List(testTenant)
	require.Nil(t, err)
	assert.Contains(t, namespaces, fmt.Sprintf("%s/%s", testTenant, testNs))
	_, err = admin.Namespaces.GetNamespaceMessageTTL(testTenant, testNs)
	require.Nil(t, err)
	err = admin.Namespaces.SetNamespaceMessageTTL(testTenant, testNs, 30)
	require.Nil(t, err)
	ttl, err := admin.Namespaces.GetNamespaceMessageTTL(testTenant, testNs)
	require.Nil(t, err)
	require.EqualValues(t, 30, ttl)
	err = admin.Namespaces.RemoveNamespaceMessageTTL(testTenant, testNs)
	require.Nil(t, err)
	ttl, err = admin.Namespaces.GetNamespaceMessageTTL(testTenant, testNs)
	require.Nil(t, err)
	require.EqualValues(t, 0, ttl)
}

func TestNamespacesImpl_OperateCompaction(t *testing.T) {
	broker := startTestBroker(t)
	defer broker.Close()
	admin := NewTestPulsarAdmin(t, broker.webPort)
	testTenant := RandStr(8)
	err := admin.Tenants.Create(testTenant, TenantInfo{
		AllowedClusters: []string{"standalone"},
	})
	require.Nil(t, err)
	testNs := RandStr(8)
	err = admin.Namespaces.Create(testTenant, testNs)
	require.Nil(t, err)
	namespaces, err := admin.Namespaces.List(testTenant)
	require.Nil(t, err)
	assert.Contains(t, namespaces, fmt.Sprintf("%s/%s", testTenant, testNs))
	threshold, err := admin.Namespaces.GetMaximumUnCompactedBytes(testTenant, testNs)
	require.Nil(t, err)
	require.EqualValues(t, 0, threshold)
	err = admin.Namespaces.SetMaximumUnCompactedBytes(testTenant, testNs, 10)
	require.Nil(t, err)
	threshold, err = admin.Namespaces.GetMaximumUnCompactedBytes(testTenant, testNs)
	require.Nil(t, err)
	require.EqualValues(t, 10, threshold)
	err = admin.Namespaces.RemoveMaximumUnCompactedBytes(testTenant, testNs)
	require.Nil(t, err)
	threshold, err = admin.Namespaces.GetMaximumUnCompactedBytes(testTenant, testNs)
	require.Nil(t, err)
	require.EqualValues(t, 0, threshold)
}
