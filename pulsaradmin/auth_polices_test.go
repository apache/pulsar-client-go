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

package utils

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuthPolicies(t *testing.T) {
	testData := "{\n" +
		"  \"namespace_auth\": {\n" +
		"    \"persistent://public/default/ns_auth\": [\n" +
		"      \"produce\",\n" +
		"      \"consume\",\n" +
		"      \"function\"\n" +
		"    ]\n" +
		"  },\n" +
		"  \"destination_auth\": {\n" +
		"    \"persistent://public/default/dest_auth\": {\n" +
		"      \"admin-role\": [\n" +
		"        \"produce\",\n" +
		"        \"consume\",\n" +
		"        \"function\"\n" +
		"      ]\n" +
		"    },\n" +
		"    \"persistent://public/default/dest_auth_1\": {\n" +
		"      \"grant-partitioned-role\": [\n" +
		"        \"produce\",\n" +
		"        \"consume\"\n" +
		"      ]\n" +
		"    },\n" +
		"    \"persistent://public/default/test-revoke-partitioned-topic\": {},\n" +
		"    \"persistent://public/default/test-revoke-non-partitioned-topic\": {}\n  },\n" +
		"  \"subscription_auth_roles\": {}\n" +
		"}"

	policies := &AuthPolicies{}
	err := json.Unmarshal([]byte(testData), policies)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 3, len(policies.NamespaceAuth["persistent://public/default/ns_auth"]))
	assert.Equal(t, 4, len(policies.DestinationAuth))
	assert.Equal(t, 3, len(policies.DestinationAuth["persistent://public/default/dest_auth"]["admin-role"]))
	assert.Equal(t, 0, len(policies.SubscriptionAuthRoles))
}
