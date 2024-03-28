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
	"os"
	"testing"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestBrokerHealthCheckWithTopicVersion(t *testing.T) {
	readFile, err := os.ReadFile("../../../integration-tests/tokens/admin-token")
	assert.NoError(t, err)
	cfg := &config.Config{
		Token: string(readFile),
	}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)
	err = admin.Brokers().HealthCheck()
	assert.NoError(t, err)
	err = admin.Brokers().HealthCheckWithTopicVersion(utils.V1)
	assert.NoError(t, err)
	err = admin.Brokers().HealthCheckWithTopicVersion(utils.V2)
	assert.NoError(t, err)
}
