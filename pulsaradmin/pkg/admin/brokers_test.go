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
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"testing"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/auth"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
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
	err = admin.Brokers().HealthCheckWithTopicVersion(utils.TopicVersionV1)
	assert.NoError(t, err)
	err = admin.Brokers().HealthCheckWithTopicVersion(utils.TopicVersionV2)
	assert.NoError(t, err)
}

func TestGetLeaderBroker(t *testing.T) {
	readFile, err := os.ReadFile("../../../integration-tests/tokens/admin-token")
	assert.NoError(t, err)
	cfg := &config.Config{
		Token: string(readFile),
	}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)
	leaderBroker, err := admin.Brokers().GetLeaderBroker()
	assert.NoError(t, err)
	assert.NotNil(t, leaderBroker)
	assert.NotEmpty(t, leaderBroker.ServiceURL)
	assert.NotEmpty(t, leaderBroker.BrokerID)
}

func TestGetAllActiveBrokers(t *testing.T) {
	readFile, err := os.ReadFile("../../../integration-tests/tokens/admin-token")
	assert.NoError(t, err)
	cfg := &config.Config{
		Token: string(readFile),
	}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	brokers, err := admin.Brokers().GetListActiveBrokers()
	assert.NoError(t, err)
	assert.NotEmpty(t, brokers)
}

func TestUpdateDynamicConfiguration(t *testing.T) {
	readFile, err := os.ReadFile("../../../integration-tests/tokens/admin-token")
	assert.NoError(t, err)
	cfg := &config.Config{
		Token: string(readFile),
	}
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	err = admin.Brokers().UpdateDynamicConfiguration("allowAutoSubscriptionCreation", "true")
	assert.NoError(t, err)

	configurations, err := admin.Brokers().GetDynamicConfigurationNames()
	assert.NoError(t, err)
	assert.NotEmpty(t, configurations)
}

func TestUpdateDynamicConfigurationWithCustomURL(t *testing.T) {
	readFile, err := os.ReadFile("../../../integration-tests/tokens/admin-token")
	assert.NoError(t, err)
	cfg := &config.Config{
		WebServiceURL: DefaultWebServiceURL,
		Token:         string(readFile),
	}

	authProvider, err := auth.GetAuthProvider(cfg)
	assert.NoError(t, err)

	client := rest.Client{
		ServiceURL:  cfg.WebServiceURL,
		VersionInfo: ReleaseVersion,
		HTTPClient: &http.Client{
			Timeout:   DefaultHTTPTimeOutDuration,
			Transport: authProvider,
		},
	}
	u, err := url.Parse(cfg.WebServiceURL)
	assert.NoError(t, err)

	// example config value with '/'
	value := `{"key/123":"https://example.com/"}`
	encoded := url.QueryEscape(value)

	resp, err := client.MakeRequestWithURL(http.MethodPost, &url.URL{
		Scheme: u.Scheme,
		User:   u.User,
		Host:   u.Host,
		// use this config to test, will restore it later
		Path:    "/admin/v2/brokers/configuration/allowAutoSubscriptionCreation/" + value,
		RawPath: "/admin/v2/brokers/configuration/allowAutoSubscriptionCreation/" + encoded,
	})
	assert.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// get the config, check if it's updated
	admin, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, admin)

	configurations, err := admin.Brokers().GetAllDynamicConfigurations()
	assert.NoError(t, err)
	assert.NotEmpty(t, configurations)

	var m map[string]interface{}
	err = json.Unmarshal([]byte(configurations["allowAutoSubscriptionCreation"]), &m)
	assert.NoError(t, err)
	assert.Equal(t, "https://example.com/", m["key/123"])

	// restore the config
	err = admin.Brokers().UpdateDynamicConfiguration("allowAutoSubscriptionCreation", "true")
	assert.NoError(t, err)
}
