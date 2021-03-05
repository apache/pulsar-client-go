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

package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInvalidServiceUris(t *testing.T) {
	uris := []string{
		"://localhost:6650",                // missing scheme
		"pulsar:///",                       // missing authority
		"pulsar://localhost:6650:6651/",    // invalid hostname pair
		"pulsar://localhost:xyz/",          // invalid port
		"pulsar://localhost:-6650/",        // negative port
		"pulsar://fec0:0:0:ffff::1:6650",   // missing brackets
	}

	for _, uri := range uris {
		testInvalidServiceUri(t, uri)
	}
}

func TestEmptyServiceUriString(t *testing.T) {
	u, err := NewPulsarServiceURIFromUriString("")
	assert.Nil(t, u)
	assert.NotNil(t, err)
}

func TestNullServiceUrlInstance(t *testing.T) {
	u, err := NewPulsarServiceURIFromUrl(nil)
	assert.Nil(t, u)
	assert.NotNil(t, err)
}

func TestMissingServiceName(t *testing.T) {
	serviceUri := "//localhost:6650/path/to/namespace"
	assertServiceUri(t, serviceUri, "", nil, []string{"localhost:6650"}, "/path/to/namespace", "")
}

func TestEmptyPath(t *testing.T) {
	serviceUri := "pulsar://localhost:6650"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"localhost:6650"}, "", "")
}

func TestRootPath(t *testing.T) {
	serviceUri := "pulsar://localhost:6650/"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"localhost:6650"}, "/", "")
}

func TestUserInfo(t *testing.T) {
	serviceUri := "pulsar://pulsaruser@localhost:6650/path/to/namespace"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"localhost:6650"}, "/path/to/namespace", "pulsaruser")
}

func TestIpv6Uri(t *testing.T) {
	serviceUri := "pulsar://pulsaruser@[fec0:0:0:ffff::1]:6650/path/to/namespace"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"[fec0:0:0:ffff::1]:6650"}, "/path/to/namespace", "pulsaruser")
}

func TestIpv6UriWithoutPulsarPort(t *testing.T) {
	serviceUri := "pulsar://[fec0:0:0:ffff::1]/path/to/namespace"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"[fec0:0:0:ffff::1]:6650"}, "/path/to/namespace", "")
}

func TestMultiIpv6Uri(t *testing.T) {
	serviceUri := "pulsar://pulsaruser@[fec0:0:0:ffff::1]:6650,[fec0:0:0:ffff::2]:6650;[fec0:0:0:ffff::3]:6650/path/to/namespace"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"[fec0:0:0:ffff::1]:6650", "[fec0:0:0:ffff::2]:6650", "[fec0:0:0:ffff::3]:6650"}, "/path/to/namespace", "pulsaruser")
}

func TestMultiIpv6UriWithoutPulsarPort(t *testing.T) {
	serviceUri := "pulsar://pulsaruser@[fec0:0:0:ffff::1],[fec0:0:0:ffff::2];[fec0:0:0:ffff::3]/path/to/namespace"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"[fec0:0:0:ffff::1]:6650", "[fec0:0:0:ffff::2]:6650", "[fec0:0:0:ffff::3]:6650"}, "/path/to/namespace", "pulsaruser")
}

func TestMultipleHostsSemiColon(t *testing.T) {
	serviceUri := "pulsar://host1:6650;host2:6650;host3:6650/path/to/namespace"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"host1:6650", "host2:6650", "host3:6650"}, "/path/to/namespace", "")
}

func TestMultipleHostsComma(t *testing.T) {
	serviceUri := "pulsar://host1:6650,host2:6650,host3:6650/path/to/namespace"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"host1:6650", "host2:6650", "host3:6650"}, "/path/to/namespace", "")
}

func TestMultipleHostsWithoutPulsarPorts(t *testing.T) {
	serviceUri := "pulsar://host1,host2,host3/path/to/namespace"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"host1:6650", "host2:6650", "host3:6650"}, "/path/to/namespace", "")
}

func TestMultipleHostsWithoutPulsarTlsPorts(t *testing.T) {
	serviceUri := "pulsar+ssl://host1,host2,host3/path/to/namespace"
	assertServiceUri(t, serviceUri, "pulsar", []string{"ssl"}, []string{"host1:6651", "host2:6651", "host3:6651"}, "/path/to/namespace", "")
}

func TestMultipleHostsWithoutHttpPorts(t *testing.T) {
	serviceUri := "http://host1,host2,host3/path/to/namespace"
	assertServiceUri(t, serviceUri, "http", nil, []string{"host1:80", "host2:80", "host3:80"}, "/path/to/namespace", "")
}

func TestMultipleHostsWithoutHttpsPorts(t *testing.T) {
	serviceUri := "https://host1,host2,host3/path/to/namespace"
	assertServiceUri(t, serviceUri, "https", nil, []string{"host1:443", "host2:443", "host3:443"}, "/path/to/namespace", "")
}

func TestMultipleHostsMixedPorts(t *testing.T) {
	serviceUri := "pulsar://host1:6640,host2:6650,host3:6660/path/to/namespace"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"host1:6640", "host2:6650", "host3:6660"}, "/path/to/namespace", "")
}

func TestMultipleHostsMixed(t *testing.T) {
	serviceUri := "pulsar://host1:6640,host2,host3:6660/path/to/namespace"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"host1:6640", "host2:6650", "host3:6660"}, "/path/to/namespace", "")
}

func TestUserInfoWithMultipleHosts(t *testing.T) {
	serviceUri := "pulsar://pulsaruser@host1:6650;host2:6650;host3:6650/path/to/namespace"
	assertServiceUri(t, serviceUri, "pulsar", nil, []string{"host1:6650", "host2:6650", "host3:6650"}, "/path/to/namespace", "pulsaruser")
}

func testInvalidServiceUri(t *testing.T, serviceUri string) {
	u, err := NewPulsarServiceURIFromUriString(serviceUri)
	t.Logf("testInvalidServiceUri %s", serviceUri)
	assert.Nil(t, u)
	assert.NotNil(t, err)
}

func assertServiceUri(t *testing.T, serviceUri, expectedServiceName string,
	expectedServiceInfo, expectedServiceHosts []string, expectedServicePath, expectedServiceUser string) {
	uri, err := NewPulsarServiceURIFromUriString(serviceUri)
	assert.Nil(t, err)
	assert.NotNil(t, serviceUri)
	assert.Equal(t, expectedServiceName, uri.ServiceName)
	assert.Equal(t, expectedServicePath, uri.servicePath)
	if expectedServiceUser != "" {
		assert.Equal(t, expectedServiceUser, uri.URL.User.Username())
	}
	assert.ElementsMatch(t, expectedServiceInfo, uri.ServiceInfos)
	assert.ElementsMatch(t, expectedServiceHosts, uri.ServiceHosts)
}