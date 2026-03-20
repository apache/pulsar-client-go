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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvalidServiceUris(t *testing.T) {
	uris := []string{
		"://localhost:6650",              // missing scheme
		"pulsar:///",                     // missing authority
		"pulsar://localhost:6650:6651/",  // invalid hostname pair
		"pulsar://localhost:xyz/",        // invalid port
		"pulsar://localhost:-6650/",      // negative port
		"pulsar://fec0:0:0:ffff::1:6650", // missing brackets
		"pulsar://[example]:6650",        // invalid hostname
		"pulsar://fec0::1",               // missing brackets
	}

	for _, uri := range uris {
		testInvalidServiceURI(t, uri)
	}
}

func TestEmptyServiceUriString(t *testing.T) {
	u, err := NewPulsarServiceURIFromURIString("")
	assert.Nil(t, u)
	assert.NotNil(t, err)
}

func TestMissingServiceName(t *testing.T) {
	serviceURI := "//localhost:6650/path/to/namespace"
	_, err := NewPulsarServiceURIFromURIString(serviceURI)
	require.Error(t, err)
}

func TestUnsupportedServiceNameError(t *testing.T) {
	_, err := NewPulsarServiceURIFromURIString("ftp://localhost:21")
	require.Error(t, err)

	var unsupportedServiceNameErr *UnsupportedServiceNameError
	require.ErrorAs(t, err, &unsupportedServiceNameErr)
	assert.Equal(t, "ftp", unsupportedServiceNameErr.ServiceName)
	assert.True(t, errors.As(err, &unsupportedServiceNameErr))
}

func TestIsHTTP(t *testing.T) {
	testCases := []struct {
		name     string
		uri      string
		expected bool
	}{
		{name: "pulsar", uri: "pulsar://localhost:6650", expected: false},
		{name: "pulsar ssl", uri: "pulsar+ssl://localhost:6651", expected: false},
		{name: "http", uri: "http://localhost", expected: true},
		{name: "https", uri: "https://localhost", expected: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			serviceURI, err := NewPulsarServiceURIFromURIString(tc.uri)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, serviceURI.IsHTTP())
		})
	}
}

func TestEmptyPath(t *testing.T) {
	serviceURI := "pulsar://localhost:6650"
	assertServiceURI(t, serviceURI, "pulsar", nil, []string{"localhost:6650"}, "", "")
}

func TestRootPath(t *testing.T) {
	serviceURI := "pulsar://localhost:6650/"
	assertServiceURI(t, serviceURI, "pulsar", nil, []string{"localhost:6650"}, "/", "")
}

func TestUserInfo(t *testing.T) {
	serviceURI := "pulsar://pulsaruser@localhost:6650/path/to/namespace"
	assertServiceURI(t, serviceURI, "pulsar", nil, []string{"localhost:6650"}, "/path/to/namespace", "pulsaruser")
}

func TestIpv6Uri(t *testing.T) {
	serviceURI := "pulsar://pulsaruser@[fec0:0:0:ffff::1]:6650/path/to/namespace"
	assertServiceURI(t, serviceURI, "pulsar", nil, []string{"[fec0:0:0:ffff::1]:6650"}, "/path/to/namespace",
		"pulsaruser")
}

func TestIpv6UriWithoutPulsarPort(t *testing.T) {
	serviceURI := "pulsar://[fec0:0:0:ffff::1]/path/to/namespace"
	assertServiceURI(t, serviceURI, "pulsar", nil, []string{"[fec0:0:0:ffff::1]:6650"}, "/path/to/namespace", "")
}

func TestMultiIpv6Uri(t *testing.T) {
	serviceURI := "pulsar://pulsaruser@[fec0:0:0:ffff::1]:6650,[fec0:0:0:ffff::2]:6650;[fec0:0:0:ffff::3]:6650" +
		"/path/to/namespace"
	assertServiceURI(t, serviceURI, "pulsar", nil,
		[]string{"[fec0:0:0:ffff::1]:6650", "[fec0:0:0:ffff::2]:6650", "[fec0:0:0:ffff::3]:6650"}, "/path/to/namespace",
		"pulsaruser")
}

func TestMultiIpv6UriWithoutPulsarPort(t *testing.T) {
	serviceURI := "pulsar://pulsaruser@[fec0:0:0:ffff::1],[fec0:0:0:ffff::2];[fec0:0:0:ffff::3]/path/to/namespace"
	assertServiceURI(t, serviceURI, "pulsar", nil,
		[]string{"[fec0:0:0:ffff::1]:6650", "[fec0:0:0:ffff::2]:6650", "[fec0:0:0:ffff::3]:6650"}, "/path/to/namespace",
		"pulsaruser")
}

func TestMultipleHostsSemiColon(t *testing.T) {
	serviceURI := "pulsar://host1:6650;host2:6650;host3:6650/path/to/namespace"
	assertServiceURI(t, serviceURI, "pulsar", nil, []string{"host1:6650", "host2:6650", "host3:6650"},
		"/path/to/namespace", "")
}

func TestMultipleHostsComma(t *testing.T) {
	serviceURI := "pulsar://host1:6650,host2:6650,host3:6650/path/to/namespace"
	assertServiceURI(t, serviceURI, "pulsar", nil, []string{"host1:6650", "host2:6650", "host3:6650"},
		"/path/to/namespace", "")
}

func TestMultipleHostsWithoutPulsarPorts(t *testing.T) {
	serviceURI := "pulsar://host1,host2,host3/path/to/namespace"
	assertServiceURI(t, serviceURI, "pulsar", nil, []string{"host1:6650", "host2:6650", "host3:6650"},
		"/path/to/namespace", "")
}

func TestMultipleHostsWithoutPulsarTlsPorts(t *testing.T) {
	serviceURI := "pulsar+ssl://host1,host2,host3/path/to/namespace"
	assertServiceURI(t, serviceURI, "pulsar", []string{"ssl"}, []string{"host1:6651", "host2:6651", "host3:6651"},
		"/path/to/namespace", "")
}

func TestMultipleHostsWithoutHttpPorts(t *testing.T) {
	serviceURI := "http://host1,host2,host3/path/to/namespace"
	assertServiceURI(t, serviceURI, "http", nil, []string{"host1:80", "host2:80", "host3:80"}, "/path/to/namespace", "")
}

func TestMultipleHostsWithoutHttpsPorts(t *testing.T) {
	serviceURI := "https://host1,host2,host3/path/to/namespace"
	assertServiceURI(t, serviceURI, "https", nil, []string{"host1:443", "host2:443", "host3:443"}, "/path/to/namespace",
		"")
}

func TestMultipleHostsMixedPorts(t *testing.T) {
	serviceURI := "pulsar://host1:6640,host2:6650,host3:6660/path/to/namespace"
	assertServiceURI(t, serviceURI, "pulsar", nil, []string{"host1:6640", "host2:6650", "host3:6660"},
		"/path/to/namespace", "")
}

func TestMultipleHostsMixed(t *testing.T) {
	serviceURI := "pulsar://host1:6640,host2,host3:6660/path/to/namespace"
	assertServiceURI(t, serviceURI, "pulsar", nil, []string{"host1:6640", "host2:6650", "host3:6660"},
		"/path/to/namespace", "")
}

func TestPathQueryAndFragmentDelimitersDoNotSplitHosts(t *testing.T) {
	serviceURI := "pulsar://host1:6650/path,with;delimiters?param=a,b;c#frag,ment;tail"
	uri, err := NewPulsarServiceURIFromURIString(serviceURI)
	require.NoError(t, err)
	require.NotNil(t, uri)
	assert.Equal(t, []string{"host1:6650"}, uri.ServiceHosts)
	assert.Equal(t, "/path,with;delimiters", uri.servicePath)
	assert.Equal(t, "param=a,b;c", uri.URL.RawQuery)
	assert.Equal(t, "frag,ment;tail", uri.URL.Fragment)
}

func TestInvalidBracketedAdditionalHost(t *testing.T) {
	testInvalidServiceURI(t, "pulsar://host1:6650,[example]:6650/path/to/namespace")
}

func TestUserInfoWithMultipleHosts(t *testing.T) {
	serviceURI := "pulsar://pulsaruser@host1:6650;host2:6650;host3:6650/path/to/namespace"
	assertServiceURI(t, serviceURI, "pulsar", nil, []string{"host1:6650", "host2:6650", "host3:6650"},
		"/path/to/namespace", "pulsaruser")
}

func testInvalidServiceURI(t *testing.T, serviceURI string) {
	u, err := NewPulsarServiceURIFromURIString(serviceURI)
	t.Logf("testInvalidServiceURI %s", serviceURI)
	assert.Nil(t, u)
	assert.NotNil(t, err)
}

func assertServiceURI(t *testing.T, serviceURI, expectedServiceName string,
	expectedServiceInfo, expectedServiceHosts []string, expectedServicePath, expectedServiceUser string) {
	uri, err := NewPulsarServiceURIFromURIString(serviceURI)
	assert.Nil(t, err)
	assert.NotNil(t, serviceURI)
	assert.Equal(t, expectedServiceName, uri.ServiceName)
	assert.Equal(t, expectedServicePath, uri.servicePath)
	if expectedServiceUser != "" {
		assert.Equal(t, expectedServiceUser, uri.URL.User.Username())
	}
	assert.ElementsMatch(t, expectedServiceInfo, uri.ServiceInfos)
	assert.ElementsMatch(t, expectedServiceHosts, uri.ServiceHosts)
}
