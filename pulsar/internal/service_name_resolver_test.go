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
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveBeforeUpdateServiceUrl(t *testing.T) {
	resolver, err := NewPulsarServiceNameResolver("")
	require.NoError(t, err)
	u, err := resolver.ResolveHost()
	assert.Nil(t, u)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "no service url is provided yet")
}

func TestUpdateInvalidServiceUrl(t *testing.T) {
	resolver, err := NewPulsarServiceNameResolver("")
	require.NoError(t, err)
	err = resolver.UpdateServiceURL("pulsar:///")
	assert.NotNil(t, err)
	assert.Nil(t, resolver.GetServiceURI())
}

func TestSimpleHostUrl(t *testing.T) {
	resolver, err := NewPulsarServiceNameResolver("")
	require.NoError(t, err)
	serviceURL := "pulsar://host1:6650"
	err = resolver.UpdateServiceURL(serviceURL)
	assert.Nil(t, err)
	expectedURI, err := NewPulsarServiceURIFromURIString(serviceURL)
	assert.Nil(t, err)
	assert.Equal(t, expectedURI, resolver.GetServiceURI())
	actualHost, err := resolver.ResolveHost()
	assert.Nil(t, err)
	assert.Equal(t, "host1", actualHost.Hostname())
	assert.Equal(t, "6650", actualHost.Port())

	newServiceURL := "pulsar://host2:6650"
	err = resolver.UpdateServiceURL(newServiceURL)
	assert.Nil(t, err)
	expectedURI, err = NewPulsarServiceURIFromURIString(newServiceURL)
	assert.Nil(t, err)
	assert.Equal(t, expectedURI, resolver.GetServiceURI())
	actualHost, err = resolver.ResolveHost()
	assert.Nil(t, err)
	assert.Equal(t, "host2", actualHost.Hostname())
	assert.Equal(t, "6650", actualHost.Port())
}

func TestMultipleHostsUrl(t *testing.T) {
	resolver, err := NewPulsarServiceNameResolver("")
	require.NoError(t, err)
	serviceURL := "pulsar://host1:6650,host2:6650"
	err = resolver.UpdateServiceURL(serviceURL)
	assert.Nil(t, err)
	expectedURI, err := NewPulsarServiceURIFromURIString(serviceURL)
	assert.Nil(t, err)
	assert.Equal(t, expectedURI, resolver.GetServiceURI())
	host1, _ := url.Parse("pulsar://host1:6650")
	host2, _ := url.Parse("pulsar://host2:6650")
	assert.Contains(t, resolver.GetAddressList(), host1)
	assert.Contains(t, resolver.GetAddressList(), host2)
	hosts := []*url.URL{host1, host2}
	for i := 0; i < 10; i++ {
		host, err := resolver.ResolveHost()
		assert.Nil(t, err)
		assert.Contains(t, hosts, host)
	}
}

func TestMultipleHostsTlsUrl(t *testing.T) {
	resolver, err := NewPulsarServiceNameResolver("")
	require.NoError(t, err)
	serviceURL := "pulsar+ssl://host1:6651,host2:6651"
	err = resolver.UpdateServiceURL(serviceURL)
	assert.Nil(t, err)
	expectedURI, err := NewPulsarServiceURIFromURIString(serviceURL)
	assert.Nil(t, err)
	assert.Equal(t, expectedURI, resolver.GetServiceURI())
	host1, _ := url.Parse("pulsar+ssl://host1:6651")
	host2, _ := url.Parse("pulsar+ssl://host2:6651")
	assert.Contains(t, resolver.GetAddressList(), host1)
	assert.Contains(t, resolver.GetAddressList(), host2)
	hosts := []*url.URL{host1, host2}
	for i := 0; i < 10; i++ {
		host, err := resolver.ResolveHost()
		assert.Nil(t, err)
		assert.Contains(t, hosts, host)
	}
}

func TestResolveIpv6Host(t *testing.T) {
	resolver, err := NewPulsarServiceNameResolver("")
	require.NoError(t, err)

	serviceURL := "pulsar://[fec0:0:0:ffff::1]:6650"
	err = resolver.UpdateServiceURL(serviceURL)
	require.NoError(t, err)

	actualHost, err := resolver.ResolveHost()
	require.NoError(t, err)
	assert.Equal(t, "pulsar", actualHost.Scheme)
	assert.Equal(t, "fec0:0:0:ffff::1", actualHost.Hostname())
	assert.Equal(t, "6650", actualHost.Port())
	assert.Equal(t, "[fec0:0:0:ffff::1]:6650", actualHost.Host)
}
