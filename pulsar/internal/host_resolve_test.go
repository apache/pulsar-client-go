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
)

func TestHostResolve_GetHost(t *testing.T) {

	var (
		h           HostResolve
		err         error
		urlString   string
		resolveAddr *url.URL
	)

	// test single host
	h, err = NewHostResolve("pulsar://localhost:6650")
	assert.Nil(t, err)
	urlString = h.GetServiceURL()
	assert.Equal(t, urlString, "pulsar://localhost:6650")
	resolveAddr, err = h.GetHost()
	assert.Nil(t, err)
	assert.Equal(t, resolveAddr.String(), "pulsar://localhost:6650")

	// test multi host
	h, err = NewHostResolve("pulsar://localhost:6650,pulsar://example:6650")
	assert.Nil(t, err)
	urlString = h.GetServiceURL()
	assert.Equal(t, urlString, "pulsar://localhost:6650,pulsar://example:6650")
	resolveAddr, err = h.GetHost()
	assert.Nil(t, err)
	assert.Equal(t, resolveAddr.Scheme, "pulsar")

	h, err = NewHostResolve("pulsar+ssl://localhost:6650,example:6650")
	assert.Nil(t, err)
	urlString = h.GetServiceURL()
	assert.Equal(t, urlString, "pulsar+ssl://localhost:6650,example:6650")
	resolveAddr, err = h.GetHost()
	assert.Nil(t, err)
	assert.Equal(t, resolveAddr.Scheme, "pulsar+ssl")

	// error test
	_, err = NewHostResolve("pul://localhost:6650")
	assert.NotNil(t, err)

}

func TestHostResolve_ResolveHost(t *testing.T) {

	var (
		h            HostResolve
		err          error
		resolveAddr  *url.URL
		resolveAddr2 *url.URL
	)

	h, err = NewHostResolve("pulsar://localhost:6650")
	assert.Nil(t, err)
	resolveAddr, err = h.ResolveHost()
	assert.Nil(t, err)
	assert.Equal(t, resolveAddr.String(), "pulsar://localhost:6650")

	h, err = NewHostResolve("pulsar://localhost:6650,example:6650")
	assert.Nil(t, err)
	resolveAddr, err = h.ResolveHost()
	assert.Nil(t, err)
	resolveAddr2, err = h.ResolveHost()
	assert.Nil(t, err)
	assert.NotEqual(t, resolveAddr.String(), resolveAddr2.String())
	t.Logf("addr1: %s, addr2: %s", resolveAddr.String(), resolveAddr2.String())

}
