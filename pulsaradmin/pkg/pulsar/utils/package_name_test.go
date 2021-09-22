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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPackageName(t *testing.T) {
	success, err := GetPackageName("function://f-tenant/f-ns/f-name@f-version")
	assert.Nil(t, err)
	assert.Equal(t, "function://f-tenant/f-ns/f-name@f-version", success.String())

	success, err = GetPackageName("function://f-tenant/f-ns/f-name")
	assert.Nil(t, err)
	assert.Equal(t, "function://f-tenant/f-ns/f-name@latest", success.String())

	success, err = GetPackageName("sink://s-tenant/s-ns/s-name@s-version")
	assert.Nil(t, err)
	assert.Equal(t, "sink://s-tenant/s-ns/s-name@s-version", success.String())

	success, err = GetPackageName("sink://s-tenant/s-ns/s-name")
	assert.Nil(t, err)
	assert.Equal(t, "sink://s-tenant/s-ns/s-name@latest", success.String())

	success, err = GetPackageName("source://s-tenant/s-ns/s-name@s-version")
	assert.Nil(t, err)
	assert.Equal(t, "source://s-tenant/s-ns/s-name@s-version", success.String())

	success, err = GetPackageName("source://s-tenant/s-ns/s-name")
	assert.Nil(t, err)
	assert.Equal(t, "source://s-tenant/s-ns/s-name@latest", success.String())

	fail, err := GetPackageName("function:///public/default/test-error@v1")
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid package name 'function:///public/default/test-error@v1', it should be in the "+
		"format of type://tenant/namespace/name@version", err.Error())
	assert.Nil(t, fail)

	fail, err = GetPackageNameWithComponents("functions", "public", "default", "test-error", "v1")
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid package type 'functions', it should be function, sink, or source", err.Error())
	assert.Nil(t, fail)

	fail, err = GetPackageNameWithComponents("function", "public/default", "default", "test-error", "v1")
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid package name 'function://public/default/default/test-error@v1', it should be in the "+
		"format of type://tenant/namespace/name@version", err.Error())
	assert.Nil(t, fail)

	fail, err = GetPackageName("function://public/default/test-error-version/v2")
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid package name 'function://public/default/test-error-version/v2', it should be in the "+
		"format of type://tenant/namespace/name@version", err.Error())
	assert.Nil(t, fail)
}
