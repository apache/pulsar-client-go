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

package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNamespaceName(t *testing.T) {
	success, err := GetNamespaceName("public/default")
	assert.Nil(t, err)
	assert.Equal(t, "public/default", success.String())

	empty, err := GetNamespaceName("")
	assert.NotNil(t, err)
	assert.Equal(t, "the namespace complete name is empty", err.Error())
	assert.Nil(t, empty)

	empty, err = GetNamespaceName("/")
	assert.NotNil(t, err)
	assert.Equal(t, "Invalid tenant or namespace. [/]", err.Error())
	assert.Nil(t, empty)

	invalid, err := GetNamespaceName("public/default/fail")
	assert.NotNil(t, err)
	assert.Equal(t, "The complete name of namespace is invalid. complete name : [public/default/fail]", err.Error())
	assert.Nil(t, invalid)

	invalid, err = GetNamespaceName("public")
	assert.NotNil(t, err)
	assert.Equal(t, "The complete name of namespace is invalid. complete name : [public]", err.Error())
	assert.Nil(t, invalid)

	special, err := GetNamespaceName("-=.:/-=.:")
	assert.Nil(t, err)
	assert.Equal(t, "-=.:/-=.:", special.String())

	tenantInvalid, err := GetNamespaceName("\"/namespace")
	assert.NotNil(t, err)
	assert.Equal(t, "Tenant name include unsupported special chars. tenant : [\"]", err.Error())
	assert.Nil(t, tenantInvalid)

	namespaceInvalid, err := GetNamespaceName("tenant/}")
	assert.NotNil(t, err)
	assert.Equal(t, "Namespace name include unsupported special chars. namespace : [}]", err.Error())
	assert.Nil(t, namespaceInvalid)
}
