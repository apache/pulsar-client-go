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

	"github.com/streamnative/pulsar-admin-go/pkg/pulsar/common"

	"github.com/stretchr/testify/assert"
)

func TestPulsarClientEndpointEscapes(t *testing.T) {
	client := pulsarClient{Client: nil, APIVersion: common.V2}
	actual := client.endpoint("/myendpoint", "abc%? /def", "ghi")
	expected := "/admin/v2/myendpoint/abc%25%3F%20%2Fdef/ghi"
	assert.Equal(t, expected, actual)
}
