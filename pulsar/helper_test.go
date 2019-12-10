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

func TestToKeyValues(t *testing.T) {
	meta := map[string]string{
		"key1": "value1",
	}

	kvs := toKeyValues(meta)
	assert.Equal(t, 1, len(kvs))
	assert.Equal(t, "key1", *kvs[0].Key)
	assert.Equal(t, "value1", *kvs[0].Value)

	meta = map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	kvs = toKeyValues(meta)
	assert.Equal(t, 2, len(kvs))
	for _, kv := range kvs {
		v := meta[*kv.Key]
		assert.Equal(t, v, *kv.Value)
	}
}
