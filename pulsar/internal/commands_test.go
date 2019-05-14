//
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
//

package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConvertStringMap(t *testing.T) {
	m := make(map[string]string)
	m["a"] = "1"
	m["b"] = "2"

	pbm := ConvertFromStringMap(m)

	assert.Equal(t, 2, len(pbm))
	assert.Equal(t, "a", *pbm[0].Key)
	assert.Equal(t, "1", *pbm[0].Value)
	assert.Equal(t, "b", *pbm[1].Key)
	assert.Equal(t, "2", *pbm[1].Value)

	m2 := ConvertToStringMap(pbm)
	assert.Equal(t, 2, len(m2))
	assert.Equal(t, "1", m2["a"])
	assert.Equal(t, "2", m2["b"])
}

