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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertStringMap(t *testing.T) {
	m := make(map[string]string)
	m["a"] = "1"
	m["b"] = "2"

	pbm := ConvertFromStringMap(m)

	assert.Equal(t, 2, len(pbm))

	m2 := ConvertToStringMap(pbm)
	assert.Equal(t, 2, len(m2))
	assert.Equal(t, "1", m2["a"])
	assert.Equal(t, "2", m2["b"])
}

func TestDecodeBatchPayload(t *testing.T) {
	payload := []byte{0, 0, 0, 2, 24, 12, 104, 101, 108, 108, 111, 45, 112, 117, 108, 115, 97, 114} // hello-pulsar
	list, err := decodeBatchPayload(payload, 1)
	if err != nil {
		t.Fatal(err)
	}
	if get, want := len(list), 1; get != want {
		t.Errorf("want %v, but get %v", get, want)
	}

	m := list[0]
	if get, want := string(m.SinglePayload), "hello-pulsar"; get != want {
		t.Errorf("want %v, but get %v", get, want)
	}
}
