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

func TestClientHandlers(t *testing.T) {
	h := NewClientHandlers()
	assert.NotNil(t, h.l)
	assert.Equal(t, h.handlers, map[Closable]bool{})

	closable := &testClosable{false}
	h.Add(closable)
	assert.True(t, h.Val(closable))

	h.Close()
	t.Log("closable is: ", closable.closed)
	assert.True(t, closable.closed)
}

type testClosable struct {
	closed bool
}

func (t *testClosable) Close() {
	t.closed = true
}
