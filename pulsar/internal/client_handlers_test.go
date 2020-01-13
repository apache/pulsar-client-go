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

	closable := &testClosable{h: &h, closed:false}
	h.Add(closable)
	assert.True(t, h.Val(closable))

	h.Close()
	t.Log("closable is: ", closable.closed)
	assert.True(t, closable.closed)
}

func TestClientHandlers_Del(t *testing.T) {
	h := NewClientHandlers()
	assert.NotNil(t, h.l)
	assert.Equal(t, h.handlers, map[Closable]bool{})

	closable1 := &testClosable{h: &h, closed:false}
	h.Add(closable1)

	closable2 := &testClosable{h: &h, closed:false}
	h.Add(closable2)

	assert.Len(t, h.handlers, 2)
	assert.True(t, h.Val(closable1))
	assert.True(t, h.Val(closable2))

	closable1.Close()
	assert.False(t, h.Val(closable1))
	assert.True(t, h.Val(closable2))
	assert.Len(t, h.handlers, 1)

	h.Close()
	t.Log("closable1 is: closed ", closable1.closed)
	t.Log("closable2 is: closed ", closable2.closed)
	assert.True(t, closable1.closed)
	assert.True(t, closable2.closed)
	assert.Len(t, h.handlers, 0)
}

type testClosable struct {
	h *ClientHandlers
	closed bool
}

func (t *testClosable) Close() {
	t.closed = true
	t.h.Del(t)
}
