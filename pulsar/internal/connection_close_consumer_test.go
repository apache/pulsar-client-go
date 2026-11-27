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

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// A consumer registers the handler of its new subscription from
// ConnectionClosed, on the consumer event loop. The handler of the closed
// subscription has to be gone by then: deleting it afterwards could remove the
// new one and leave the consumer without a handler.
type reconnectingHandler struct {
	onClose func()
}

func (c *reconnectingHandler) MessageReceived(_ *pb.CommandMessage, _ Buffer) error {
	return nil
}

func (c *reconnectingHandler) ActiveConsumerChanged(_ bool) {}

func (c *reconnectingHandler) SetRedirectedClusterURI(_ string) {}

func (c *reconnectingHandler) ConnectionClosed(_ Connection, _ *pb.CommandCloseConsumer) {
	c.onClose()
}

func TestHandleCloseConsumerRemovesHandlerBeforeNotifying(t *testing.T) {
	c := newTestConnection()
	const id = 7
	var next *reconnectingHandler
	// onClose stands in for the event loop: it re-registers the handler, as
	// grabConn does, while handleCloseConsumer is still on the stack.
	closed := &reconnectingHandler{onClose: func() {
		_, stillRegistered := c.consumerHandler(id)
		assert.False(t, stillRegistered, "the closed handler must be removed before ConnectionClosed")
		next = &reconnectingHandler{}
		require.NoError(t, c.AddConsumeHandler(id, next))
	}}
	require.NoError(t, c.AddConsumeHandler(id, closed))

	c.handleCloseConsumer(&pb.CommandCloseConsumer{ConsumerId: proto.Uint64(id)})

	registered, ok := c.consumerHandler(id)
	require.True(t, ok, "the handler registered during ConnectionClosed must survive")
	assert.Same(t, next, registered)
}
