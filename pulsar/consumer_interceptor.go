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

type ConsumerInterceptor interface {
	// BeforeConsume This is called just before the message is send to Consumer's ConsumerMessage channel.
	BeforeConsume(message ConsumerMessage)

	// OnAcknowledge This is called consumer sends the acknowledgment to the broker.
	OnAcknowledge(consumer Consumer, msgID MessageID)

	// OnNegativeAcksSend This method will be called when a redelivery from a negative acknowledge occurs.
	OnNegativeAcksSend(consumer Consumer, msgIDs []MessageID)
}

// ConsumerCloseInterceptor is an optional interface that a ConsumerInterceptor
// may also implement to be notified once the consumer has been closed. The hook
// fires exactly once per consumer. When err is nil, the close was initiated by
// the user; a non-nil err carries the cause that triggered an internal close
// (for example, exhausting MaxReconnectToBroker or a non-retriable broker error).
type ConsumerCloseInterceptor interface {
	OnConsumerClose(consumer Consumer, err error)
}

type ConsumerInterceptors []ConsumerInterceptor

func (x ConsumerInterceptors) BeforeConsume(message ConsumerMessage) {
	for i := range x {
		x[i].BeforeConsume(message)
	}
}

func (x ConsumerInterceptors) OnAcknowledge(consumer Consumer, msgID MessageID) {
	for i := range x {
		x[i].OnAcknowledge(consumer, msgID)
	}
}

func (x ConsumerInterceptors) OnNegativeAcksSend(consumer Consumer, msgIDs []MessageID) {
	for i := range x {
		x[i].OnNegativeAcksSend(consumer, msgIDs)
	}
}

func (x ConsumerInterceptors) OnConsumerClose(consumer Consumer, err error) {
	for i := range x {
		if c, ok := x[i].(ConsumerCloseInterceptor); ok {
			c.OnConsumerClose(consumer, err)
		}
	}
}

var defaultConsumerInterceptors = make(ConsumerInterceptors, 0)
