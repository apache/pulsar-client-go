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

import "context"

type ProducerInterceptor interface {
	// BeforeSend This is called before send the message to the brokers. This method is allowed to modify the
	// message.
	BeforeSend(ctx context.Context, producer Producer, message *ProducerMessage) context.Context

	// OnSendAcknowledgement This method is called when the message sent to the broker has been acknowledged,
	// or when sending the message fails.
	OnSendAcknowledgement(ctx context.Context, producer Producer, message *ProducerMessage, msgID MessageID)
}

type ProducerInterceptors []ProducerInterceptor

func (x ProducerInterceptors) BeforeSend(
	ctx context.Context,
	producer Producer,
	message *ProducerMessage,
) context.Context {
	for i := range x {
		ctx = x[i].BeforeSend(ctx, producer, message)
	}
	return ctx
}

func (x ProducerInterceptors) OnSendAcknowledgement(
	ctx context.Context,
	producer Producer,
	message *ProducerMessage,
	msgID MessageID,
) {
	for i := range x {
		x[i].OnSendAcknowledgement(ctx, producer, message, msgID)
	}
}

var defaultProducerInterceptors = make(ProducerInterceptors, 0)
