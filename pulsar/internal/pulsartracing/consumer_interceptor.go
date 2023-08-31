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

package pulsartracing

import (
	"context"

	"github.com/opentracing/opentracing-go"

	"github.com/apache/pulsar-client-go/pulsar"
)

const fromPrefix = "From__"

type ConsumerInterceptor struct {
}

func (t *ConsumerInterceptor) BeforeConsume(message pulsar.ConsumerMessage) {
	buildAndInjectChildSpan(message).Finish()
}

func (t *ConsumerInterceptor) OnAcknowledge(consumer pulsar.Consumer, msgID pulsar.MessageID) {}

func (t *ConsumerInterceptor) OnNegativeAcksSend(consumer pulsar.Consumer, msgIDs []pulsar.MessageID) {
}

func buildAndInjectChildSpan(message pulsar.ConsumerMessage) opentracing.Span {
	tracer := opentracing.GlobalTracer()
	parentContext := ExtractSpanContextFromConsumerMessage(message)

	var span opentracing.Span

	var startSpanOptions []opentracing.StartSpanOption
	if parentContext != nil {
		startSpanOptions = []opentracing.StartSpanOption{opentracing.FollowsFrom(parentContext)}
	}

	span = tracer.StartSpan(fromPrefix+message.Topic()+"__"+message.Subscription(), startSpanOptions...)

	enrichConsumerSpan(&message, span)
	InjectConsumerMessageSpanContext(opentracing.ContextWithSpan(context.Background(), span), message)

	return span
}
