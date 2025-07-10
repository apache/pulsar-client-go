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

package tracing

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const fromPrefix = "From__"

type ConsumerInterceptor struct{}

func (t *ConsumerInterceptor) BeforeConsume(message pulsar.ConsumerMessage) {
	ctx := ExtractSpanContextFromConsumerMessage(context.Background(), message)
	tracer := otel.Tracer(componentName)
	ctx, span := tracer.Start(ctx, fromPrefix+message.Topic()+"__"+message.Subscription(), trace.WithSpanKind(trace.SpanKindConsumer))
	enrichConsumerSpan(&message, span)
	InjectConsumerMessageSpanContext(ctx, message)
	span.End()
}

func (t *ConsumerInterceptor) OnAcknowledge(_ pulsar.Consumer, _ pulsar.MessageID)        {}
func (t *ConsumerInterceptor) OnNegativeAcksSend(_ pulsar.Consumer, _ []pulsar.MessageID) {}
