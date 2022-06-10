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

	"github.com/skulkarni-ns/pulsar-client-go/pulsar"
	"github.com/opentracing/opentracing-go"
)

const toPrefix = "To__"

type ProducerInterceptor struct {
}

func (t *ProducerInterceptor) BeforeSend(producer pulsar.Producer, message *pulsar.ProducerMessage) {
	buildAndInjectSpan(message, producer).Finish()
}

func (t *ProducerInterceptor) OnSendAcknowledgement(producer pulsar.Producer,
	message *pulsar.ProducerMessage,
	msgID pulsar.MessageID) {
}

func buildAndInjectSpan(message *pulsar.ProducerMessage, producer pulsar.Producer) opentracing.Span {
	tracer := opentracing.GlobalTracer()
	spanContext := ExtractSpanContextFromProducerMessage(message)

	var span opentracing.Span

	var startSpanOptions []opentracing.StartSpanOption
	if spanContext != nil {
		startSpanOptions = []opentracing.StartSpanOption{opentracing.FollowsFrom(spanContext)}
	}

	span = tracer.StartSpan(toPrefix+producer.Topic(), startSpanOptions...)

	enrichProducerSpan(message, producer, span)

	InjectProducerMessageSpanContext(opentracing.ContextWithSpan(context.Background(), span), message)

	return span
}
