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
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
)

func InjectProducerMessageSpanContext(ctx context.Context, message *pulsar.ProducerMessage) {
	injectAdapter := &ProducerMessageInjectAdapter{message}

	span := opentracing.SpanFromContext(ctx)

	for k, v := range message.Properties {
		span.SetTag(k, v)
	}

	err := opentracing.GlobalTracer().Inject(span.Context(), opentracing.TextMap, injectAdapter)

	if err != nil {
		log.Error("could not inject span context into pulsar message", err)
	}
}

func ExtractSpanContextFromProducerMessage(message *pulsar.ProducerMessage) opentracing.SpanContext {
	extractAdapter := &ProducerMessageExtractAdapter{message}

	spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, extractAdapter)

	if err != nil {
		log.Error("could not extract span context from pulsar message", err)
	}

	return spanContext
}

func ExtractSpanContextFromConsumerMessage(message pulsar.ConsumerMessage) opentracing.SpanContext {
	extractAdapter := &ConsumerMessageExtractAdapter{message}

	spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, extractAdapter)

	if err != nil {
		log.Error("could not extract span context from pulsar message", err)
	}

	return spanContext
}

func InjectConsumerMessageSpanContext(ctx context.Context, message pulsar.ConsumerMessage) {
	injectAdapter := &ConsumerMessageInjectAdapter{message}
	span := opentracing.SpanFromContext(ctx)

	if span == nil {
		log.Warn("no span could be extracted from context, nothing will be injected into the message properties")
		return
	}

	for k, v := range message.Properties() {
		span.SetTag(k, v)
	}

	err := opentracing.GlobalTracer().Inject(span.Context(), opentracing.TextMap, injectAdapter)

	if err != nil {
		log.Error("could not inject span context into pulsar message", err)
	}
}

func CreateSpanFromMessage(cm *pulsar.ConsumerMessage, tracer opentracing.Tracer, label string) opentracing.Span {
	parentSpan := ExtractSpanContextFromConsumerMessage(*cm)
	var span opentracing.Span
	if parentSpan != nil {
		span = tracer.StartSpan(label, opentracing.ChildOf(parentSpan))
	} else {
		span = tracer.StartSpan(label)
	}
	return span
}
