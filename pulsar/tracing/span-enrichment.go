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
	"github.com/apache/pulsar-client-go/pulsar"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const componentName = "pulsar-client-go"

func enrichConsumerSpan(message *pulsar.ConsumerMessage, span trace.Span) {
	spanCommonAttributes(span)
	for k, v := range message.Properties() {
		span.SetAttributes(attribute.String(k, v))
	}
	span.SetAttributes(
		attribute.String("topic", message.Topic()),
		attribute.String("messageId", message.ID().String()),
		attribute.String("subscription", message.Subscription()),
	)
}

func enrichProducerSpan(message *pulsar.ProducerMessage, producer pulsar.Producer, span trace.Span) {
	spanCommonAttributes(span)
	for k, v := range message.Properties {
		span.SetAttributes(attribute.String(k, v))
	}
	span.SetAttributes(
		attribute.String("topic", producer.Topic()),
		attribute.Int64("sequenceId", producer.LastSequenceID()),
	)
}

func spanCommonAttributes(span trace.Span) {
	span.SetAttributes(
		attribute.String("component", "pulsar-client-go"),
		attribute.String("peer.service", "pulsar-broker"),
	)
}
