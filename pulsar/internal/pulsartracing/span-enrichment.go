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
	"github.com/skulkarni-ns/pulsar-client-go/pulsar"
	"github.com/opentracing/opentracing-go"
)

func enrichConsumerSpan(message *pulsar.ConsumerMessage, span opentracing.Span) {
	spanCommonTags(span)

	for k, v := range message.Properties() {
		span.SetTag(k, v)
	}
	span.SetTag("message_bus.destination", message.Topic())
	span.SetTag("messageId", message.ID())
	span.SetTag("subscription", message.Subscription())
}

func enrichProducerSpan(message *pulsar.ProducerMessage, producer pulsar.Producer, span opentracing.Span) {
	spanCommonTags(span)

	for k, v := range message.Properties {
		span.SetTag(k, v)
	}
	span.SetTag("span.kind", "producer")
	span.SetTag("message_bus.destination", producer.Topic())
	span.SetTag("sequenceId", producer.LastSequenceID())
}

func spanCommonTags(span opentracing.Span) {
	span.SetTag("component", "pulsar-client-go")
	span.SetTag("peer.service", "pulsar-broker")
}
