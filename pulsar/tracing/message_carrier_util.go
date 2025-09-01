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

func InjectProducerMessageSpanContext(ctx context.Context, message *pulsar.ProducerMessage) {
	carrier := ProducerMessageCarrier{message}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

func ExtractSpanContextFromProducerMessage(ctx context.Context, message *pulsar.ProducerMessage) context.Context {
	carrier := ProducerMessageCarrier{message}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func InjectConsumerMessageSpanContext(ctx context.Context, message pulsar.ConsumerMessage) {
	carrier := ConsumerMessageCarrier{message}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

func ExtractSpanContextFromConsumerMessage(ctx context.Context, message pulsar.ConsumerMessage) context.Context {
	carrier := ConsumerMessageCarrier{message}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// CreateSpanFromMessage create a span from a consumer message, using extracted context as parent
func CreateSpanFromMessage(cm *pulsar.ConsumerMessage, tracerName, label string) (context.Context, trace.Span) {
	ctx := ExtractSpanContextFromConsumerMessage(context.Background(), *cm)
	tracer := otel.Tracer(tracerName)
	return tracer.Start(ctx, label)
}
