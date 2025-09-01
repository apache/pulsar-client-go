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
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestProducerBuildAndInjectSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	otel.SetTracerProvider(provider)

	message := &pulsar.ProducerMessage{
		Properties: map[string]string{},
	}

	producer := &mockProducer{}
	interceptor := &ProducerInterceptor{}
	interceptor.BeforeSend(producer, message)

	spans := exporter.GetSpans()
	assert.NotEmpty(t, spans)
	span := spans[0]
	assert.Contains(t, span.Name, "To__")
}

type mockProducer struct{}

func (p *mockProducer) Topic() string { return "test-topic" }
func (p *mockProducer) Name() string  { return "" }
func (p *mockProducer) Send(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	return nil, nil
}
func (p *mockProducer) SendAsync(context.Context, *pulsar.ProducerMessage, func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
}
func (p *mockProducer) LastSequenceID() int64                { return 0 }
func (p *mockProducer) Flush() error                         { return nil }
func (p *mockProducer) FlushWithCtx(_ context.Context) error { return nil }
func (p *mockProducer) Close()                               {}
