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
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestProducerMessageInjectAndExtract(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	otel.SetTracerProvider(provider)

	message := &pulsar.ProducerMessage{
		Properties: map[string]string{},
	}

	tracer := otel.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test-span")
	InjectProducerMessageSpanContext(ctx, message)
	assert.NotNil(t, span)

	ctx2 := ExtractSpanContextFromProducerMessage(context.Background(), message)
	span2 := trace.SpanFromContext(ctx2)
	assert.NotNil(t, span2)
}

func TestConsumerMessageInjectAndExtract(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	otel.SetTracerProvider(provider)

	message := pulsar.ConsumerMessage{
		Message: &mockConsumerMessage{
			properties: map[string]string{},
		},
	}

	tracer := otel.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test-span")
	InjectConsumerMessageSpanContext(ctx, message)
	assert.NotNil(t, span)

	ctx2 := ExtractSpanContextFromConsumerMessage(context.Background(), message)
	span2 := trace.SpanFromContext(ctx2)
	assert.NotNil(t, span2)
}

type mockConsumerMessage struct {
	properties map[string]string
}

func (msg *mockConsumerMessage) Topic() string                      { return "test-topic" }
func (msg *mockConsumerMessage) Properties() map[string]string      { return msg.properties }
func (msg *mockConsumerMessage) Payload() []byte                    { return nil }
func (msg *mockConsumerMessage) ID() pulsar.MessageID               { return pulsar.NewMessageID(-1, -1, -1, 0) }
func (msg *mockConsumerMessage) PublishTime() time.Time             { return time.Time{} }
func (msg *mockConsumerMessage) EventTime() time.Time               { return time.Time{} }
func (msg *mockConsumerMessage) Key() string                        { return "" }
func (msg *mockConsumerMessage) OrderingKey() string                { return "" }
func (msg *mockConsumerMessage) RedeliveryCount() uint32            { return 0 }
func (msg *mockConsumerMessage) IsReplicated() bool                 { return false }
func (msg *mockConsumerMessage) GetReplicatedFrom() string          { return "" }
func (msg *mockConsumerMessage) GetSchemaValue(_ interface{}) error { return nil }
func (msg *mockConsumerMessage) ProducerName() string               { return "" }
func (msg *mockConsumerMessage) SchemaVersion() []byte              { return nil }
func (msg *mockConsumerMessage) GetEncryptionContext() *pulsar.EncryptionContext {
	return &pulsar.EncryptionContext{}
}
func (msg *mockConsumerMessage) Index() *uint64                { return nil }
func (msg *mockConsumerMessage) BrokerPublishTime() *time.Time { return nil }
