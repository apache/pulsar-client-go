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
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
)

func TestProducerMessageInjectAndExtract(t *testing.T) {
	message := &pulsar.ProducerMessage{
		Properties: map[string]string{},
	}

	tracer := mocktracer.New()

	opentracing.SetGlobalTracer(tracer)

	span := tracer.StartSpan("test")

	InjectProducerMessageSpanContext(opentracing.ContextWithSpan(context.Background(), span), message)
	assert.True(t, len(message.Properties) > 0)
	extractedSpanContext := ExtractSpanContextFromProducerMessage(message)
	assert.Equal(t, span.Context(), extractedSpanContext)
}

func TestConsumerMessageInjectAndExtract(t *testing.T) {
	message := pulsar.ConsumerMessage{
		Message: &mockConsumerMessage{
			properties: map[string]string{},
		},
	}

	tracer := mocktracer.New()

	opentracing.SetGlobalTracer(tracer)

	span := tracer.StartSpan("test")

	InjectConsumerMessageSpanContext(opentracing.ContextWithSpan(context.Background(), span), message)
	assert.True(t, len(message.Properties()) > 0)
	extractedSpanContext := ExtractSpanContextFromConsumerMessage(message)
	assert.Equal(t, span.Context(), extractedSpanContext)
}

type mockConsumerMessage struct {
	properties map[string]string
}

func (msg *mockConsumerMessage) Topic() string {
	return ""
}

func (msg *mockConsumerMessage) Properties() map[string]string {
	return msg.properties
}

func (msg *mockConsumerMessage) Payload() []byte {
	return nil
}

func (msg *mockConsumerMessage) ID() pulsar.MessageID {
	return nil
}

func (msg *mockConsumerMessage) PublishTime() time.Time {
	return time.Time{}
}

func (msg *mockConsumerMessage) EventTime() time.Time {
	return time.Time{}
}

func (msg *mockConsumerMessage) Key() string {
	return ""
}

func (msg *mockConsumerMessage) OrderingKey() string {
	return ""
}

func (msg *mockConsumerMessage) RedeliveryCount() uint32 {
	return 0
}

func (msg *mockConsumerMessage) IsReplicated() bool {
	return false
}

func (msg *mockConsumerMessage) GetReplicatedFrom() string {
	return ""
}

func (msg *mockConsumerMessage) GetSchemaValue(v interface{}) error {
	return nil
}

func (msg *mockConsumerMessage) ProducerName() string {
	return ""
}

func (msg *mockConsumerMessage) SchemaVersion() []byte {
	return nil
}
func (msg *mockConsumerMessage) GetEncryptionContext() *pulsar.EncryptionContext {
	return &pulsar.EncryptionContext{}
}
