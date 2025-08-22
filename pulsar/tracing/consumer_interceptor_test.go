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
)

func TestConsumerBuildAndInjectChildSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	otel.SetTracerProvider(provider)

	message := pulsar.ConsumerMessage{
		Consumer: &mockConsumer{},
		Message: &mockConsumerMessage{
			properties: map[string]string{},
		},
	}

	interceptor := &ConsumerInterceptor{}
	interceptor.BeforeConsume(message)

	spans := exporter.GetSpans()
	assert.NotEmpty(t, spans)
	span := spans[0]
	assert.Contains(t, span.Name, "From__")
}

type mockConsumer struct{}

func (c *mockConsumer) Subscription() string                                    { return "test-sub" }
func (c *mockConsumer) AckWithTxn(_ pulsar.Message, _ pulsar.Transaction) error { return nil }
func (c *mockConsumer) Unsubscribe() error                                      { return nil }
func (c *mockConsumer) UnsubscribeForce() error                                 { return nil }
func (c *mockConsumer) Receive(_ context.Context) (message pulsar.Message, err error) {
	return nil, nil
}
func (c *mockConsumer) Chan() <-chan pulsar.ConsumerMessage              { return nil }
func (c *mockConsumer) Ack(_ pulsar.Message) error                       { return nil }
func (c *mockConsumer) AckID(_ pulsar.MessageID) error                   { return nil }
func (c *mockConsumer) AckIDList(_ []pulsar.MessageID) error             { return nil }
func (c *mockConsumer) AckCumulative(_ pulsar.Message) error             { return nil }
func (c *mockConsumer) AckIDCumulative(_ pulsar.MessageID) error         { return nil }
func (c *mockConsumer) ReconsumeLater(_ pulsar.Message, _ time.Duration) {}
func (c *mockConsumer) ReconsumeLaterWithCustomProperties(_ pulsar.Message, _ map[string]string, _ time.Duration) {
}
func (c *mockConsumer) Nack(_ pulsar.Message)         {}
func (c *mockConsumer) NackID(_ pulsar.MessageID)     {}
func (c *mockConsumer) Close()                        {}
func (c *mockConsumer) Seek(_ pulsar.MessageID) error { return nil }
func (c *mockConsumer) SeekByTime(_ time.Time) error  { return nil }
func (c *mockConsumer) Name() string                  { return "" }
func (c *mockConsumer) GetLastMessageIDs() ([]pulsar.TopicMessageID, error) {
	return []pulsar.TopicMessageID{}, nil
}
