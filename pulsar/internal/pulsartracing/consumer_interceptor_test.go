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

func TestConsumerBuildAndInjectChildSpan(t *testing.T) {
	tracer := mocktracer.New()

	opentracing.SetGlobalTracer(tracer)

	message := pulsar.ConsumerMessage{
		Consumer: &mockConsumer{},
		Message: &mockConsumerMessage{
			properties: map[string]string{},
		},
	}

	span := buildAndInjectChildSpan(message)
	assert.NotNil(t, span)
	assert.True(t, len(message.Properties()) > 0)
}

type mockConsumer struct {
}

func (c *mockConsumer) Subscription() string {
	return ""
}

func (c *mockConsumer) AckWithTxn(msg pulsar.Message, txn pulsar.Transaction) error {
	return nil
}

func (c *mockConsumer) Unsubscribe() error {
	return nil
}
func (c *mockConsumer) UnsubscribeForce() error {
	return nil
}

func (c *mockConsumer) Receive(ctx context.Context) (message pulsar.Message, err error) {
	return nil, nil
}

func (c *mockConsumer) Chan() <-chan pulsar.ConsumerMessage {
	return nil
}

func (c *mockConsumer) Ack(msg pulsar.Message) error {
	return nil
}

func (c *mockConsumer) AckID(msgID pulsar.MessageID) error {
	return nil
}

func (c *mockConsumer) AckCumulative(msg pulsar.Message) error {
	return nil
}

func (c *mockConsumer) AckIDCumulative(msgID pulsar.MessageID) error {
	return nil
}

func (c *mockConsumer) ReconsumeLater(msg pulsar.Message, delay time.Duration) {}

func (c *mockConsumer) ReconsumeLaterWithCustomProperties(msg pulsar.Message, customProperties map[string]string,
	delay time.Duration) {
}

func (c *mockConsumer) Nack(msg pulsar.Message) {}

func (c *mockConsumer) NackID(msgID pulsar.MessageID) {}

func (c *mockConsumer) Close() {}

func (c *mockConsumer) Seek(msgID pulsar.MessageID) error {
	return nil
}

func (c *mockConsumer) SeekByTime(time time.Time) error {
	return nil
}

func (c *mockConsumer) Name() string {
	return ""
}

func (c *mockConsumer) GetLastMessageIDs() ([]pulsar.TopicMessageID, error) {
	ids := make([]pulsar.TopicMessageID, 0)
	return ids, nil
}
