package pulsartracing

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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

func (c *mockConsumer) Unsubscribe() error {
	return nil
}

func (c *mockConsumer) Receive(ctx context.Context) (message pulsar.Message, err error) {
	return nil, nil
}

func (c *mockConsumer) Chan() <-chan pulsar.ConsumerMessage {
	return nil
}

func (c *mockConsumer) Ack(msg pulsar.Message) {}

func (c *mockConsumer) AckID(msgID pulsar.MessageID) {}

func (c *mockConsumer) ReconsumeLater(msg pulsar.Message, delay time.Duration) {}

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
