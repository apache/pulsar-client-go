package pulsartracing

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProducerBuildAndInjectSpan(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	message := &pulsar.ProducerMessage{
		Properties: map[string]string{},
	}

	span := buildAndInjectSpan(message, &mockProducer{})
	assert.NotNil(t, span)
	assert.True(t, len(message.Properties) > 0)
}

type mockProducer struct {
}

func (p *mockProducer) Topic() string {
	return ""
}

func (p *mockProducer) Name() string {
	return ""
}

func (p *mockProducer) Send(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	return nil, nil
}

func (p *mockProducer) SendAsync(context.Context, *pulsar.ProducerMessage, func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
}

func (p *mockProducer) LastSequenceID() int64 {
	return 0
}

func (p *mockProducer) Flush() error {
	return nil
}

func (p *mockProducer) Close() {}
