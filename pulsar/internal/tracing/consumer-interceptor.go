package pulsartracing

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/opentracing/opentracing-go"
)

const fromPrefix = "From__"

type ConsumerInterceptor struct {
}

func (t *ConsumerInterceptor) BeforeConsume(message pulsar.ConsumerMessage) {
	buildAndInjectChildSpan(message).Finish()
}

func (t *ConsumerInterceptor) OnAcknowledge(consumer pulsar.Consumer, msgID pulsar.MessageID) {}

func (t *ConsumerInterceptor) OnNegativeAcksSend(consumer pulsar.Consumer, msgIDs []pulsar.MessageID) {
}

func buildAndInjectChildSpan(message pulsar.ConsumerMessage) opentracing.Span {
	tracer := opentracing.GlobalTracer()
	parentContext := ExtractSpanContextFromConsumerMessage(message)

	var span opentracing.Span

	var startSpanOptions []opentracing.StartSpanOption
	if parentContext != nil {
		startSpanOptions = []opentracing.StartSpanOption{opentracing.FollowsFrom(parentContext)}
	}

	span = tracer.StartSpan(fromPrefix+message.Topic()+"__"+message.Subscription(), startSpanOptions...)

	enrichConsumerSpan(message, span)
	InjectConsumerMessageSpanContext(opentracing.ContextWithSpan(context.Background(), span), message)

	return span
}
