package pulsartracing

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/opentracing/opentracing-go"
)

const toPrefix = "To__"

type ProducerInterceptor struct {
}

func (t *ProducerInterceptor) BeforeSend(producer pulsar.Producer, message *pulsar.ProducerMessage) {
	buildAndInjectSpan(message, producer).Finish()
}

func (t *ProducerInterceptor) OnSendAcknowledgement(producer pulsar.Producer, message *pulsar.ProducerMessage, msgID pulsar.MessageID) {
}

func buildAndInjectSpan(message *pulsar.ProducerMessage, producer pulsar.Producer) opentracing.Span {
	tracer := opentracing.GlobalTracer()
	spanContext := ExtractSpanContextFromProducerMessage(message)

	var span opentracing.Span

	var startSpanOptions []opentracing.StartSpanOption
	if spanContext != nil {
		startSpanOptions = []opentracing.StartSpanOption{opentracing.FollowsFrom(spanContext)}
	}

	span = tracer.StartSpan(toPrefix+producer.Topic(), startSpanOptions...)
	span.SetTag("span.kind", "producer")
	enrichProducerSpan(producer, span)

	InjectProducerMessageSpanContext(opentracing.ContextWithSpan(context.Background(), span), message)

	return span
}
