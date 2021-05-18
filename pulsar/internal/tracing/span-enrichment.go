package pulsartracing

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/opentracing/opentracing-go"
)

func enrichConsumerSpan(message pulsar.ConsumerMessage, span opentracing.Span) {
	spanCommonTags(span)

	span.SetTag("message_bus.destination", message.Topic())
	span.SetTag("messageId", message.ID())
	span.SetTag("subscription", message.Subscription())
}

func enrichProducerSpan(producer pulsar.Producer, span opentracing.Span) {
	spanCommonTags(span)

	span.SetTag("message_bus.destination", producer.Topic())
	span.SetTag("sequenceId", producer.LastSequenceID())
}

func spanCommonTags(span opentracing.Span) {
	span.SetTag("component", "pulsar-client-go")
	span.SetTag("peer.service", "pulsar-broker")
}
