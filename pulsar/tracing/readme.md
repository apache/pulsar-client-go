# Pulsar Client Go Tracing (OpenTelemetry)

This package provides distributed tracing support for [pulsar-client-go](https://github.com/apache/pulsar-client-go) using [OpenTelemetry (otel)](https://opentelemetry.io/). It enables you to trace message production and consumption across Pulsar topics, propagating context via message properties.

**Note:** This package is fully based on OpenTelemetry.

## Features
- Producer and Consumer interceptors for automatic span creation
- Context propagation using OpenTelemetry's W3C Trace Context
- Utilities for extracting/injecting context and creating child spans from messages

## Usage

### 1. Set up OpenTelemetry globally

Before using the interceptors, set up your OpenTelemetry tracer provider and propagator:

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/sdk/trace"
)

// Set up your exporter and tracer provider (example uses stdout)
provider := trace.NewTracerProvider(/* ...options... */)
otel.SetTracerProvider(provider)

// Set the global propagator (W3C TraceContext is default)
otel.SetTextMapPropagator(propagation.TraceContext{})
```

### 2. Producer Interceptor

```go
import "github.com/apache/pulsar-client-go/pulsar/tracing"

tracingInterceptor := &tracing.ProducerInterceptor{}

producerOptions := pulsar.ProducerOptions{
    Topic:        "your-topic",
    Interceptors: pulsar.ProducerInterceptors{tracingInterceptor},
}
producer, err := client.CreateProducer(producerOptions)
```

### 3. Consumer Interceptor

```go
import "github.com/apache/pulsar-client-go/pulsar/tracing"

tracingInterceptor := &tracing.ConsumerInterceptor{}

consumerOptions := pulsar.ConsumerOptions{
    Topics:           []string{"your-topic"},
    SubscriptionName: "your-subscription",
    Type:             pulsar.Shared,
    Interceptors:     pulsar.ConsumerInterceptors{tracingInterceptor},
}
consumer, err := client.Subscribe(consumerOptions)
```

### 4. Creating a child span from a message

If you want to create a span that is a child of the context propagated in a Pulsar message:

```go
import "github.com/apache/pulsar-client-go/pulsar/tracing"

ctx, span := tracing.CreateSpanFromMessage(&msg, "your-tracer-name", "operation-name")
defer span.End()
// ... do work with ctx ...
```

### 5. Manual context propagation

You can manually inject or extract context using:

```go
tracing.InjectProducerMessageSpanContext(ctx, producerMessage)
ctx = tracing.ExtractSpanContextFromProducerMessage(ctx, producerMessage)

tracing.InjectConsumerMessageSpanContext(ctx, consumerMessage)
ctx = tracing.ExtractSpanContextFromConsumerMessage(ctx, consumerMessage)
```

## License

[Apache 2.0 License](../../LICENSE)