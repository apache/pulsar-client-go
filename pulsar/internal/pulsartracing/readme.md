### Usage

#### Interceptors based solution

```go
// create new tracer
// register tracer with GlobalTracer
opentracing.SetGlobalTracer(tracer)
```

**Producer**

```go
tracingInterceptor := &pulsartracing.ProducerInterceptor{}

options := pulsar.ProducerOptions{
Topic:            topicName,
Interceptors:     pulsar.ProducerInterceptors{tracingInterceptor},
}
```

**Consumer**
```go
tracingInterceptor := &pulsartracing.ConsumerInterceptor{}

options := pulsar.ConsumerOptions{
Topics:           topicName,
SubscriptionName: subscriptionName,
Type:             pulsar.Shared,
Interceptors:      pulsar.ConsumerInterceptors{tracingInterceptor},
}


// to create span with message as parent span
span := pulsartracing.CreateSpanFromMessage(message, tracer, "child_span")
```

## License

[Apache 2.0 License](./LICENSE).