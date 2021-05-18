package pulsartracing

import (
	"errors"
	"github.com/apache/pulsar-client-go/pulsar"
)

// ProducerMessageExtractAdapter Implements TextMap Interface
type ProducerMessageExtractAdapter struct {
	message *pulsar.ProducerMessage
}

func (a *ProducerMessageExtractAdapter) ForeachKey(handler func(key, val string) error) error {
	for k, v := range (*a.message).Properties {
		if err := handler(k, v); err != nil {
			return err
		}
	}

	return nil
}

func (a *ProducerMessageExtractAdapter) Set(key, val string) {}

// ProducerMessageInjectAdapter Implements TextMap Interface
type ProducerMessageInjectAdapter struct {
	message *pulsar.ProducerMessage
}

func (a *ProducerMessageInjectAdapter) ForeachKey(handler func(key, val string) error) error {
	return errors.New("iterator should never be used with Tracer.inject()")
}

func (a *ProducerMessageInjectAdapter) Set(key, val string) {
	a.message.Properties[key] = val
}

// ConsumerMessageExtractAdapter Implements TextMap Interface
type ConsumerMessageExtractAdapter struct {
	message pulsar.ConsumerMessage
}

func (a *ConsumerMessageExtractAdapter) ForeachKey(handler func(key, val string) error) error {
	for k, v := range a.message.Properties() {
		if err := handler(k, v); err != nil {
			return err
		}
	}

	return nil
}

func (a *ConsumerMessageExtractAdapter) Set(key, val string) {}

// ConsumerMessageInjectAdapter Implements TextMap Interface
type ConsumerMessageInjectAdapter struct {
	message pulsar.ConsumerMessage
}

func (a *ConsumerMessageInjectAdapter) ForeachKey(handler func(key, val string) error) error {
	return errors.New("iterator should never be used with tracer.inject()")
}

func (a *ConsumerMessageInjectAdapter) Set(key, val string) {
	a.message.Properties()[key] = val
}
