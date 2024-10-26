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

func (a *ProducerMessageExtractAdapter) Set(_, _ string) {}

// ProducerMessageInjectAdapter Implements TextMap Interface
type ProducerMessageInjectAdapter struct {
	message *pulsar.ProducerMessage
}

func (a *ProducerMessageInjectAdapter) ForeachKey(_ func(_, _ string) error) error {
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

func (a *ConsumerMessageExtractAdapter) Set(_, _ string) {}

// ConsumerMessageInjectAdapter Implements TextMap Interface
type ConsumerMessageInjectAdapter struct {
	message pulsar.ConsumerMessage
}

func (a *ConsumerMessageInjectAdapter) ForeachKey(_ func(_, _ string) error) error {
	return errors.New("iterator should never be used with tracer.inject()")
}

func (a *ConsumerMessageInjectAdapter) Set(key, val string) {
	a.message.Properties()[key] = val
}
