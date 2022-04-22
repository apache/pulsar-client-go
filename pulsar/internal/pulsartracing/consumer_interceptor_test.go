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
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
)

func TestConsumerBuildAndInjectChildSpan(t *testing.T) {
	tracer := mocktracer.New()

	opentracing.SetGlobalTracer(tracer)

	message := pulsar.ConsumerMessage{
		Consumer: pulsar.NewMockConsumer(),
		Message: &mockConsumerMessage{
			properties: map[string]string{},
		},
	}

	span := buildAndInjectChildSpan(message)
	assert.NotNil(t, span)
	assert.True(t, len(message.Properties()) > 0)
}
