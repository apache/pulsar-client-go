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

package tracing

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

// ProducerMessageCarrier carrier adapters for Pulsar messages
type ProducerMessageCarrier struct {
	msg *pulsar.ProducerMessage
}

func (c ProducerMessageCarrier) Get(key string) string {
	if len(c.msg.Properties) == 0 {
		c.msg.Properties = make(map[string]string)
	}
	return c.msg.Properties[key]
}

func (c ProducerMessageCarrier) Set(key, value string) {
	if len(c.msg.Properties) == 0 {
		c.msg.Properties = make(map[string]string)
	}
	c.msg.Properties[key] = value
}

func (c ProducerMessageCarrier) Keys() []string {
	if len(c.msg.Properties) == 0 {
		return nil
	}
	keys := make([]string, 0, len(c.msg.Properties))
	for k := range c.msg.Properties {
		keys = append(keys, k)
	}
	return keys
}

type ConsumerMessageCarrier struct {
	msg pulsar.ConsumerMessage
}

func (c ConsumerMessageCarrier) Get(key string) string {
	if len(c.msg.Message.Properties()) == 0 {
		return ""
	}
	return c.msg.Properties()[key]
}

func (c ConsumerMessageCarrier) Set(key, value string) {
	c.msg.Properties()[key] = value
}

func (c ConsumerMessageCarrier) Keys() []string {
	props := c.msg.Properties()
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	return keys
}
