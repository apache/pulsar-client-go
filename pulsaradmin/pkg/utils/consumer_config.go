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

package utils

import "encoding/json"

type ConsumerConfig struct {
	SchemaType           string `json:"schemaType,omitempty" yaml:"schemaType"`
	SerdeClassName       string `json:"serdeClassName,omitempty" yaml:"serdeClassName"`
	RegexPattern         bool   `json:"regexPattern,omitempty" yaml:"regexPattern"`
	ReceiverQueueSize    int    `json:"-" yaml:"receiverQueueSize"`
	receiverQueueSizeSet bool
	SchemaProperties     map[string]string `json:"schemaProperties,omitempty" yaml:"schemaProperties"`
	ConsumerProperties   map[string]string `json:"consumerProperties,omitempty" yaml:"consumerProperties"`
	CryptoConfig         *CryptoConfig     `json:"cryptoConfig,omitempty" yaml:"cryptoConfig"`
	PoolMessages         bool              `json:"poolMessages,omitempty" yaml:"poolMessages"`
}

// SetReceiverQueueSize records an explicitly configured receiver queue size. It is required when
// the value is zero so JSON marshaling can distinguish zero from an unset value.
func (c *ConsumerConfig) SetReceiverQueueSize(value int) {
	c.ReceiverQueueSize = value
	c.receiverQueueSizeSet = true
}

// HasReceiverQueueSize reports whether receiverQueueSize was explicitly configured or returned by
// the server. Non-zero values assigned directly remain supported for backward compatibility.
func (c ConsumerConfig) HasReceiverQueueSize() bool {
	return c.receiverQueueSizeSet || c.ReceiverQueueSize != 0
}

type consumerConfigJSON ConsumerConfig

func (c ConsumerConfig) MarshalJSON() ([]byte, error) {
	var receiverQueueSize *int
	if c.HasReceiverQueueSize() {
		value := c.ReceiverQueueSize
		receiverQueueSize = &value
	}

	return json.Marshal(struct {
		consumerConfigJSON
		ReceiverQueueSize *int `json:"receiverQueueSize,omitempty"`
	}{
		consumerConfigJSON: consumerConfigJSON(c),
		ReceiverQueueSize:  receiverQueueSize,
	})
}

func (c *ConsumerConfig) UnmarshalJSON(data []byte) error {
	var value struct {
		consumerConfigJSON
		ReceiverQueueSize *int `json:"receiverQueueSize,omitempty"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	*c = ConsumerConfig(value.consumerConfigJSON)
	c.ReceiverQueueSize = 0
	c.receiverQueueSizeSet = false
	if value.ReceiverQueueSize != nil {
		c.SetReceiverQueueSize(*value.ReceiverQueueSize)
	}

	return nil
}
