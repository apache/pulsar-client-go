// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

type ConsumerConfig struct {
	SchemaType         string            `json:"schemaType,omitempty" yaml:"schemaType"`
	SerdeClassName     string            `json:"serdeClassName,omitempty" yaml:"serdeClassName"`
	RegexPattern       bool              `json:"regexPattern,omitempty" yaml:"regexPattern"`
	ReceiverQueueSize  int               `json:"receiverQueueSize,omitempty" yaml:"receiverQueueSize"`
	SchemaProperties   map[string]string `json:"schemaProperties,omitempty" yaml:"schemaProperties"`
	ConsumerProperties map[string]string `json:"consumerProperties,omitempty" yaml:"consumerProperties"`
	CryptoConfig       *CryptoConfig     `json:"cryptoConfig,omitempty" yaml:"cryptoConfig"`
	PoolMessages       bool              `json:"poolMessages,omitempty" yaml:"poolMessages"`
}
