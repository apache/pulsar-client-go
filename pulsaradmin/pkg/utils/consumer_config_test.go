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

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerConfigReceiverQueueSizeJSON(t *testing.T) {
	tests := []struct {
		name   string
		config ConsumerConfig
		want   string
	}{
		{
			name:   "unset is omitted",
			config: ConsumerConfig{},
			want:   `{}`,
		},
		{
			name: "direct non-zero assignment and other fields remain supported",
			config: ConsumerConfig{
				SchemaType:         "STRING",
				RegexPattern:       true,
				ReceiverQueueSize:  100,
				SchemaProperties:   map[string]string{"schema": "value"},
				ConsumerProperties: map[string]string{"consumer": "value"},
				PoolMessages:       true,
			},
			want: `{
				"schemaType":"STRING",
				"regexPattern":true,
				"receiverQueueSize":100,
				"schemaProperties":{"schema":"value"},
				"consumerProperties":{"consumer":"value"},
				"poolMessages":true
			}`,
		},
		{
			name: "explicit zero is included",
			config: func() ConsumerConfig {
				config := ConsumerConfig{}
				config.SetReceiverQueueSize(0)
				return config
			}(),
			want: `{"receiverQueueSize":0}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.config)
			require.NoError(t, err)
			assert.JSONEq(t, test.want, string(data))
		})
	}
}

func TestConsumerConfigReceiverQueueSizeUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    int
		isSet   bool
		schema  string
	}{
		{name: "unset", payload: `{}`, want: 0, isSet: false},
		{name: "explicit zero", payload: `{"receiverQueueSize":0}`, want: 0, isSet: true},
		{
			name:    "non-zero with other fields",
			payload: `{"receiverQueueSize":100,"schemaType":"STRING"}`,
			want:    100,
			isSet:   true,
			schema:  "STRING",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var config ConsumerConfig
			require.NoError(t, json.Unmarshal([]byte(test.payload), &config))
			assert.Equal(t, test.want, config.ReceiverQueueSize)
			assert.Equal(t, test.isSet, config.HasReceiverQueueSize())
			assert.Equal(t, test.schema, config.SchemaType)
		})
	}
}
