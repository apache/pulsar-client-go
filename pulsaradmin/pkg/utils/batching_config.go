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

// DefaultBatchingMaxPublishDelayMs is the maximum publish delay the broker applies when a producer
// has no batching configuration. It mirrors the default in the Java runtime's
// BatchingUtils.convertFromSpec(null).
const DefaultBatchingMaxPublishDelayMs = 10

// BatchingConfig is the producer batching configuration for a Pulsar Function, Source or Sink,
// introduced by PIP-401.
//
// It requires a broker running Apache Pulsar 4.1.0 or later. Earlier releases have no batchingConfig
// field on producerConfig and ignore this value, leaving the producer on its built-in defaults.
//
// The pointer fields distinguish "not configured" from an explicit value, matching the boxed
// Integer fields in the Java model. Note that the broker applies each of them only when it is
// present and greater than zero, so a nil field and a zero field both defer to the default; zero is
// not a way to switch a limit off. Use Enabled to turn batching off entirely.
type BatchingConfig struct {
	// Enabled reports whether batching is on. It is always serialized, with no omitempty, because
	// the broker reads it as a primitive boolean with no fallback: BatchingUtils.convert() calls
	// setEnabled(config.isEnabled()) unconditionally, so a payload that omits the field can leave
	// batching disabled rather than defaulting to on. Use NewBatchingConfig to start from the same
	// defaults the broker applies when no configuration is present.
	Enabled bool `json:"enabled" yaml:"enabled"`

	// BatchingMaxPublishDelayMs is the batching linger in milliseconds.
	//
	// Zero does not disable the linger. BatchingUtils.convert() applies the value only when it is
	// greater than zero, so the broker treats zero as unconfigured and falls back to
	// DefaultBatchingMaxPublishDelayMs. Verified against Pulsar 4.1.0: a request carrying zero reads
	// back as 10. Set Enabled to false to turn batching off.
	BatchingMaxPublishDelayMs *int `json:"batchingMaxPublishDelayMs,omitempty" yaml:"batchingMaxPublishDelayMs"`

	//nolint:lll
	RoundRobinRouterBatchingPartitionSwitchFrequency *int `json:"roundRobinRouterBatchingPartitionSwitchFrequency,omitempty" yaml:"roundRobinRouterBatchingPartitionSwitchFrequency"`

	BatchingMaxMessages *int `json:"batchingMaxMessages,omitempty" yaml:"batchingMaxMessages"`

	BatchingMaxBytes *int `json:"batchingMaxBytes,omitempty" yaml:"batchingMaxBytes"`

	// BatchBuilder selects the batch construction method, either DEFAULT or KEY_BASED. When set, it
	// takes precedence over ProducerConfig.BatchBuilder, matching the order the Java runtime's
	// ProducerBuilderFactory applies them in.
	BatchBuilder string `json:"batchBuilder,omitempty" yaml:"batchBuilder"`
}

// NewBatchingConfig returns a BatchingConfig holding the defaults the broker applies to a producer
// with no batching configuration: batching enabled with a 10ms maximum publish delay.
//
// Prefer it over a bare BatchingConfig literal when tuning a single field. Go's zero value for
// Enabled is false, so a literal that sets only, say, BatchingMaxMessages would disable batching
// rather than cap the batch size.
func NewBatchingConfig() *BatchingConfig {
	maxPublishDelayMs := DefaultBatchingMaxPublishDelayMs

	return &BatchingConfig{
		Enabled:                   true,
		BatchingMaxPublishDelayMs: &maxPublishDelayMs,
	}
}
