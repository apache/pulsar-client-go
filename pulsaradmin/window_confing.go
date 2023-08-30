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

const WindowConfigKey = "__WINDOWCONFIGS__"

type WindowConfig struct {
	WindowLengthCount             *int    `json:"windowLengthCount" yaml:"windowLengthCount"`
	WindowLengthDurationMs        *int64  `json:"windowLengthDurationMs" yaml:"windowLengthDurationMs"`
	SlidingIntervalCount          *int    `json:"slidingIntervalCount" yaml:"slidingIntervalCount"`
	SlidingIntervalDurationMs     *int64  `json:"slidingIntervalDurationMs" yaml:"slidingIntervalDurationMs"`
	LateDataTopic                 *string `json:"lateDataTopic" yaml:"lateDataTopic"`
	MaxLagMs                      *int64  `json:"maxLagMs" yaml:"maxLagMs"`
	WatermarkEmitIntervalMs       *int64  `json:"watermarkEmitIntervalMs" yaml:"watermarkEmitIntervalMs"`
	TimestampExtractorClassName   *string `json:"timestampExtractorClassName" yaml:"timestampExtractorClassName"`
	ActualWindowFunctionClassName *string `json:"actualWindowFunctionClassName" yaml:"actualWindowFunctionClassName"`
	ProcessingGuarantees          *string `json:"processingGuarantees" yaml:"processingGuarantees"`
}

func NewDefaultWindowConfing() *WindowConfig {
	windowConfig := &WindowConfig{}

	return windowConfig
}
