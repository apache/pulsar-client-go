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

package pulsaradmin

type DispatchRate struct {
	DispatchThrottlingRateInMsg  int   `json:"dispatchThrottlingRateInMsg"`
	DispatchThrottlingRateInByte int64 `json:"dispatchThrottlingRateInByte"`
	RatePeriodInSecond           int   `json:"ratePeriodInSecond"`
}

func NewDispatchRate() *DispatchRate {
	return &DispatchRate{
		DispatchThrottlingRateInMsg:  -1,
		DispatchThrottlingRateInByte: -1,
		RatePeriodInSecond:           1,
	}
}

type SubscribeRate struct {
	SubscribeThrottlingRatePerConsumer int `json:"subscribeThrottlingRatePerConsumer"`
	RatePeriodInSecond                 int `json:"ratePeriodInSecond"`
}

func NewSubscribeRate() *SubscribeRate {
	return &SubscribeRate{
		SubscribeThrottlingRatePerConsumer: -1,
		RatePeriodInSecond:                 30,
	}
}
