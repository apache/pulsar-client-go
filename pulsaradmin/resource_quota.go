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

type ResourceQuota struct {
	// messages published per second
	MsgRateIn float64 `json:"msgRateIn"`
	// messages consumed per second
	MsgRateOut float64 `json:"msgRateOut"`
	// incoming bytes per second
	BandwidthIn float64 `json:"bandwidthIn"`
	// outgoing bytes per second
	BandwidthOut float64 `json:"bandwidthOut"`
	// used memory in Mbytes
	Memory float64 `json:"memory"`
	// allow the quota be dynamically re-calculated according to real traffic
	Dynamic bool `json:"dynamic"`
}

func NewResourceQuota() *ResourceQuota {
	return &ResourceQuota{
		MsgRateIn:    0.0,
		MsgRateOut:   0.0,
		BandwidthIn:  0.0,
		BandwidthOut: 0.0,
		Memory:       0.0,
		Dynamic:      true,
	}
}
