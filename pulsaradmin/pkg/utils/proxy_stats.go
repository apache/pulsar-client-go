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

// ProxyStats represents statistics for a Pulsar proxy
type ProxyStats struct {
	ProxyName              string  `json:"proxyName"`
	DirectMemoryUsed       int64   `json:"directMemoryUsed"`
	JvmMemoryUsed          int64   `json:"jvmMemoryUsed"`
	SystemCpuUsage         float64 `json:"systemCpuUsage"`
	ProcessCpuUsage        float64 `json:"processCpuUsage"`
	ActiveConnections      int64   `json:"activeConnections"`
	TotalConnections       int64   `json:"totalConnections"`
	BytesIn                int64   `json:"bytesIn"`
	BytesOut               int64   `json:"bytesOut"`
	MsgIn                  int64   `json:"msgIn"`
	MsgOut                 int64   `json:"msgOut"`
	TopicLoadTimeMs        int64   `json:"topicLoadTimeMs"`
	BrokerCount            int     `json:"brokerCount"`
	BundleCount            int     `json:"bundleCount"`
	ConsumerCount          int64   `json:"consumerCount"`
	ProducerCount          int64   `json:"producerCount"`
	RequestParseTimeMs     int64   `json:"requestParseTimeMs"`
	RequestProcessTimeMs   int64   `json:"requestProcessTimeMs"`
}

// ConnectionStats represents connection statistics
type ConnectionStats struct {
	Address                string  `json:"address"`
	ConnectedSince         string  `json:"connectedSince"`
	ClientVersion          string  `json:"clientVersion"`
	ConnectTime            int64   `json:"connectTime"`
	RemoteAddress          string  `json:"remoteAddress"`
	MsgRateIn              float64 `json:"msgRateIn"`
	MsgRateOut             float64 `json:"msgRateOut"`
	MsgThroughputIn        float64 `json:"msgThroughputIn"`
	MsgThroughputOut       float64 `json:"msgThroughputOut"`
	ConsumerCount          int     `json:"consumerCount"`
	ProducerCount          int     `json:"producerCount"`
}

// ProxyTopicStats represents topic statistics at proxy level
type ProxyTopicStats struct {
	TopicName              string  `json:"topicName"`
	ConsumerCount          int     `json:"consumerCount"`
	ProducerCount          int     `json:"producerCount"`
	MsgRateIn              float64 `json:"msgRateIn"`
	MsgRateOut             float64 `json:"msgRateOut"`
	MsgThroughputIn        float64 `json:"msgThroughputIn"`
	MsgThroughputOut       float64 `json:"msgThroughputOut"`
}

// BundleUnloadingMetrics represents bundle unloading metrics for load balancing
type BundleUnloadingMetrics struct {
	LoadBalanceSuccessCount   int64   `json:"loadBalanceSuccessCount"`
	LoadBalanceFailCount      int64   `json:"loadBalanceFailCount"`
	UnloadBundleTotal         int64   `json:"unloadBundleTotal"`
	LoadAvg                   float64 `json:"loadAvg"`
	OverloadedBrokerCount     int     `json:"overloadedBrokerCount"`
	UnderLoadedBrokerCount    int     `json:"underLoadedBrokerCount"`
}