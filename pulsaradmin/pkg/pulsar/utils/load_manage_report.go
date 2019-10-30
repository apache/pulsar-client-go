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
	"math"
)

type LocalBrokerData struct {
	// URLs to satisfy contract of ServiceLookupData (used by NamespaceService).
	WebServiceURL              string `json:"webServiceUrl"`
	WebServiceURLTLS           string `json:"webServiceUrlTls"`
	PulsarServiceURL           string `json:"pulsarServiceUrl"`
	PulsarServiceURLTLS        string `json:"pulsarServiceUrlTls"`
	PersistentTopicsEnabled    bool   `json:"persistentTopicsEnabled"`
	NonPersistentTopicsEnabled bool   `json:"nonPersistentTopicsEnabled"`

	// Most recently available system resource usage.
	CPU          ResourceUsage `json:"cpu"`
	Memory       ResourceUsage `json:"memory"`
	DirectMemory ResourceUsage `json:"directMemory"`
	BandwidthIn  ResourceUsage `json:"bandwidthIn"`
	BandwidthOut ResourceUsage `json:"bandwidthOut"`

	// Message data from the most recent namespace bundle stats.
	MsgThroughputIn  float64 `json:"msgThroughputIn"`
	MsgThroughputOut float64 `json:"msgThroughputOut"`
	MsgRateIn        float64 `json:"msgRateIn"`
	MsgRateOut       float64 `json:"msgRateOut"`

	// Timestamp of last update.
	LastUpdate int64 `json:"lastUpdate"`

	// The stats given in the most recent invocation of update.
	LastStats    map[string]*NamespaceBundleStats `json:"lastStats"`
	NumTopics    int                              `json:"numTopics"`
	NumBundles   int                              `json:"numBundles"`
	NumConsumers int                              `json:"numConsumers"`
	NumProducers int                              `json:"numProducers"`

	// All bundles belonging to this broker.
	Bundles []string `json:"bundles"`

	// The bundles gained since the last invocation of update.
	LastBundleGains []string `json:"lastBundleGains"`

	// The bundles lost since the last invocation of update.
	LastBundleLosses []string `json:"lastBundleLosses"`

	// The version string that this broker is running, obtained from the Maven build artifact in the POM
	BrokerVersionString string `json:"brokerVersionString"`

	// This place-holder requires to identify correct LoadManagerReport type while deserializing
	LoadReportType string `json:"loadReportType"`

	// the external protocol data advertised by protocol handlers.
	Protocols map[string]string `json:"protocols"`
}

func NewLocalBrokerData() LocalBrokerData {
	lastStats := make(map[string]*NamespaceBundleStats)
	lastStats[""] = NewNamespaceBundleStats()
	return LocalBrokerData{
		LastStats: lastStats,
	}
}

type NamespaceBundleStats struct {
	MsgRateIn        float64 `json:"msgRateIn"`
	MsgThroughputIn  float64 `json:"msgThroughputIn"`
	MsgRateOut       float64 `json:"msgRateOut"`
	MsgThroughputOut float64 `json:"msgThroughputOut"`
	ConsumerCount    int     `json:"consumerCount"`
	ProducerCount    int     `json:"producerCount"`
	TopicsNum        int64   `json:"topics"`
	CacheSize        int64   `json:"cacheSize"`

	// Consider the throughput equal if difference is less than 100 KB/s
	ThroughputDifferenceThreshold float64 `json:"throughputDifferenceThreshold"`
	// Consider the msgRate equal if the difference is less than 100
	MsgRateDifferenceThreshold float64 `json:"msgRateDifferenceThreshold"`
	// Consider the total topics/producers/consumers equal if the difference is less than 500
	TopicConnectionDifferenceThreshold int64 `json:"topicConnectionDifferenceThreshold"`
	// Consider the cache size equal if the difference is less than 100 kb
	CacheSizeDifferenceThreshold int64 `json:"cacheSizeDifferenceThreshold"`
}

func NewNamespaceBundleStats() *NamespaceBundleStats {
	return &NamespaceBundleStats{
		ThroughputDifferenceThreshold:      1e5,
		MsgRateDifferenceThreshold:         100,
		TopicConnectionDifferenceThreshold: 500,
		CacheSizeDifferenceThreshold:       100000,
	}
}

type ResourceUsage struct {
	Usage float64 `json:"usage"`
	Limit float64 `json:"limit"`
}

func (ru *ResourceUsage) Reset() {
	ru.Usage = -1
	ru.Limit = -1
}

func (ru *ResourceUsage) CompareTo(o *ResourceUsage) int {
	required := o.Limit - o.Usage
	available := ru.Limit - ru.Usage
	return compare(required, available)
}

func (ru *ResourceUsage) PercentUsage() float32 {
	var proportion float32
	if ru.Limit > 0 {
		proportion = float32(ru.Usage) / float32(ru.Limit)
	}
	return proportion * 100
}

func compare(val1, val2 float64) int {
	if val1 < val2 {
		return -1
	}

	if val1 < val2 {
		return 1
	}

	thisBits := math.Float64bits(val1)
	anotherBits := math.Float64bits(val2)

	if thisBits == anotherBits {
		return 0
	}

	if thisBits < anotherBits {
		return -1
	}
	return 1
}
