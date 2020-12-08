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

package internal

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var topicLabelNames = []string{"pulsar_tenant", "pulsar_namespace", "topic"}

type Metrics struct {
	userDefinedLabels map[string]string

	messagesPublished *prometheus.CounterVec
	bytesPublished    *prometheus.CounterVec
	messagesPending   *prometheus.GaugeVec
	bytesPending      *prometheus.GaugeVec
	publishErrors     *prometheus.CounterVec
	publishLatency    *prometheus.HistogramVec
	publishRPCLatency *prometheus.HistogramVec

	messagesReceived   *prometheus.CounterVec
	bytesReceived      *prometheus.CounterVec
	prefetchedMessages *prometheus.GaugeVec
	prefetchedBytes    *prometheus.GaugeVec
	acksCounter        *prometheus.CounterVec
	nacksCounter       *prometheus.CounterVec
	dlqCounter         *prometheus.CounterVec
	processingTime     *prometheus.HistogramVec

	producersOpened     *prometheus.CounterVec
	producersClosed     *prometheus.CounterVec
	producersPartitions *prometheus.GaugeVec
	consumersOpened     *prometheus.CounterVec
	consumersClosed     *prometheus.CounterVec
	consumersPartitions *prometheus.GaugeVec
	readersOpened       *prometheus.CounterVec
	readersClosed       *prometheus.CounterVec

	// Metrics that are not labeled with topic, are immediately available
	ConnectionsOpened              prometheus.Counter
	ConnectionsClosed              prometheus.Counter
	ConnectionsEstablishmentErrors prometheus.Counter
	ConnectionsHandshakeErrors     prometheus.Counter
	LookupRequestsCount            prometheus.Counter
	RpcRequestCount                prometheus.Counter
}

type TopicMetrics struct {
	MessagesPublished        prometheus.Counter
	BytesPublished           prometheus.Counter
	MessagesPending          prometheus.Gauge
	BytesPending             prometheus.Gauge
	PublishErrorsTimeout     prometheus.Counter
	PublishErrorsMsgTooLarge prometheus.Counter
	PublishLatency           prometheus.Observer
	PublishRPCLatency        prometheus.Observer

	MessagesReceived   prometheus.Counter
	BytesReceived      prometheus.Counter
	PrefetchedMessages prometheus.Gauge
	PrefetchedBytes    prometheus.Gauge
	AcksCounter        prometheus.Counter
	NacksCounter       prometheus.Counter
	DlqCounter         prometheus.Counter
	ProcessingTime     prometheus.Observer

	ProducersOpened     prometheus.Counter
	ProducersClosed     prometheus.Counter
	ProducersPartitions prometheus.Gauge
	ConsumersOpened     prometheus.Counter
	ConsumersClosed     prometheus.Counter
	ConsumersPartitions prometheus.Gauge
	ReadersOpened       prometheus.Counter
	ReadersClosed       prometheus.Counter
}

func keys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func NewMetricsProvider(userDefinedLabels map[string]string) *Metrics {
	constLabels := map[string]string{
		"client": "go",
	}
	for k, v := range userDefinedLabels {
		constLabels[k] = v
	}

	return &Metrics{
		userDefinedLabels: userDefinedLabels,
		messagesPublished: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_messages_published",
			Help:        "Counter of messages published by the client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		bytesPublished: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_bytes_published",
			Help:        "Counter of messages published by the client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		messagesPending: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pulsar_client_producer_pending_messages",
			Help:        "Counter of messages pending to be published by the client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		bytesPending: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pulsar_client_producer_pending_bytes",
			Help:        "Counter of bytes pending to be published by the client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		publishErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_producer_errors",
			Help:        "Counter of publish errors",
			ConstLabels: constLabels,
		}, append(topicLabelNames, "error")),

		publishLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "pulsar_client_producer_latency_seconds",
			Help:        "Publish latency experienced by the client",
			ConstLabels: constLabels,
			Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, topicLabelNames),

		publishRPCLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "pulsar_client_producer_rpc_latency_seconds",
			Help:        "Publish RPC latency experienced internally by the client when sending data to receiving an ack",
			ConstLabels: constLabels,
			Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, topicLabelNames),

		producersOpened: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_producers_opened",
			Help:        "Counter of producers created by the client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		producersClosed: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_producers_closed",
			Help:        "Counter of producers closed by the client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		producersPartitions: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pulsar_client_producers_partitions_active",
			Help:        "Counter of individual partitions the producers are currently active",
			ConstLabels: constLabels,
		}, topicLabelNames),

		consumersOpened: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_consumers_opened",
			Help:        "Counter of consumers created by the client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		consumersClosed: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_consumers_closed",
			Help:        "Counter of consumers closed by the client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		consumersPartitions: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pulsar_client_consumers_partitions_active",
			Help:        "Counter of individual partitions the consumers are currently active",
			ConstLabels: constLabels,
		}, topicLabelNames),

		messagesReceived: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_messages_received",
			Help:        "Counter of messages received by the client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		bytesReceived: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_bytes_received",
			Help:        "Counter of bytes received by the client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		prefetchedMessages: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pulsar_client_consumer_prefetched_messages",
			Help:        "Number of messages currently sitting in the consumer pre-fetch queue",
			ConstLabels: constLabels,
		}, topicLabelNames),

		prefetchedBytes: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pulsar_client_consumer_prefetched_bytes",
			Help:        "Total number of bytes currently sitting in the consumer pre-fetch queue",
			ConstLabels: constLabels,
		}, topicLabelNames),

		acksCounter: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_consumer_acks",
			Help:        "Counter of messages acked by client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		nacksCounter: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_consumer_nacks",
			Help:        "Counter of messages nacked by client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		dlqCounter: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_consumer_dlq_messages",
			Help:        "Counter of messages sent to Dead letter queue",
			ConstLabels: constLabels,
		}, topicLabelNames),

		processingTime: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "pulsar_client_consumer_processing_time_seconds",
			Help:        "Time it takes for application to process messages",
			Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			ConstLabels: constLabels,
		}, topicLabelNames),

		readersOpened: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_readers_opened",
			Help:        "Counter of readers created by the client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		readersClosed: promauto.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_readers_closed",
			Help:        "Counter of readers closed by the client",
			ConstLabels: constLabels,
		}, topicLabelNames),

		ConnectionsOpened: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_connections_opened",
			Help:        "Counter of connections created by the client",
			ConstLabels: constLabels,
		}),

		ConnectionsClosed: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_connections_closed",
			Help:        "Counter of connections closed by the client",
			ConstLabels: constLabels,
		}),

		ConnectionsEstablishmentErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_connections_establishment_errors",
			Help:        "Counter of errors in connections establishment",
			ConstLabels: constLabels,
		}),

		ConnectionsHandshakeErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_connections_handshake_errors",
			Help:        "Counter of errors in connections handshake (eg: authz)",
			ConstLabels: constLabels,
		}),

		LookupRequestsCount: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_lookup_count",
			Help:        "Counter of lookup requests made by the client",
			ConstLabels: constLabels,
		}),

		RpcRequestCount: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_rpc_count",
			Help:        "Counter of RPC requests made by the client",
			ConstLabels: constLabels,
		}),
	}
}

func (mp *Metrics) GetTopicMetrics(t string) *TopicMetrics {
	tn, _ := ParseTopicName(t)
	topic := TopicNameWithoutPartitionPart(tn)
	labels := map[string]string{
		"pulsar_tenant":    tn.Tenant,
		"pulsar_namespace": tn.Namespace,
		"topic":            topic,
	}

	tm := &TopicMetrics{
		MessagesPublished:        mp.messagesPublished.With(labels),
		BytesPublished:           mp.bytesPublished.With(labels),
		MessagesPending:          mp.messagesPending.With(labels),
		BytesPending:             mp.bytesPending.With(labels),
		PublishErrorsTimeout:     mp.publishErrors.With(mergeMaps(labels, map[string]string{"error": "timeout"})),
		PublishErrorsMsgTooLarge: mp.publishErrors.With(mergeMaps(labels, map[string]string{"error": "msg_too_large"})),
		PublishLatency:           mp.publishLatency.With(labels),
		PublishRPCLatency:        mp.publishRPCLatency.With(labels),

		MessagesReceived:   mp.messagesReceived.With(labels),
		BytesReceived:      mp.bytesReceived.With(labels),
		PrefetchedMessages: mp.prefetchedMessages.With(labels),
		PrefetchedBytes:    mp.prefetchedBytes.With(labels),
		AcksCounter:        mp.acksCounter.With(labels),
		NacksCounter:       mp.nacksCounter.With(labels),
		DlqCounter:         mp.dlqCounter.With(labels),
		ProcessingTime:     mp.processingTime.With(labels),

		ProducersOpened:     mp.producersOpened.With(labels),
		ProducersClosed:     mp.producersClosed.With(labels),
		ProducersPartitions: mp.producersPartitions.With(labels),
		ConsumersOpened:     mp.consumersOpened.With(labels),
		ConsumersClosed:     mp.consumersClosed.With(labels),
		ConsumersPartitions: mp.consumersPartitions.With(labels),
		ReadersOpened:       mp.readersOpened.With(labels),
		ReadersClosed:       mp.readersClosed.With(labels),
	}

	return tm
}

func mergeMaps(a, b map[string]string) map[string]string {
	res := make(map[string]string)
	for k, v := range a {
		res[k] = v
	}
	for k, v := range b {
		res[k] = v
	}

	return res
}
