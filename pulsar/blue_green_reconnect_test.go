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

package pulsar

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/backoff"
	"github.com/apache/pulsar-client-go/pulsar/internal"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type migrationMockLookup struct {
	internal.LookupService
	clusterURI string
	proxyURL   *url.URL
	lookupAddr *url.URL
}

func (m *migrationMockLookup) Lookup(_ string) (*internal.LookupResult, error) {
	return &internal.LookupResult{
		LogicalAddr:  m.lookupAddr,
		PhysicalAddr: m.proxyURL,
	}, nil
}

func (m *migrationMockLookup) GetBrokerAddress(
	brokerServiceURL string, proxyThroughServiceURL bool,
) (*internal.LookupResult, error) {
	logicalAddr, err := url.ParseRequestURI(brokerServiceURL)
	if err != nil {
		return nil, err
	}
	physicalAddr := logicalAddr
	if proxyThroughServiceURL {
		physicalAddr = m.proxyURL
	}
	return &internal.LookupResult{
		LogicalAddr:  logicalAddr,
		PhysicalAddr: physicalAddr,
	}, nil
}

type migrationMockConn struct {
	dummyConnection
	proxied  bool
	id       string
	mu       sync.Mutex
	listener internal.ConnectionListener
	consumer internal.ConsumerHandler
}

func newMigrationMockConn(id string) *migrationMockConn {
	return &migrationMockConn{
		id:      id,
		proxied: true,
	}
}

func (c *migrationMockConn) ID() string      { return c.id }
func (c *migrationMockConn) IsProxied() bool { return c.proxied }

func (c *migrationMockConn) RegisterListener(_ uint64, l internal.ConnectionListener) error {
	c.mu.Lock()
	c.listener = l
	c.mu.Unlock()
	return nil
}

func (c *migrationMockConn) UnregisterListener(_ uint64) {
	c.mu.Lock()
	c.listener = nil
	c.mu.Unlock()
}

func (c *migrationMockConn) AddConsumeHandler(_ uint64, h internal.ConsumerHandler) error {
	c.mu.Lock()
	c.consumer = h
	c.mu.Unlock()
	return nil
}

func (c *migrationMockConn) DeleteConsumeHandler(_ uint64) {
	c.mu.Lock()
	c.consumer = nil
	c.mu.Unlock()
}

type migrationMockPool struct {
	internal.ConnectionPool
	byPhysicalHost map[string]internal.Connection
}

func (p *migrationMockPool) GetConnection(_ *url.URL, physicalAddr *url.URL, _ int32) (internal.Connection, error) {
	if cnx, ok := p.byPhysicalHost[physicalAddr.Host]; ok {
		return cnx, nil
	}
	return nil, errors.New("unexpected physical host: " + physicalAddr.Host)
}

type migrationRequestFunc func(
	ctx context.Context, cnx internal.Connection, reqID uint64, cmdType pb.BaseCommand_Type, msg proto.Message,
) (*internal.RPCResult, error)

type migrationMockRPCClient struct {
	internal.RPCClient
	reqID       atomic.Uint64
	lookups     map[string]internal.LookupService
	onRequest   migrationRequestFunc
	lookupCalls []string
	mu          sync.Mutex
}

func (r *migrationMockRPCClient) NewRequestID() uint64 {
	return r.reqID.Add(1)
}

func (r *migrationMockRPCClient) LookupService(clusterURI string) (internal.LookupService, error) {
	r.mu.Lock()
	r.lookupCalls = append(r.lookupCalls, clusterURI)
	r.mu.Unlock()
	ls, ok := r.lookups[clusterURI]
	if !ok {
		return nil, errors.New("unexpected lookup service URI: " + clusterURI)
	}
	return ls, nil
}

func (r *migrationMockRPCClient) RequestOnCnx(cnx internal.Connection, reqID uint64,
	cmdType pb.BaseCommand_Type, msg proto.Message) (*internal.RPCResult, error) {
	return r.RequestOnCnxWithContext(context.Background(), cnx, reqID, cmdType, msg)
}

func (r *migrationMockRPCClient) RequestOnCnxWithContext(ctx context.Context, cnx internal.Connection,
	reqID uint64, cmdType pb.BaseCommand_Type, msg proto.Message) (*internal.RPCResult, error) {
	return r.onRequest(ctx, cnx, reqID, cmdType, msg)
}

type fixedBackoffPolicy struct {
	delay time.Duration
	calls atomic.Int32
}

func (f *fixedBackoffPolicy) Next() time.Duration {
	f.calls.Add(1)
	return f.delay
}

func (f *fixedBackoffPolicy) IsMaxBackoffReached() bool { return false }
func (f *fixedBackoffPolicy) Reset()                    {}

func TestConsumerLookupTopicUsesRedirectedClusterURIForAssignedBroker(t *testing.T) {
	blueProxyURL, err := url.Parse("pulsar://proxy:6650")
	require.NoError(t, err)
	greenProxyURL, err := url.Parse("pulsar://green-proxy:6650")
	require.NoError(t, err)

	blueLookup := &migrationMockLookup{clusterURI: "", proxyURL: blueProxyURL}
	greenLookup := &migrationMockLookup{clusterURI: "pulsar://green-proxy:6650", proxyURL: greenProxyURL}

	rpc := &migrationMockRPCClient{
		lookups: map[string]internal.LookupService{
			"":                          blueLookup,
			"pulsar://green-proxy:6650": greenLookup,
		},
	}

	pc := &partitionConsumer{
		client: &client{
			lookupService: blueLookup,
			rpcClient:     rpc,
			log:           log.DefaultNopLogger(),
		},
		topic: "persistent://public/default/migrated-topic",
		log:   log.DefaultNopLogger(),
	}
	pc._setConn(newMigrationMockConn("green-cnx"))
	pc.SetRedirectedClusterURI("pulsar://green-proxy:6650")

	lr, err := pc.lookupTopic("pulsar://green-broker-2:6650")
	require.NoError(t, err)
	assert.Equal(t, "green-broker-2:6650", lr.LogicalAddr.Host)
	assert.Equal(t, "green-proxy:6650", lr.PhysicalAddr.Host,
		"consumer must resolve proxied assigned broker via redirected green cluster URI, not blue lookupService")
}

func TestProducerReconnectCancelsInFlightConnectOnMigrationClose(t *testing.T) {
	blueProxyURL, _ := url.Parse("pulsar://proxy:6650")
	blueBrokerURL, _ := url.Parse("pulsar://broker-1:6650")
	greenProxyURL, _ := url.Parse("pulsar://green-proxy:6650")
	greenBrokerURL, _ := url.Parse("pulsar://green-broker-1:6650")

	blueCnx := newMigrationMockConn("blue-cnx")
	greenCnx := newMigrationMockConn("green-cnx")

	var blueCalls, greenCalls atomic.Int32
	rpc := &migrationMockRPCClient{
		lookups: map[string]internal.LookupService{
			"": &migrationMockLookup{
				proxyURL:   blueProxyURL,
				lookupAddr: blueBrokerURL,
			},
			"pulsar://green-proxy:6650": &migrationMockLookup{
				proxyURL:   greenProxyURL,
				lookupAddr: greenBrokerURL,
			},
		},
	}
	rpc.onRequest = func(
		ctx context.Context, cnx internal.Connection, _ uint64, cmdType pb.BaseCommand_Type, _ proto.Message,
	) (*internal.RPCResult, error) {
		require.Equal(t, pb.BaseCommand_PRODUCER, cmdType)
		mc := cnx.(*migrationMockConn)
		if mc == blueCnx {
			blueCalls.Add(1)
			// Simulate broker ServerCnx.redirectOrFailMigratedProducer:
			// no CommandProducer response is sent; instead broker sends
			// CommandTopicMigrated followed by CommandCloseProducer.
			go func() {
				mc.mu.Lock()
				l := mc.listener
				mc.mu.Unlock()
				l.SetRedirectedClusterURI("pulsar://green-proxy:6650")
				l.ConnectionClosed(&pb.CommandCloseProducer{ProducerId: proto.Uint64(1)})
			}()
			select {
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			case <-time.After(5 * time.Second):
				return nil, internal.ErrRequestTimeOut
			}
		}
		greenCalls.Add(1)
		return &internal.RPCResult{
			Cnx: greenCnx,
			Response: &pb.BaseCommand{
				Type: pb.BaseCommand_PRODUCER_SUCCESS.Enum(),
				ProducerSuccess: &pb.CommandProducerSuccess{
					ProducerName: proto.String("migrated-producer"),
				},
			},
		}, nil
	}

	bo := &fixedBackoffPolicy{delay: 50 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &partitionProducer{
		client: &client{
			cnxPool: &migrationMockPool{
				byPhysicalHost: map[string]internal.Connection{
					"proxy:6650":       blueCnx,
					"green-proxy:6650": greenCnx,
				},
			},
			rpcClient: rpc,
			log:       log.DefaultNopLogger(),
			metrics:   internal.NewMetricsProvider(0, map[string]string{}, prometheus.NewRegistry()),
		},
		topic:           "persistent://public/default/migrated-topic",
		producerID:      1,
		options:         &ProducerOptions{DisableBatching: true},
		log:             log.DefaultNopLogger(),
		connectClosedCh: make(chan *connectionClosed, 1),
		pendingQueue:    internal.NewBlockingQueue(10),
		schemaCache:     newSchemaCache(),
		metrics:         newTestMetrics(),
		ctx:             ctx,
		cancelFunc:      cancel,
		backOffPolicyFunc: func() backoff.Policy {
			return bo
		},
	}
	p.setProducerState(producerReady)
	p._setConn(blueCnx)

	start := time.Now()
	p.reconnectToBroker(&connectionClosed{})
	elapsed := time.Since(start)

	// Should wait initial backoff (~50ms), fail fast on migration close, and
	// immediately connect to green cluster without a 30s timeout or extra close event.
	assert.GreaterOrEqual(t, elapsed, 45*time.Millisecond,
		"first reconnect without assignedBrokerURL should wait initial backoff")
	assert.Less(t, elapsed, 2*time.Second,
		"in-flight PRODUCER RPC must be cancelled immediately on migration close instead of timing out")
	assert.Equal(t, int32(1), blueCalls.Load())
	assert.Equal(t, int32(1), greenCalls.Load())
	assert.Equal(t, greenCnx, p._getConn())
	assert.Equal(t, 0, len(p.connectClosedCh),
		"cancelling the in-flight connect must not enqueue a redundant reconnect event")
}

func TestProducerReconnectWithAssignedBrokerSkipsInitialBackoff(t *testing.T) {
	greenProxyURL, _ := url.Parse("pulsar://green-proxy:6650")
	greenCnx := newMigrationMockConn("green-cnx")

	rpc := &migrationMockRPCClient{
		lookups: map[string]internal.LookupService{
			"": &migrationMockLookup{
				proxyURL: greenProxyURL,
			},
		},
		onRequest: func(
			_ context.Context, cnx internal.Connection, _ uint64, _ pb.BaseCommand_Type, _ proto.Message,
		) (*internal.RPCResult, error) {
			return &internal.RPCResult{
				Cnx: cnx,
				Response: &pb.BaseCommand{
					Type: pb.BaseCommand_PRODUCER_SUCCESS.Enum(),
					ProducerSuccess: &pb.CommandProducerSuccess{
						ProducerName: proto.String("assigned-producer"),
					},
				},
			}, nil
		},
	}

	bo := &fixedBackoffPolicy{delay: 500 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &partitionProducer{
		client: &client{
			cnxPool: &migrationMockPool{
				byPhysicalHost: map[string]internal.Connection{
					"green-proxy:6650": greenCnx,
				},
			},
			rpcClient: rpc,
			log:       log.DefaultNopLogger(),
			metrics:   internal.NewMetricsProvider(0, map[string]string{}, prometheus.NewRegistry()),
		},
		topic:           "persistent://public/default/assigned-topic",
		producerID:      1,
		options:         &ProducerOptions{DisableBatching: true},
		log:             log.DefaultNopLogger(),
		connectClosedCh: make(chan *connectionClosed, 1),
		pendingQueue:    internal.NewBlockingQueue(10),
		schemaCache:     newSchemaCache(),
		metrics:         newTestMetrics(),
		ctx:             ctx,
		cancelFunc:      cancel,
		backOffPolicyFunc: func() backoff.Policy {
			return bo
		},
	}
	p.setProducerState(producerReady)
	p._setConn(greenCnx)

	start := time.Now()
	p.reconnectToBroker(&connectionClosed{assignedBrokerURL: "pulsar://green-broker-2:6650"})
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 250*time.Millisecond,
		"reconnect with assignedBrokerURL should connect immediately without initial backoff")
	assert.Equal(t, int32(0), bo.calls.Load())
}

func TestConsumerReconnectCancelsInFlightConnectOnMigrationClose(t *testing.T) {
	blueProxyURL, _ := url.Parse("pulsar://proxy:6650")
	blueBrokerURL, _ := url.Parse("pulsar://broker-1:6650")
	greenProxyURL, _ := url.Parse("pulsar://green-proxy:6650")
	greenBrokerURL, _ := url.Parse("pulsar://green-broker-1:6650")

	blueCnx := newMigrationMockConn("blue-cnx")
	greenCnx := newMigrationMockConn("green-cnx")

	var blueCalls, greenCalls atomic.Int32
	rpc := &migrationMockRPCClient{
		lookups: map[string]internal.LookupService{
			"": &migrationMockLookup{
				proxyURL:   blueProxyURL,
				lookupAddr: blueBrokerURL,
			},
			"pulsar://green-proxy:6650": &migrationMockLookup{
				proxyURL:   greenProxyURL,
				lookupAddr: greenBrokerURL,
			},
		},
	}
	rpc.onRequest = func(
		ctx context.Context, cnx internal.Connection, _ uint64, cmdType pb.BaseCommand_Type, _ proto.Message,
	) (*internal.RPCResult, error) {
		require.Equal(t, pb.BaseCommand_SUBSCRIBE, cmdType)
		mc := cnx.(*migrationMockConn)
		if mc == blueCnx {
			blueCalls.Add(1)
			go func() {
				mc.mu.Lock()
				h := mc.consumer
				mc.mu.Unlock()
				h.SetRedirectedClusterURI("pulsar://green-proxy:6650")
				h.ConnectionClosed(mc, &pb.CommandCloseConsumer{ConsumerId: proto.Uint64(1)})
			}()
			select {
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			case <-time.After(5 * time.Second):
				return nil, internal.ErrRequestTimeOut
			}
		}
		greenCalls.Add(1)
		return &internal.RPCResult{
			Cnx: greenCnx,
			Response: &pb.BaseCommand{
				Type: pb.BaseCommand_SUCCESS.Enum(),
			},
		}, nil
	}

	bo := &fixedBackoffPolicy{delay: 50 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc := &partitionConsumer{
		client: &client{
			cnxPool: &migrationMockPool{
				byPhysicalHost: map[string]internal.Connection{
					"proxy:6650":       blueCnx,
					"green-proxy:6650": greenCnx,
				},
			},
			rpcClient: rpc,
			log:       log.DefaultNopLogger(),
		},
		topic:                "persistent://public/default/migrated-topic",
		consumerID:           1,
		options:              &partitionConsumerOpts{subscription: "sub", subscriptionMode: Durable},
		log:                  log.DefaultNopLogger(),
		compressionProviders: sync.Map{},
		connectedCh:          make(chan struct{}, 1),
		connectClosedCh:      make(chan *connectionClosed, 1),
		queueCh:              make(chan []*message, 1),
		clearQueueCh:         make(chan func(id *trackingMessageID)),
		metrics:              newTestMetrics(),
		ctx:                  ctx,
		cancelFunc:           cancel,
		backoffPolicyFunc: func() backoff.Policy {
			return bo
		},
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case cb := <-pc.clearQueueCh:
				cb(nil)
			}
		}
	}()
	pc.setConsumerState(consumerReady)
	pc._setConn(blueCnx)

	start := time.Now()
	pc.reconnectToBroker(&connectionClosed{})
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, 45*time.Millisecond)
	assert.Less(t, elapsed, 2*time.Second)
	assert.Equal(t, int32(1), blueCalls.Load())
	assert.Equal(t, int32(1), greenCalls.Load())
	assert.Equal(t, greenCnx, pc._getConn())
	assert.Equal(t, 0, len(pc.connectClosedCh))
}

func TestRedirectedClusterURIConcurrentAccess(t *testing.T) {
	p := &partitionProducer{}
	pc := &partitionConsumer{}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				p.SetRedirectedClusterURI("pulsar://green-proxy:6650")
				_ = p.getRedirectedClusterURI()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				pc.SetRedirectedClusterURI("pulsar://green-proxy:6650")
				_ = pc.getRedirectedClusterURI()
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, "pulsar://green-proxy:6650", p.getRedirectedClusterURI())
	assert.Equal(t, "pulsar://green-proxy:6650", pc.getRedirectedClusterURI())
}

func TestProducerReconnectRetriesTopicTerminatedAfterRedirectReceived(t *testing.T) {
	blueProxyURL, _ := url.Parse("pulsar://proxy:6650")
	blueBrokerURL, _ := url.Parse("pulsar://broker-1:6650")
	greenProxyURL, _ := url.Parse("pulsar://green-proxy:6650")
	greenBrokerURL, _ := url.Parse("pulsar://green-broker-1:6650")

	blueCnx := newMigrationMockConn("blue-cnx")
	greenCnx := newMigrationMockConn("green-cnx")

	var p *partitionProducer
	rpc := &migrationMockRPCClient{
		lookups: map[string]internal.LookupService{
			"": &migrationMockLookup{
				proxyURL:   blueProxyURL,
				lookupAddr: blueBrokerURL,
			},
			"pulsar://green-proxy:6650": &migrationMockLookup{
				proxyURL:   greenProxyURL,
				lookupAddr: greenBrokerURL,
			},
		},
		onRequest: func(
			_ context.Context, cnx internal.Connection, _ uint64, _ pb.BaseCommand_Type, _ proto.Message,
		) (*internal.RPCResult, error) {
			if cnx == blueCnx {
				// Simulate receiving CommandTopicMigrated while a stale request to blue
				// fails with TopicTerminatedError.
				p.SetRedirectedClusterURI("pulsar://green-proxy:6650")
				return nil, errors.New("server error: TopicTerminatedError: Topic was already terminated")
			}
			return &internal.RPCResult{
				Cnx: greenCnx,
				Response: &pb.BaseCommand{
					Type: pb.BaseCommand_PRODUCER_SUCCESS.Enum(),
					ProducerSuccess: &pb.CommandProducerSuccess{
						ProducerName: proto.String("migrated-producer"),
					},
				},
			}, nil
		},
	}

	bo := &fixedBackoffPolicy{delay: 10 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p = &partitionProducer{
		client: &client{
			cnxPool: &migrationMockPool{
				byPhysicalHost: map[string]internal.Connection{
					"proxy:6650":       blueCnx,
					"green-proxy:6650": greenCnx,
				},
			},
			rpcClient: rpc,
			log:       log.DefaultNopLogger(),
			metrics:   internal.NewMetricsProvider(0, map[string]string{}, prometheus.NewRegistry()),
		},
		topic:           "persistent://public/default/migrated-topic",
		producerID:      1,
		options:         &ProducerOptions{DisableBatching: true},
		log:             log.DefaultNopLogger(),
		connectClosedCh: make(chan *connectionClosed, 1),
		pendingQueue:    internal.NewBlockingQueue(10),
		schemaCache:     newSchemaCache(),
		metrics:         newTestMetrics(),
		ctx:             ctx,
		cancelFunc:      cancel,
		backOffPolicyFunc: func() backoff.Policy {
			return bo
		},
	}
	p.setProducerState(producerReady)
	p._setConn(blueCnx)

	p.reconnectToBroker(&connectionClosed{})

	assert.False(t, p.isClosingOrClosed(), "producer must not close when redirected during TopicTerminatedError")
	assert.Equal(t, greenCnx, p._getConn())
}

func TestProducerReconnectRespectsMaxRetryAndBackoffAfterMigration(t *testing.T) {
	greenProxyURL, _ := url.Parse("pulsar://green-proxy:6650")
	greenBrokerURL, _ := url.Parse("pulsar://green-broker-1:6650")
	greenCnx := newMigrationMockConn("green-cnx")

	var attempts atomic.Int32
	rpc := &migrationMockRPCClient{
		lookups: map[string]internal.LookupService{
			"pulsar://green-proxy:6650": &migrationMockLookup{
				proxyURL:   greenProxyURL,
				lookupAddr: greenBrokerURL,
			},
		},
		onRequest: func(
			ctx context.Context, cnx internal.Connection, _ uint64, _ pb.BaseCommand_Type, _ proto.Message,
		) (*internal.RPCResult, error) {
			attempts.Add(1)
			mc := cnx.(*migrationMockConn)
			go func() {
				mc.mu.Lock()
				l := mc.listener
				mc.mu.Unlock()
				// Broker closes producer without changing redirectedClusterURI.
				l.ConnectionClosed(&pb.CommandCloseProducer{ProducerId: proto.Uint64(1)})
			}()
			<-ctx.Done()
			return nil, context.Cause(ctx)
		},
	}

	maxRetry := uint(2)
	bo := &fixedBackoffPolicy{delay: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &partitionProducer{
		client: &client{
			cnxPool: &migrationMockPool{
				byPhysicalHost: map[string]internal.Connection{
					"green-proxy:6650": greenCnx,
				},
			},
			rpcClient: rpc,
			log:       log.DefaultNopLogger(),
			metrics:   internal.NewMetricsProvider(0, map[string]string{}, prometheus.NewRegistry()),
		},
		topic:      "persistent://public/default/migrated-topic",
		producerID: 1,
		options: &ProducerOptions{
			DisableBatching:      true,
			MaxReconnectToBroker: &maxRetry,
		},
		log:             log.DefaultNopLogger(),
		connectClosedCh: make(chan *connectionClosed, 1),
		pendingQueue:    internal.NewBlockingQueue(10),
		schemaCache:     newSchemaCache(),
		metrics:         newTestMetrics(),
		ctx:             ctx,
		cancelFunc:      cancel,
		backOffPolicyFunc: func() backoff.Policy {
			return bo
		},
	}
	p.setProducerState(producerReady)
	p._setConn(greenCnx)
	p.SetRedirectedClusterURI("pulsar://green-proxy:6650")

	start := time.Now()
	p.reconnectToBroker(&connectionClosed{})
	elapsed := time.Since(start)

	assert.Equal(t, int32(2), attempts.Load(),
		"post-migration broker close without a new redirect must consume MaxReconnectToBroker")
	assert.GreaterOrEqual(t, bo.calls.Load(), int32(2),
		"post-migration broker close without a new redirect must apply backoff")
	assert.GreaterOrEqual(t, elapsed, 35*time.Millisecond)
}

func TestConsumerReconnectRespectsMaxRetryAndBackoffAfterMigration(t *testing.T) {
	greenProxyURL, _ := url.Parse("pulsar://green-proxy:6650")
	greenBrokerURL, _ := url.Parse("pulsar://green-broker-1:6650")
	greenCnx := newMigrationMockConn("green-cnx")

	var attempts atomic.Int32
	rpc := &migrationMockRPCClient{
		lookups: map[string]internal.LookupService{
			"pulsar://green-proxy:6650": &migrationMockLookup{
				proxyURL:   greenProxyURL,
				lookupAddr: greenBrokerURL,
			},
		},
		onRequest: func(
			ctx context.Context, cnx internal.Connection, _ uint64, _ pb.BaseCommand_Type, _ proto.Message,
		) (*internal.RPCResult, error) {
			attempts.Add(1)
			mc := cnx.(*migrationMockConn)
			go func() {
				mc.mu.Lock()
				h := mc.consumer
				mc.mu.Unlock()
				// Broker closes consumer without changing redirectedClusterURI.
				h.ConnectionClosed(mc, &pb.CommandCloseConsumer{ConsumerId: proto.Uint64(1)})
			}()
			<-ctx.Done()
			return nil, context.Cause(ctx)
		},
	}

	maxRetry := uint(2)
	bo := &fixedBackoffPolicy{delay: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var parentClosed atomic.Bool
	pc := &partitionConsumer{
		parentConsumer: &reconnectGiveUpStubConsumer{onClose: func() { parentClosed.Store(true) }},
		client: &client{
			cnxPool: &migrationMockPool{
				byPhysicalHost: map[string]internal.Connection{
					"green-proxy:6650": greenCnx,
				},
			},
			rpcClient: rpc,
			log:       log.DefaultNopLogger(),
		},
		topic:      "persistent://public/default/migrated-topic",
		consumerID: 1,
		options: &partitionConsumerOpts{
			subscription:         "sub",
			subscriptionMode:     Durable,
			maxReconnectToBroker: &maxRetry,
		},
		log:                  log.DefaultNopLogger(),
		compressionProviders: sync.Map{},
		connectedCh:          make(chan struct{}, 1),
		connectClosedCh:      make(chan *connectionClosed, 1),
		queueCh:              make(chan []*message, 1),
		clearQueueCh:         make(chan func(id *trackingMessageID)),
		metrics:              newTestMetrics(),
		ctx:                  ctx,
		cancelFunc:           cancel,
		backoffPolicyFunc: func() backoff.Policy {
			return bo
		},
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case cb := <-pc.clearQueueCh:
				cb(nil)
			}
		}
	}()
	pc.setConsumerState(consumerReady)
	pc._setConn(greenCnx)
	pc.SetRedirectedClusterURI("pulsar://green-proxy:6650")

	start := time.Now()
	pc.reconnectToBroker(&connectionClosed{})
	elapsed := time.Since(start)

	assert.Equal(t, int32(2), attempts.Load(),
		"post-migration broker close without a new redirect must consume maxReconnectToBroker")
	assert.GreaterOrEqual(t, bo.calls.Load(), int32(2),
		"post-migration broker close without a new redirect must apply backoff")
	assert.GreaterOrEqual(t, elapsed, 35*time.Millisecond)
	assert.Eventually(t, parentClosed.Load, time.Second, 10*time.Millisecond)
}

type reconnectGiveUpStubConsumer struct {
	Consumer
	onClose func()
}

func (c *reconnectGiveUpStubConsumer) Close() {
	if c.onClose != nil {
		c.onClose()
	}
}
