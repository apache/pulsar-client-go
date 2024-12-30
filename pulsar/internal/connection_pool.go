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
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/auth"

	"github.com/apache/pulsar-client-go/pulsar/log"
)

// ConnectionPool is a interface of connection pool.
type ConnectionPool interface {
	// GetConnection get a connection from ConnectionPool.
	GetConnection(logicalAddr *url.URL, physicalAddr *url.URL, keySuffix int32) (Connection, error)

	// GetConnections get all connections in the pool.
	GetConnections() map[string]Connection

	// GenerateRoundRobinIndex generates a round-robin index.
	GenerateRoundRobinIndex() uint32

	// Close all the connections in the pool
	Close()
}

type connectionPool struct {
	sync.Mutex
	connections           map[string]*connection
	connectionTimeout     time.Duration
	tlsOptions            *TLSOptions
	auth                  auth.Provider
	maxConnectionsPerHost uint32
	roundRobinCnt         uint32
	keepAliveInterval     time.Duration
	closeCh               chan struct{}

	metrics     *Metrics
	log         log.Logger
	description string
}

// NewConnectionPool init connection pool.
func NewConnectionPool(
	tlsOptions *TLSOptions,
	auth auth.Provider,
	connectionTimeout time.Duration,
	keepAliveInterval time.Duration,
	maxConnectionsPerHost int,
	logger log.Logger,
	metrics *Metrics,
	description string,
	connectionMaxIdleTime time.Duration) ConnectionPool {
	p := &connectionPool{
		connections:           make(map[string]*connection),
		tlsOptions:            tlsOptions,
		auth:                  auth,
		connectionTimeout:     connectionTimeout,
		maxConnectionsPerHost: uint32(maxConnectionsPerHost),
		keepAliveInterval:     keepAliveInterval,
		log:                   logger,
		metrics:               metrics,
		closeCh:               make(chan struct{}),
		description:           description,
	}
	go p.checkAndCleanIdleConnections(connectionMaxIdleTime)
	return p
}

func (p *connectionPool) GetConnection(logicalAddr *url.URL, physicalAddr *url.URL,
	keySuffix int32) (Connection, error) {
	p.log.WithField("logicalAddr", logicalAddr).
		WithField("physicalAddr", physicalAddr).
		WithField("keySuffix", keySuffix).Debug("Getting pooled connection")
	key := fmt.Sprint(logicalAddr.Host, "-", physicalAddr.Host, "-", keySuffix)

	p.Lock()
	conn, ok := p.connections[key]
	if ok {
		p.log.Debugf("Found connection in pool key=%s logical_addr=%+v physical_addr=%+v",
			key, conn.logicalAddr, conn.physicalAddr)

		// When the current connection is in a closed state or the broker actively notifies that the
		// current connection is closed, we need to remove the connection object from the current
		// connection pool and create a new connection.
		if conn.closed() {
			p.log.Debugf("Removed connection from pool key=%s logical_addr=%+v physical_addr=%+v",
				key, conn.logicalAddr, conn.physicalAddr)
			delete(p.connections, key)
			conn.Close()
			conn = nil // set to nil so we create a new one
		}
	}

	if conn == nil {
		conn = newConnection(connectionOptions{
			logicalAddr:       logicalAddr,
			physicalAddr:      physicalAddr,
			tls:               p.tlsOptions,
			connectionTimeout: p.connectionTimeout,
			auth:              p.auth,
			keepAliveInterval: p.keepAliveInterval,
			logger:            p.log,
			metrics:           p.metrics,
			description:       p.description,
		})
		p.connections[key] = conn
		p.Unlock()
		conn.start()
	} else {
		conn.ResetLastActive()
		// we already have a connection
		p.Unlock()
	}

	err := conn.waitUntilReady()
	return conn, err
}

func (p *connectionPool) GetConnections() map[string]Connection {
	p.Lock()
	conns := make(map[string]Connection)
	for k, c := range p.connections {
		conns[k] = c
	}
	p.Unlock()
	return conns
}

func (p *connectionPool) GenerateRoundRobinIndex() uint32 {
	return atomic.AddUint32(&p.roundRobinCnt, 1) % p.maxConnectionsPerHost
}

func (p *connectionPool) Close() {
	p.Lock()
	close(p.closeCh)
	for k, c := range p.connections {
		delete(p.connections, k)
		c.Close()
	}
	p.Unlock()
}

func (p *connectionPool) checkAndCleanIdleConnections(maxIdleTime time.Duration) {
	if maxIdleTime < 0 {
		return
	}
	for {
		select {
		case <-p.closeCh:
			return
		case <-time.After(maxIdleTime):
			p.Lock()
			for k, c := range p.connections {
				if c.CheckIdle(maxIdleTime) {
					p.log.Debugf("Closed connection from pool due to inactivity. logical_addr=%+v physical_addr=%+v",
						c.logicalAddr, c.physicalAddr)
					delete(p.connections, k)
					c.Close()
				}
			}
			p.Unlock()
		}
	}
}
