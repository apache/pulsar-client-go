//
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
//

package internal

import (
	"github.com/apache/pulsar-client-go/pkg/log"
	"net/url"
	"github.com/apache/pulsar-client-go/pkg/auth"
	"sync"
)

type ConnectionPool interface {
	GetConnection(logicalAddr *url.URL, physicalAddr *url.URL) (Connection, error)

	// Close all the connections in the pool
	Close()
}

type connectionPool struct {
	pool       sync.Map
	tlsOptions *TLSOptions
	auth       auth.Provider
}

func NewConnectionPool(tlsOptions *TLSOptions, auth auth.Provider) ConnectionPool {
	return &connectionPool{
		tlsOptions: tlsOptions,
		auth:       auth,
	}
}

func (p *connectionPool) GetConnection(logicalAddr *url.URL, physicalAddr *url.URL) (Connection, error) {
	cachedCnx, found := p.pool.Load(logicalAddr.Host)
	if found {
		cnx := cachedCnx.(*connection)
		log.Debugf("found connection in cache, logicalAddr is: &v, physicalAddr is: %v", cnx.logicalAddr, cnx.physicalAddr)

		if err := cnx.waitUntilReady(); err == nil {
			// Connection is ready to be used
			return cnx, nil
		} else {
			// The cached connection is failed
			p.pool.Delete(logicalAddr.Host)
			log.Debugf("removed failed connection from pool, logicalAddr is: &v, physicalAddr is: %v", cnx.logicalAddr, cnx.physicalAddr)
		}
	}

	// Try to create a new connection
	newCnx, wasCached := p.pool.LoadOrStore(logicalAddr.Host,
		newConnection(logicalAddr, physicalAddr, p.tlsOptions, p.auth))
	cnx := newCnx.(*connection)
	if !wasCached {
		cnx.start()
	}

	if err := cnx.waitUntilReady(); err != nil {
		return nil, err
	} else {
		return cnx, nil
	}
}

func (p *connectionPool) Close() {
	p.pool.Range(func(key, value interface{}) bool {
		value.(Connection).Close()
		return true
	})
}
