package impl

import (
	log "github.com/sirupsen/logrus"
	"net/url"
	"sync"
)

type ConnectionPool interface {
	GetConnection(logicalAddr *url.URL, physicalAddr *url.URL) (Connection, error)

	// Close all the connections in the pool
	Close()
}

type connectionPool struct {
	pool sync.Map
}

func NewConnectionPool() ConnectionPool {
	return &connectionPool{}
}

func (p *connectionPool) GetConnection(logicalAddr *url.URL, physicalAddr *url.URL) (Connection, error) {
	cachedCnx, found := p.pool.Load(logicalAddr.Host)
	if found {
		cnx := cachedCnx.(*connection)
		log.Debug("Found connection in cache:", cnx.logicalAddr, cnx.physicalAddr)

		if err := cnx.waitUntilReady(); err == nil {
			// Connection is ready to be used
			return cnx, nil
		} else {
			// The cached connection is failed
			p.pool.Delete(logicalAddr)
		}
	}

	// Try to create a new connection
	newCnx, wasCached := p.pool.LoadOrStore(logicalAddr.Host, newConnection(logicalAddr, physicalAddr))
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
