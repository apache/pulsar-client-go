package impl

import (
	"sync"
)

type ConnectionPool interface {
	GetConnection(logicalAddr string, physicalAddr string) (Connection, error)

	// Close all the connections in the pool
	Close()
}

type connectionPool struct {
	pool sync.Map
}

func NewConnectionPool() ConnectionPool {
	return &connectionPool{}
}

func (p *connectionPool) GetConnection(logicalAddr string, physicalAddr string) (Connection, error) {
	cachedCnx, found := p.pool.Load(logicalAddr)
	if found {
		cnx := cachedCnx.(*connection)
		if err := cnx.waitUntilReady(); err != nil {
			// Connection is ready to be used
			return cnx, nil
		} else {
			// The cached connection is failed
			p.pool.Delete(logicalAddr)
		}
	}

	// Try to create a new connection
	newCnx, wasCached := p.pool.LoadOrStore(logicalAddr, newConnection(logicalAddr, physicalAddr))
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
