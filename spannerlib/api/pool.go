package api

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"spannerlib/backend"
)

var pools = sync.Map{}
var poolsIdx = atomic.Int64{}

// Pool is the equivalent of a sql.DB. It contains a pool of connections to the same database.
type Pool struct {
	backend        *backend.Pool
	connections    *sync.Map
	connectionsIdx atomic.Int64
}

func CreatePool(dsn string) (int64, error) {
	backendPool, err := backend.CreatePool(dsn)
	if err != nil {
		return 0, err
	}
	id := poolsIdx.Add(1)
	pool := &Pool{
		backend:     backendPool,
		connections: &sync.Map{},
	}
	pools.Store(id, pool)
	return id, nil
}

func ClosePool(id int64) error {
	p, ok := pools.LoadAndDelete(id)
	if !ok {
		return fmt.Errorf("pool %v not found", id)
	}
	pool := p.(*Pool)
	pool.connections.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		conn.close()
		return true
	})
	if err := pool.backend.Close(); err != nil {
		return err
	}
	return nil
}

func CreateConnection(poolId int64) (int64, error) {
	p, ok := pools.Load(poolId)
	if !ok {
		return 0, fmt.Errorf("pool %v not found", poolId)
	}
	pool := p.(*Pool)
	sqlConn, err := pool.backend.Conn(context.Background())
	if err != nil {
		return 0, err
	}
	id := poolsIdx.Add(1)
	conn := &Connection{
		backend:      &backend.SpannerConnection{Conn: sqlConn},
		results:      &sync.Map{},
		transactions: &sync.Map{},
	}
	pool.connections.Store(id, conn)

	return id, nil
}

func findConnection(poolId, connId int64) (*Connection, error) {
	p, ok := pools.Load(poolId)
	if !ok {
		return nil, fmt.Errorf("pool %v not found", poolId)
	}
	pool := p.(*Pool)
	c, ok := pool.connections.Load(connId)
	if !ok {
		return nil, fmt.Errorf("connection %v not found", connId)
	}
	conn := c.(*Connection)
	return conn, nil
}

func findRows(poolId, connId, rowsId int64) (*rows, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return nil, err
	}
	r, ok := conn.results.Load(rowsId)
	if !ok {
		return nil, fmt.Errorf("rows %v not found", rowsId)
	}
	res := r.(*rows)
	return res, nil
}

func findTx(poolId, connId, txId int64) (*transaction, error) {
	conn, err := findConnection(poolId, connId)
	if err != nil {
		return nil, err
	}
	r, ok := conn.transactions.Load(txId)
	if !ok {
		return nil, fmt.Errorf("tx %v not found", txId)
	}
	res := r.(*transaction)
	return res, nil
}
