package exported

import "C"
import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"spannerlib/backend"
)

var pools = sync.Map{}
var poolsIdx = atomic.Int64{}

type Pool struct {
	backend        *backend.Pool
	connections    *sync.Map
	connectionsIdx atomic.Int64
}

func CreatePool() *Message {
	backendPool := &backend.Pool{}
	id := poolsIdx.Add(1)
	pool := &Pool{
		backend:     backendPool,
		connections: &sync.Map{},
	}
	pools.Store(id, pool)
	return idMessage(id)
}

func ClosePool(id int64) *Message {
	p, ok := pools.LoadAndDelete(id)
	if !ok {
		return errMessage(fmt.Errorf("pool %v not found", id))
	}
	pool := p.(*Pool)
	pool.connections.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		conn.close()
		return true
	})
	return &Message{}
}

func CreateConnection(poolId int64, project, instance, database string) *Message {
	p, ok := pools.Load(poolId)
	if !ok {
		return errMessage(fmt.Errorf("pool %v not found", poolId))
	}
	pool := p.(*Pool)
	sqlConn, err := pool.backend.Conn(context.Background(), project, instance, database)
	if err != nil {
		return errMessage(err)
	}
	id := poolsIdx.Add(1)
	conn := &Connection{
		backend: &backend.SpannerConnection{Conn: sqlConn},
		results: &sync.Map{},
	}
	pool.connections.Store(id, conn)

	return idMessage(id)
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
