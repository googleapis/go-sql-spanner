package backend

import (
	"context"
	"database/sql"

	spannerdriver "github.com/googleapis/go-sql-spanner"
)

// Pool is a simple wrapper around sql.DB and contains a pool of connections.
type Pool struct {
	db *sql.DB
}

func CreatePool(dsn string) (*Pool, error) {
	config, err := spannerdriver.ExtractConnectorConfig(dsn)
	if err != nil {
		return nil, err
	}
	connector, err := spannerdriver.CreateConnector(config)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	pool := &Pool{
		db: db,
	}
	return pool, nil
}

func (pool *Pool) Close() (err error) {
	return pool.db.Close()
}

func (pool *Pool) Conn(ctx context.Context) (*sql.Conn, error) {
	return pool.db.Conn(ctx)
}
