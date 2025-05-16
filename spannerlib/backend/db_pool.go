package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	spannerdriver "github.com/googleapis/go-sql-spanner"
)

type Pool struct {
	Project  string
	Instance string
	Database string

	mu      sync.Mutex
	entries map[string]*sql.DB
}

func (pool *Pool) Close() (err error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	for _, db := range pool.entries {
		err = errors.Join(err, db.Close())
	}
	return err
}

func (pool *Pool) Conn(ctx context.Context, project, instance, database string) (*sql.Conn, error) {
	if project == "" {
		project = pool.Project
	}
	if instance == "" {
		instance = pool.Instance
	}
	if database == "" {
		database = pool.Database
	}
	key := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if db, ok := pool.entries[key]; ok {
		return db.Conn(ctx)
	}
	config := spannerdriver.ConnectorConfig{
		Project:  project,
		Instance: instance,
		Database: database,
	}
	connector, err := spannerdriver.CreateConnector(config)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	if pool.entries == nil {
		pool.entries = make(map[string]*sql.DB)
	}
	pool.entries[key] = db
	return db.Conn(ctx)
}
