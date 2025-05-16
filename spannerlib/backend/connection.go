package backend

import (
	"database/sql"
)

type SpannerConnection struct {
	Conn *sql.Conn
}

func (conn *SpannerConnection) Close() error {
	return conn.Conn.Close()
}
