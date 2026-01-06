package client

import (
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
	"spannerlib/socket-server/protocol"
)

type Rows struct {
	conn *Connection
	id   int64

	metadata *spannerpb.ResultSetMetadata
	stats    *spannerpb.ResultSetStats
}

func (r *Rows) Next() (*structpb.ListValue, error) {
	var hasMoreRows bool
	if err := protocol.ReadBool(r.conn.reader, &hasMoreRows); err != nil {
		return nil, err
	}
	if !hasMoreRows {
		stats, err := protocol.ReadStats(r.conn.reader)
		if err != nil {
			return nil, err
		}
		r.stats = stats
		return nil, nil
	}

	row, err := protocol.ReadRow(r.conn.reader, r.metadata)
	if err != nil {
		return nil, err
	}
	return row, nil
}

func (r *Rows) Close() error {
	if r.stats == nil {
		for {
			row, err := r.Next()
			if err != nil {
				return err
			}
			if row == nil {
				break
			}
		}
	}
	return nil
}
