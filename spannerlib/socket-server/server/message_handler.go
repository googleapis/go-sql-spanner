package server

import (
	"context"

	"spannerlib/api"
	"spannerlib/socket-server/message"
	"spannerlib/socket-server/protocol"
)

var _ message.Handler = &messageHandler{}

type messageHandler struct {
	conn *ConnectionHandler
}

func (h *messageHandler) HandleStatus(msg *message.StatusMessage) error {
	return nil
}

func (h *messageHandler) HandleStartup(msg *message.StartupMessage) error {
	ctx := context.Background()
	var err error

	p, ok := h.conn.server.pools.Load(msg.Pool)
	if !ok {
		h.conn.poolId, err = api.CreatePool(ctx, msg.DSN)
		if err != nil {
			return err
		}
		h.conn.server.pools.Store(msg.Pool, h.conn.poolId)
	} else {
		h.conn.poolId = p.(int64)
	}
	h.conn.connId, err = api.CreateConnection(ctx, h.conn.poolId)
	if err != nil {
		return err
	}

	if err := message.OK.Write(h.conn.writer); err != nil {
		return err
	}

	return nil
}

func (h *messageHandler) HandleBegin(msg *message.BeginMessage) error {
	ctx := context.Background()

	if err := api.BeginTransaction(ctx, h.conn.poolId, h.conn.connId, msg.Options); err != nil {
		return err
	}
	if err := message.OK.Write(h.conn.writer); err != nil {
		return err
	}
	return nil
}

func (h *messageHandler) HandleCommit(msg *message.CommitMessage) error {
	ctx := context.Background()

	resp, err := api.Commit(ctx, h.conn.poolId, h.conn.connId)
	if err != nil {
		return err
	}
	result := message.CreateCommitResultMessage(resp)
	if err := result.Write(h.conn.writer); err != nil {
		return err
	}

	return nil
}

func (h *messageHandler) HandleRollback(msg *message.RollbackMessage) error {
	ctx := context.Background()

	if err := api.Rollback(ctx, h.conn.poolId, h.conn.connId); err != nil {
		return err
	}
	if err := message.OK.Write(h.conn.writer); err != nil {
		return err
	}

	return nil
}

func (h *messageHandler) HandleCommitResult(msg *message.CommitResultMessage) error {
	return nil
}

func (h *messageHandler) HandleExecute(msg *message.ExecuteMessage) error {
	ctx := context.Background()

	rows, err := api.Execute(ctx, h.conn.poolId, h.conn.connId, msg.Request)
	if err != nil {
		return err
	}
	defer func() { _ = api.CloseRows(ctx, h.conn.poolId, h.conn.connId, rows) }()

	rowsMsg := message.CreateRowsMessage(rows)
	if err := rowsMsg.Write(h.conn.writer); err != nil {
		return err
	}
	metadata, err := api.Metadata(ctx, h.conn.poolId, h.conn.connId, rows)
	if err != nil {
		return err
	}
	if err := protocol.WriteMetadata(h.conn.writer, metadata); err != nil {
		return err
	}

	for {
		values, err := api.Next(ctx, h.conn.poolId, h.conn.connId, rows)
		if err != nil {
			return err
		}
		if values == nil {
			if err := protocol.WriteBool(h.conn.writer, false); err != nil {
				return err
			}
			break
		}
		if err := protocol.WriteBool(h.conn.writer, true); err != nil {
			return err
		}
		if err := protocol.WriteRow(h.conn.writer, values); err != nil {
			return err
		}
	}
	stats, err := api.ResultSetStats(ctx, h.conn.poolId, h.conn.connId, rows)
	if err != nil {
		return err
	}
	if err := protocol.WriteStats(h.conn.writer, stats); err != nil {
		return err
	}

	return nil
}

func (h *messageHandler) HandleExecuteBatch(msg *message.ExecuteBatchMessage) error {
	ctx := context.Background()

	res, err := api.ExecuteBatch(ctx, h.conn.poolId, h.conn.connId, msg.Request)
	if err != nil {
		return err
	}
	batchResultMsg := message.CreateBatchResultMessage(res)
	if err := batchResultMsg.Write(h.conn.writer); err != nil {
		return err
	}

	return nil
}

func (h *messageHandler) HandleRows(msg *message.RowsMessage) error {
	return nil
}

func (h *messageHandler) HandleBatchResult(msg *message.BatchResultMessage) error {
	return nil
}
