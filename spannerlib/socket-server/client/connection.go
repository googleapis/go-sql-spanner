package client

import (
	"bufio"
	"net"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"spannerlib/socket-server/message"
	"spannerlib/socket-server/protocol"
)

type Connection struct {
	pool *Pool

	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func (c *Connection) Begin(options *spannerpb.TransactionOptions) error {
	msg := message.CreateBeginMessage(options)
	if err := msg.Write(c.writer); err != nil {
		return err
	}
	if err := c.writer.Flush(); err != nil {
		return err
	}
	_, err := message.ReadMessageOrError(c.reader)
	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) Commit() (*spannerpb.CommitResponse, error) {
	msg := message.CreateCommitMessage()
	if err := msg.Write(c.writer); err != nil {
		return nil, err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, err
	}
	m, err := message.ReadMessageOrError(c.reader)
	if err != nil {
		return nil, err
	}
	result, ok := m.(*message.CommitResultMessage)
	if !ok {
		return nil, status.Error(codes.Internal, "message is not a CommitResultMessage")
	}

	return result.Response, nil
}

func (c *Connection) Rollback() error {
	msg := message.CreateRollbackMessage()
	if err := msg.Write(c.writer); err != nil {
		return err
	}
	if err := c.writer.Flush(); err != nil {
		return err
	}
	_, err := message.ReadMessageOrError(c.reader)
	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) Execute(request *spannerpb.ExecuteSqlRequest) (*Rows, error) {
	msg := message.CreateExecuteMessage(request)
	if err := msg.Write(c.writer); err != nil {
		return nil, err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, err
	}
	r, err := message.ReadMessageOrError(c.reader)
	if err != nil {
		return nil, err
	}
	rowsMsg, ok := r.(*message.RowsMessage)
	if !ok {
		return nil, status.Error(codes.Internal, "message type is not RowsMessage")
	}
	metadata, err := protocol.ReadMetadata(c.reader)
	if err != nil {
		return nil, err
	}
	return &Rows{
		conn:     c,
		id:       rowsMsg.Id,
		metadata: metadata,
	}, nil
}

func (c *Connection) ExecuteBatch(request *spannerpb.ExecuteBatchDmlRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
	msg := message.CreateExecuteBatchMessage(request)
	if err := msg.Write(c.writer); err != nil {
		return nil, err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, err
	}
	r, err := message.ReadMessageOrError(c.reader)
	if err != nil {
		return nil, err
	}
	batchMsg, ok := r.(*message.BatchResultMessage)
	if !ok {
		return nil, status.Error(codes.Internal, "message type is not BatchResultMessage")
	}
	return batchMsg.Response, nil
}

func (c *Connection) Close() error {
	if err := c.conn.Close(); err != nil {
		return err
	}
	c.reader = nil
	c.writer = nil
	c.conn = nil
	return nil
}
