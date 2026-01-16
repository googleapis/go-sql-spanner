package client

import (
	"bufio"
	"net"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"spannerlib/socket-server/message"
)

type Pool struct {
	tp   string
	addr string

	id  string
	dsn string
}

func CreatePool(tp, addr, dsn string) *Pool {
	return CreatePoolWithId(tp, addr, dsn, uuid.New().String())
}

func CreatePoolWithId(tp, addr, dsn, id string) *Pool {
	return &Pool{tp: tp, addr: addr, id: id, dsn: dsn}
}

func (p *Pool) CreateConnection() (*Connection, error) {
	conn, err := net.Dial(p.tp, p.addr)
	if err != nil {
		return nil, err
	}
	connection := &Connection{
		pool:   p,
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
	startup := message.CreateStartupMessage(p.id, p.dsn)
	if err := startup.Write(connection.writer); err != nil {
		return nil, err
	}
	if err := connection.writer.Flush(); err != nil {
		return nil, err
	}
	if msg, err := message.ReadMessageOrError(connection.reader); err != nil {
		_ = connection.Close()
		return nil, err
	} else {
		if _, ok := msg.(*message.StatusMessage); !ok {
			return nil, status.Error(codes.Internal, "message type is not StatusMessage")
		}
	}

	return connection, nil
}
