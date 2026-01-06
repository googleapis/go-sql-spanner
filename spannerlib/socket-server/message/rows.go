package message

import (
	"bufio"
	"fmt"

	"spannerlib/socket-server/protocol"
)

var _ Message = &RowsMessage{}

type RowsMessage struct {
	message
	Id int64
}

func CreateRowsMessage(id int64) *RowsMessage {
	return &RowsMessage{
		message: message{messageId: RowsMessageId},
		Id:      id,
	}
}

func (m *RowsMessage) String() string {
	return fmt.Sprintf("RowsMessage: %v", m.Id)
}

func (m *RowsMessage) MessageId() Id {
	return RowsMessageId
}

func (m *RowsMessage) Write(writer *bufio.Writer) error {
	if err := m.writeHeader(writer); err != nil {
		return err
	}
	if err := protocol.WriteInt64(writer, m.Id); err != nil {
		return err
	}
	return nil
}

func (m *RowsMessage) Handle(handler Handler) error {
	return handler.HandleRows(m)
}
