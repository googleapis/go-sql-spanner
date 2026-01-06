package message

import (
	"bufio"
	"fmt"
)

var _ Message = &RollbackMessage{}

type RollbackMessage struct {
	message
}

func CreateRollbackMessage() *RollbackMessage {
	return &RollbackMessage{
		message: message{messageId: RollbackMessageId},
	}
}

func (m *RollbackMessage) String() string {
	return fmt.Sprintf("RollbackMessage")
}

func (m *RollbackMessage) Write(writer *bufio.Writer) error {
	if err := m.writeHeader(writer); err != nil {
		return err
	}
	return nil
}

func (m *RollbackMessage) Handle(handler Handler) error {
	return handler.HandleRollback(m)
}
