package message

import (
	"bufio"
	"fmt"

	"spannerlib/socket-server/protocol"
)

var _ Message = &StartupMessage{}

type StartupMessage struct {
	message
	Pool string
	DSN  string
}

func CreateStartupMessage(pool, dsn string) *StartupMessage {
	return &StartupMessage{
		message: message{messageId: StartupMessageId},
		Pool:    pool,
		DSN:     dsn,
	}
}

func (m *StartupMessage) String() string {
	return fmt.Sprintf("StartupMessage: %v", m.DSN)
}

func (m *StartupMessage) Write(writer *bufio.Writer) error {
	if err := m.writeHeader(writer); err != nil {
		return err
	}
	if err := protocol.WriteString(writer, m.Pool); err != nil {
		return err
	}
	if err := protocol.WriteString(writer, m.DSN); err != nil {
		return err
	}
	return nil
}

func (m *StartupMessage) Handle(handler Handler) error {
	return handler.HandleStartup(m)
}
