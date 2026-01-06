package message

import (
	"bufio"
	"fmt"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"spannerlib/socket-server/protocol"
)

var _ Message = &BeginMessage{}

type BeginMessage struct {
	message
	Options *spannerpb.TransactionOptions
}

func CreateBeginMessage(options *spannerpb.TransactionOptions) *BeginMessage {
	return &BeginMessage{
		message: message{messageId: BeginMessageId},
		Options: options,
	}
}

func (m *BeginMessage) String() string {
	return fmt.Sprintf("BeginMessage: %v", m.Options)
}

func (m *BeginMessage) Write(writer *bufio.Writer) error {
	if err := m.writeHeader(writer); err != nil {
		return err
	}
	if err := protocol.WriteTransactionOptions(writer, m.Options); err != nil {
		return err
	}
	return nil
}

func (m *BeginMessage) Handle(handler Handler) error {
	return handler.HandleBegin(m)
}
