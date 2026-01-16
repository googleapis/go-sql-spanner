package message

import (
	"bufio"
	"fmt"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"spannerlib/socket-server/protocol"
)

var _ Message = &CommitResultMessage{}

type CommitResultMessage struct {
	message
	Response *spannerpb.CommitResponse
}

func CreateCommitResultMessage(resp *spannerpb.CommitResponse) *CommitResultMessage {
	return &CommitResultMessage{
		message:  message{messageId: CommitResultMessageId},
		Response: resp,
	}
}

func (m *CommitResultMessage) String() string {
	return fmt.Sprintf("CommitResultMessage: %v", m.Response)
}

func (m *CommitResultMessage) Write(writer *bufio.Writer) error {
	if err := m.writeHeader(writer); err != nil {
		return err
	}
	if err := protocol.WriteCommitResponse(writer, m.Response); err != nil {
		return err
	}
	return nil
}

func (m *CommitResultMessage) Handle(handler Handler) error {
	return handler.HandleCommitResult(m)
}
