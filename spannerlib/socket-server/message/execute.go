package message

import (
	"bufio"
	"fmt"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"spannerlib/socket-server/protocol"
)

var _ Message = &ExecuteMessage{}

type ExecuteMessage struct {
	message
	Request *spannerpb.ExecuteSqlRequest
}

func CreateExecuteMessage(request *spannerpb.ExecuteSqlRequest) *ExecuteMessage {
	return &ExecuteMessage{
		message: message{messageId: ExecuteMessageId},
		Request: request,
	}
}

func (m *ExecuteMessage) String() string {
	return fmt.Sprintf("ExecuteMessage: %v", m.Request.Sql)
}

func (m *ExecuteMessage) Write(writer *bufio.Writer) error {
	if err := m.writeHeader(writer); err != nil {
		return err
	}
	if err := protocol.WriteExecuteSqlRequest(writer, m.Request); err != nil {
		return err
	}
	return nil
}

func (m *ExecuteMessage) Handle(handler Handler) error {
	return handler.HandleExecute(m)
}
