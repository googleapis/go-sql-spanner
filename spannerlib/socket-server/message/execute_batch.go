package message

import (
	"bufio"
	"fmt"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"spannerlib/socket-server/protocol"
)

var _ Message = &ExecuteBatchMessage{}

type ExecuteBatchMessage struct {
	message
	Request *spannerpb.ExecuteBatchDmlRequest
}

func CreateExecuteBatchMessage(request *spannerpb.ExecuteBatchDmlRequest) *ExecuteBatchMessage {
	return &ExecuteBatchMessage{
		message: message{messageId: ExecuteBatchMessageId},
		Request: request,
	}
}

func (m *ExecuteBatchMessage) String() string {
	return fmt.Sprintf("ExecuteBatchMessage: %v", m.Request)
}

func (m *ExecuteBatchMessage) Write(writer *bufio.Writer) error {
	if err := m.writeHeader(writer); err != nil {
		return err
	}
	if err := protocol.WriteExecuteBatchRequest(writer, m.Request); err != nil {
		return err
	}
	return nil
}

func (m *ExecuteBatchMessage) Handle(handler Handler) error {
	return handler.HandleExecuteBatch(m)
}
