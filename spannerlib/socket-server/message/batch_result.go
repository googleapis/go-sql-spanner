package message

import (
	"bufio"
	"fmt"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"spannerlib/socket-server/protocol"
)

var _ Message = &BatchResultMessage{}

type BatchResultMessage struct {
	message
	Response *spannerpb.ExecuteBatchDmlResponse
}

func CreateBatchResultMessage(res *spannerpb.ExecuteBatchDmlResponse) *BatchResultMessage {
	return &BatchResultMessage{
		message:  message{messageId: BatchResultMessageId},
		Response: res,
	}
}

func (m *BatchResultMessage) String() string {
	return fmt.Sprintf("BatchResultMessage: %v", m.Response)
}

func (m *BatchResultMessage) MessageId() Id {
	return BatchResultMessageId
}

func (m *BatchResultMessage) Write(writer *bufio.Writer) error {
	if err := m.writeHeader(writer); err != nil {
		return err
	}
	if err := protocol.WriteExecuteBatchResponse(writer, m.Response); err != nil {
		return err
	}
	return nil
}

func (m *BatchResultMessage) Handle(handler Handler) error {
	return handler.HandleBatchResult(m)
}
