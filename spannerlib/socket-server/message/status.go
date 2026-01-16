package message

import (
	"bufio"
	"fmt"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"spannerlib/socket-server/protocol"
)

var _ Message = &StatusMessage{}
var OK = &StatusMessage{
	message: message{messageId: StatusMessageId},
	status:  &spb.Status{Code: int32(codes.OK)},
}

type StatusMessage struct {
	message
	status *spb.Status
}

func CreateStatusMessage(err error) *StatusMessage {
	s, _ := status.FromError(err)
	msg := &StatusMessage{
		message: message{messageId: StatusMessageId},
		status:  s.Proto(),
	}
	return msg
}

func (m *StatusMessage) Err() error {
	if m.status == nil {
		return nil
	}
	return status.ErrorProto(m.status)
}

func (m *StatusMessage) String() string {
	return fmt.Sprintf("StatusMessage: %v", m.status)
}

func (m *StatusMessage) Write(writer *bufio.Writer) error {
	if err := m.writeHeader(writer); err != nil {
		return err
	}
	if m.status != nil {
		b, _ := proto.Marshal(m.status)
		if err := protocol.WriteBytes(writer, b); err != nil {
			return err
		}
	}
	return nil
}

func (m *StatusMessage) Handle(handler Handler) error {
	return handler.HandleStatus(m)
}
