package message

import (
	"bufio"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"spannerlib/socket-server/protocol"
)

type Id byte

const StatusMessageId Id = '0'
const StartupMessageId Id = 'S'
const ExecuteMessageId Id = 'E'
const ExecuteBatchMessageId Id = 'B'
const RowsMessageId Id = 'R'
const BatchResultMessageId Id = 'A'

const BeginMessageId Id = 'b'
const CommitMessageId Id = 'c'
const RollbackMessageId Id = 'r'
const CommitResultMessageId Id = 't'

const CloseMessageId Id = 'X'

type Message interface {
	String() string
	MessageId() Id
	Write(writer *bufio.Writer) error

	Handle(handler Handler) error
}

type message struct {
	messageId Id
}

func (m *message) writeHeader(writer *bufio.Writer) error {
	if err := writer.WriteByte(byte(m.messageId)); err != nil {
		return err
	}
	return nil
}

func (m *message) MessageId() Id {
	return m.messageId
}

type CloseMessage struct {
	message
	RowsId int64
}

func ReadMessageOrError(reader *bufio.Reader) (Message, error) {
	msg, err := ReadMessage(reader)
	if err != nil {
		return nil, err
	}
	if msg.MessageId() != StatusMessageId {
		return msg, nil
	}
	s := msg.(*StatusMessage)
	if s.status == nil || s.status.Code == int32(codes.OK) {
		return msg, nil
	}
	return nil, status.ErrorProto(s.status)
}

func ReadMessage(reader *bufio.Reader) (Message, error) {
	var id byte
	if err := protocol.ReadByte(reader, &id); err != nil {
		return nil, err
	}
	msg, err := CreateMessage(Id(id), reader)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func CreateMessage(id Id, reader *bufio.Reader) (Message, error) {
	switch id {
	case StatusMessageId:
		return createStatusMessage(reader)
	case StartupMessageId:
		return createStartupMessage(reader)
	case BeginMessageId:
		return createBeginMessage(reader)
	case CommitMessageId:
		return createCommitMessage(reader)
	case RollbackMessageId:
		return createRollbackMessage(reader)
	case CommitResultMessageId:
		return createCommitResultMessage(reader)
	case ExecuteMessageId:
		return createExecuteMessage(reader)
	case ExecuteBatchMessageId:
		return createExecuteBatchMessage(reader)
	case RowsMessageId:
		return createRowsMessage(reader)
	case BatchResultMessageId:
		return createBatchResultMessage(reader)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown MessageId: %v", id)
	}
}

func createStatusMessage(reader *bufio.Reader) (*StatusMessage, error) {
	msg := &StatusMessage{message: message{messageId: StatusMessageId}}
	var b []byte
	if err := protocol.ReadBytes(reader, &b); err != nil {
		return nil, err
	}
	msg.status = &spb.Status{}
	if err := proto.Unmarshal(b, msg.status); err != nil {
		return nil, err
	}
	return msg, nil
}

func createStartupMessage(reader *bufio.Reader) (*StartupMessage, error) {
	msg := &StartupMessage{message: message{messageId: StartupMessageId}}
	if err := protocol.ReadString(reader, &msg.Pool); err != nil {
		return nil, err
	}
	if err := protocol.ReadString(reader, &msg.DSN); err != nil {
		return nil, err
	}
	return msg, nil
}

func createBeginMessage(reader *bufio.Reader) (*BeginMessage, error) {
	msg := &BeginMessage{}
	options, err := protocol.ReadTransactionOptions(reader)
	if err != nil {
		return nil, err
	}
	msg.Options = options
	return msg, nil
}

func createCommitMessage(reader *bufio.Reader) (*CommitMessage, error) {
	return &CommitMessage{}, nil
}

func createRollbackMessage(reader *bufio.Reader) (*RollbackMessage, error) {
	return &RollbackMessage{}, nil
}

func createCommitResultMessage(reader *bufio.Reader) (*CommitResultMessage, error) {
	msg := &CommitResultMessage{}
	resp, err := protocol.ReadCommitResponse(reader)
	if err != nil {
		return nil, err
	}
	msg.Response = resp

	return msg, nil
}

func createExecuteMessage(reader *bufio.Reader) (*ExecuteMessage, error) {
	msg := &ExecuteMessage{message: message{messageId: ExecuteMessageId}}
	request, err := protocol.ReadExecuteSqlRequest(reader)
	if err != nil {
		return nil, err
	}
	msg.Request = request
	return msg, nil
}

func createExecuteBatchMessage(reader *bufio.Reader) (*ExecuteBatchMessage, error) {
	msg := &ExecuteBatchMessage{message: message{messageId: ExecuteBatchMessageId}}
	request, err := protocol.ReadExecuteBatchRequest(reader)
	if err != nil {
		return nil, err
	}
	msg.Request = request
	return msg, nil
}

func createRowsMessage(reader *bufio.Reader) (*RowsMessage, error) {
	msg := &RowsMessage{message: message{messageId: RowsMessageId}}
	if err := protocol.ReadInt64(reader, &msg.Id); err != nil {
		return nil, err
	}
	return msg, nil
}

func createBatchResultMessage(reader *bufio.Reader) (*BatchResultMessage, error) {
	msg := &BatchResultMessage{message: message{messageId: BatchResultMessageId}}
	resp, err := protocol.ReadExecuteBatchResponse(reader)
	if err != nil {
		return nil, err
	}
	msg.Response = resp
	return msg, nil
}
