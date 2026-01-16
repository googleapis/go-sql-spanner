package message

import (
	"bufio"
	"fmt"
)

var _ Message = &CommitMessage{}

type CommitMessage struct {
	message
}

func CreateCommitMessage() *CommitMessage {
	return &CommitMessage{
		message: message{messageId: CommitMessageId},
	}
}

func (m *CommitMessage) String() string {
	return fmt.Sprintf("CommitMessage")
}

func (m *CommitMessage) Write(writer *bufio.Writer) error {
	if err := m.writeHeader(writer); err != nil {
		return err
	}
	return nil
}

func (m *CommitMessage) Handle(handler Handler) error {
	return handler.HandleCommit(m)
}
