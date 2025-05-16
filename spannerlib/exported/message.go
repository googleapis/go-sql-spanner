package exported

import (
	"unsafe"

	"github.com/google/uuid"
	"google.golang.org/grpc/status"
)

type Message struct {
	Code     int32
	ObjectId int64
	Res      []byte
}

func (m *Message) Length() int32 {
	if m.Res == nil {
		return 0
	}
	return int32(len(m.Res))
}

func (m *Message) ResPointer() unsafe.Pointer {
	if m.Res == nil {
		return nil
	}
	return unsafe.Pointer(&(m.Res[0]))
}

func generateId() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

type BaseMsg struct {
	Allowed bool
}

type OtherMsg struct {
	BaseMsg
	Foo string
}

func idMessage(id int64) *Message {
	return &Message{ObjectId: id}
}

func errMessage(err error) *Message {
	errCode := status.Code(err)
	b := []byte(err.Error())
	return &Message{Code: int32(errCode), Res: b}
}
