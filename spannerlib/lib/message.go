package lib

import (
	"unsafe"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

func idMessage(id int64) *Message {
	return &Message{ObjectId: id}
}

func errMessage(err error) *Message {
	s := status.Convert(err)
	p := s.Proto()
	// Ignore any marshalling errors and just return an empty status in that case.
	b, _ := proto.Marshal(p)
	return &Message{Code: int32(s.Code()), Res: b}
}
