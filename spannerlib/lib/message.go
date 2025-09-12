// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lib

import (
	"unsafe"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Message is the generic return type for all functions in the shared library. The exact contents of a Message depends
// on the actual return type for a function.
type Message struct {
	// Code indicates whether the operation succeeded or not and corresponds to a gRPC status code.
	// Zero indicates OK.
	Code int32
	// ObjectId is the ID of the object (e.g. Pool, Connection, Rows) that was created by the function.
	// This field only has a non-zero value if the function actually created an object.
	ObjectId int64
	// Res is any data that was returned by the function. The contents of this byte array is defined by each function.
	// It contains an encoded protobuf Status instance if Code is non-zero.
	Res []byte
}

// Length returns the length of the data in this message. It is only non-zero if the Message actually contains data or
// an error.
func (m *Message) Length() int32 {
	if m.Res == nil {
		return 0
	}
	return int32(len(m.Res))
}

// ResPointer returns a raw pointer to the first byte in the result slice of the Message. The returned pointer is
// therefore a pointer to the actual data. Byte slices in Go are internally represented as a struct with a length and
// capacity field in addition to a pointer to the actual data, so returning a pointer to the byte slice itself would
// not return a pointer to the actual data.
func (m *Message) ResPointer() unsafe.Pointer {
	if m.Res == nil {
		return nil
	}
	return unsafe.Pointer(&(m.Res[0]))
}

// idMessage creates a new Message that only contains an object ID.
func idMessage(id int64) *Message {
	return &Message{ObjectId: id}
}

// errMessage creates a new Message with a non-zero status code and an error. The error is encoded as a protobuf
// Status instance.
func errMessage(err error) *Message {
	s := status.Convert(err)
	p := s.Proto()
	// Ignore any marshalling errors and just return an empty status in that case.
	b, _ := proto.Marshal(p)
	return &Message{Code: int32(s.Code()), Res: b}
}
