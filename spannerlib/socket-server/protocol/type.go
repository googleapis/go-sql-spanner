package protocol

import (
	"bufio"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
)

func ReadType(reader *bufio.Reader) (*spannerpb.Type, error) {
	tp := &spannerpb.Type{}
	var code int32
	if err := ReadInt32(reader, &code); err != nil {
		return nil, err
	}
	tp.Code = spannerpb.TypeCode(code)
	if code == int32(spannerpb.TypeCode_ARRAY) {
		elementType, err := ReadType(reader)
		if err != nil {
			return nil, err
		}
		tp.ArrayElementType = elementType
	}
	return tp, nil
}

func WriteType(writer *bufio.Writer, tp *spannerpb.Type) error {
	if err := WriteInt32(writer, int32(tp.Code)); err != nil {
		return err
	}
	if tp.Code == spannerpb.TypeCode_ARRAY {
		if err := WriteInt32(writer, int32(tp.ArrayElementType.Code)); err != nil {
			return err
		}
	}
	return nil
}
