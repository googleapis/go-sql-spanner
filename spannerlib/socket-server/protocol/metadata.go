package protocol

import (
	"bufio"
	"encoding/binary"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
)

func ReadMetadata(reader *bufio.Reader) (*spannerpb.ResultSetMetadata, error) {
	metadata := &spannerpb.ResultSetMetadata{}
	var numFields int32
	if err := binary.Read(reader, binary.BigEndian, &numFields); err != nil {
		return nil, err
	}
	metadata.RowType = &spannerpb.StructType{}
	metadata.RowType.Fields = make([]*spannerpb.StructType_Field, numFields)
	for i := 0; i < int(numFields); i++ {
		metadata.RowType.Fields[i] = &spannerpb.StructType_Field{Type: &spannerpb.Type{}}
		var code int32
		if err := binary.Read(reader, binary.BigEndian, &code); err != nil {
			return nil, err
		}
		metadata.RowType.Fields[i].Type.Code = spannerpb.TypeCode(code)
		if code == int32(spannerpb.TypeCode_ARRAY) {
			var elementCode int32
			if err := binary.Read(reader, binary.BigEndian, &elementCode); err != nil {
				return nil, err
			}
			metadata.RowType.Fields[i].Type.ArrayElementType = &spannerpb.Type{Code: spannerpb.TypeCode(elementCode)}
		}
		if err := ReadString(reader, &metadata.RowType.Fields[i].Name); err != nil {
			return nil, err
		}
	}
	return metadata, nil
}

func WriteMetadata(writer *bufio.Writer, metadata *spannerpb.ResultSetMetadata) error {
	if err := WriteInt32(writer, int32(len(metadata.RowType.Fields))); err != nil {
		return err
	}
	for _, field := range metadata.RowType.Fields {
		if err := WriteType(writer, field.Type); err != nil {
			return err
		}
		if err := WriteString(writer, field.Name); err != nil {
			return err
		}
	}
	return nil
}
