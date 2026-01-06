package protocol

import (
	"bufio"
	"encoding/binary"
	"io"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func StringLength(s string) uint32 {
	return 4 + uint32(len([]byte(s)))
}

func ReadBool(reader *bufio.Reader, b *bool) error {
	if err := binary.Read(reader, binary.BigEndian, b); err != nil {
		return err
	}
	return nil
}

func WriteBool(writer *bufio.Writer, b bool) error {
	if err := binary.Write(writer, binary.BigEndian, b); err != nil {
		return err
	}
	return nil
}

func ReadInt32(reader *bufio.Reader, n *int32) error {
	if err := binary.Read(reader, binary.BigEndian, n); err != nil {
		return err
	}
	return nil
}

func WriteInt32(writer *bufio.Writer, value int32) error {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		return err
	}
	return nil
}

func ReadUInt32(reader *bufio.Reader, n *uint32) error {
	if err := binary.Read(reader, binary.BigEndian, n); err != nil {
		return err
	}
	return nil
}

func WriteUInt32(writer *bufio.Writer, value uint32) error {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		return err
	}
	return nil
}

func ReadInt64(reader *bufio.Reader, n *int64) error {
	if err := binary.Read(reader, binary.BigEndian, n); err != nil {
		return err
	}
	return nil
}

func WriteInt64(writer *bufio.Writer, value int64) error {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		return err
	}
	return nil
}

func ReadFloat64(reader *bufio.Reader, n *float64) error {
	if err := binary.Read(reader, binary.BigEndian, n); err != nil {
		return err
	}
	return nil
}

func WriteFloat64(writer *bufio.Writer, value float64) error {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		return err
	}
	return nil
}

func ReadString(reader *bufio.Reader, s *string) error {
	var l uint32
	if err := binary.Read(reader, binary.BigEndian, &l); err != nil {
		return err
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return err
	}
	*s = string(buf)
	return nil
}

func WriteString(writer *bufio.Writer, s string) error {
	l := uint32(len([]byte(s)))
	if err := binary.Write(writer, binary.BigEndian, l); err != nil {
		return err
	}
	if _, err := writer.WriteString(s); err != nil {
		return err
	}
	return nil
}

func ReadByte(reader *bufio.Reader, b *byte) error {
	if err := binary.Read(reader, binary.BigEndian, b); err != nil {
		return err
	}
	return nil
}

func WriteByte(writer *bufio.Writer, b byte) error {
	if err := binary.Write(writer, binary.BigEndian, b); err != nil {
		return err
	}
	return nil
}

func ReadBytes(reader *bufio.Reader, b *[]byte) error {
	var l uint32
	if err := binary.Read(reader, binary.BigEndian, &l); err != nil {
		return err
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return err
	}
	*b = buf
	return nil
}

func WriteBytes(writer *bufio.Writer, b []byte) error {
	l := uint32(len(b))
	if err := binary.Write(writer, binary.BigEndian, l); err != nil {
		return err
	}
	if _, err := writer.Write(b); err != nil {
		return err
	}
	return nil
}

func ReadStats(reader *bufio.Reader) (*spannerpb.ResultSetStats, error) {
	c := &spannerpb.ResultSetStats_RowCountExact{}
	if err := binary.Read(reader, binary.BigEndian, &c.RowCountExact); err != nil {
		return nil, err
	}
	return &spannerpb.ResultSetStats{RowCount: c}, nil
}

func WriteStats(writer *bufio.Writer, stats *spannerpb.ResultSetStats) error {
	var c int64
	switch stats.GetRowCount().(type) {
	case *spannerpb.ResultSetStats_RowCountExact:
		c = stats.GetRowCountExact()
	case *spannerpb.ResultSetStats_RowCountLowerBound:
		c = stats.GetRowCountLowerBound()
	}
	if err := binary.Write(writer, binary.BigEndian, c); err != nil {
		return err
	}
	return nil
}

func ReadRow(reader *bufio.Reader, metadata *spannerpb.ResultSetMetadata) (*structpb.ListValue, error) {
	row := &structpb.ListValue{}
	row.Values = make([]*structpb.Value, len(metadata.RowType.Fields))
	for i := range metadata.RowType.Fields {
		value, err := ReadValue(reader)
		if err != nil {
			return nil, err
		}
		row.Values[i] = value
	}
	return row, nil
}

func WriteRow(writer *bufio.Writer, values *structpb.ListValue) error {
	for _, value := range values.Values {
		if err := WriteValue(writer, value); err != nil {
			return err
		}
	}
	return nil
}

type ValueType byte

const (
	ValueTypeNull ValueType = iota
	ValueTypeBool
	ValueTypeNumber
	ValueTypeString
	ValueTypeList
	ValueTypeStruct
)

func ReadValue(reader *bufio.Reader) (*structpb.Value, error) {
	var tp byte
	if err := ReadByte(reader, &tp); err != nil {
		return nil, err
	}

	valueType := ValueType(tp)
	switch valueType {
	case ValueTypeNull:
		return structpb.NewNullValue(), nil
	case ValueTypeBool:
		var b bool
		if err := binary.Read(reader, binary.BigEndian, &b); err != nil {
			return nil, err
		}
		return structpb.NewBoolValue(b), nil
	case ValueTypeNumber:
		var f float64
		if err := binary.Read(reader, binary.BigEndian, &f); err != nil {
			return nil, err
		}
		return structpb.NewNumberValue(f), nil
	case ValueTypeString:
		var s string
		if err := ReadString(reader, &s); err != nil {
			return nil, err
		}
		return structpb.NewStringValue(s), nil
	case ValueTypeList:
		var num int32
		if err := binary.Read(reader, binary.BigEndian, &num); err != nil {
			return nil, err
		}
		lv := &structpb.ListValue{Values: make([]*structpb.Value, num)}
		for i := 0; i < int(num); i++ {
			v, err := ReadValue(reader)
			if err != nil {
				return nil, err
			}
			lv.Values[i] = v
		}
		return structpb.NewListValue(lv), nil
	case ValueTypeStruct:
		return nil, status.Errorf(codes.Unimplemented, "struct type not yet implemented")
	default:
		return nil, status.Errorf(codes.Internal, "unknown type: %v", valueType)
	}
}

func WriteValue(writer *bufio.Writer, value *structpb.Value) error {
	switch value.Kind.(type) {
	case *structpb.Value_NullValue:
		if err := WriteByte(writer, 0); err != nil {
			return err
		}
	case *structpb.Value_BoolValue:
		if err := WriteByte(writer, 1); err != nil {
			return err
		}
		if err := WriteBool(writer, value.GetBoolValue()); err != nil {
			return err
		}
	case *structpb.Value_NumberValue:
		if err := WriteByte(writer, 2); err != nil {
			return err
		}
		if err := WriteFloat64(writer, value.GetNumberValue()); err != nil {
			return err
		}
	case *structpb.Value_StringValue:
		if err := WriteByte(writer, 3); err != nil {
			return err
		}
		if err := WriteString(writer, value.GetStringValue()); err != nil {
			return err
		}
	case *structpb.Value_ListValue:
		if err := WriteByte(writer, 4); err != nil {
			return err
		}
		// Write the number of values and then each value.
		if err := WriteInt32(writer, int32(len(value.GetListValue().GetValues()))); err != nil {
			return err
		}
		for _, value := range value.GetListValue().GetValues() {
			if err := WriteValue(writer, value); err != nil {
				return err
			}
		}
	case *structpb.Value_StructValue:
		return status.Errorf(codes.Unimplemented, "struct value not yet supported")
	}
	return nil
}
