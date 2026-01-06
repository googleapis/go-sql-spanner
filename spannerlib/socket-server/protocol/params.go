package protocol

import (
	"bufio"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func ReadParams(reader *bufio.Reader) (*structpb.Struct, error) {
	params := &structpb.Struct{}
	var numParams int32
	if err := ReadInt32(reader, &numParams); err != nil {
		return nil, err
	}
	if numParams > 0 {
		params = &structpb.Struct{}
		params.Fields = map[string]*structpb.Value{}
		for i := 0; i < int(numParams); i++ {
			var name string
			if err := ReadString(reader, &name); err != nil {
				return nil, err
			}
			value, err := ReadValue(reader)
			if err != nil {
				return nil, err
			}
			params.Fields[name] = value
		}
	}
	return params, nil
}

func WriteParams(writer *bufio.Writer, params *structpb.Struct) error {
	if params != nil {
		if err := WriteInt32(writer, int32(len(params.Fields))); err != nil {
			return err
		}
		for key, value := range params.Fields {
			if err := WriteString(writer, key); err != nil {
				return err
			}
			if err := WriteValue(writer, value); err != nil {
				return err
			}
		}
	} else {
		if err := WriteInt32(writer, int32(0)); err != nil {
			return err
		}
	}
	return nil
}

func ReadParamTypes(reader *bufio.Reader) (map[string]*spannerpb.Type, error) {
	paramTypes := map[string]*spannerpb.Type{}
	var numParamTypes int32
	if err := ReadInt32(reader, &numParamTypes); err != nil {
		return nil, err
	}
	if numParamTypes > 0 {
		paramTypes = map[string]*spannerpb.Type{}
		for i := 0; i < int(numParamTypes); i++ {
			var name string
			if err := ReadString(reader, &name); err != nil {
				return nil, err
			}
			tp, err := ReadType(reader)
			if err != nil {
				return nil, err
			}
			paramTypes[name] = tp
		}
	}
	return paramTypes, nil
}

func WriteParamTypes(writer *bufio.Writer, paramTypes map[string]*spannerpb.Type) error {
	if paramTypes != nil {
		if err := WriteInt32(writer, int32(len(paramTypes))); err != nil {
			return err
		}
		for key, tp := range paramTypes {
			if err := WriteString(writer, key); err != nil {
				return err
			}
			if err := WriteType(writer, tp); err != nil {
				return err
			}
		}
	} else {
		if err := WriteInt32(writer, int32(0)); err != nil {
			return err
		}
	}
	return nil
}
