package protocol

import (
	"bufio"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
)

func ReadExecuteSqlRequest(reader *bufio.Reader) (*spannerpb.ExecuteSqlRequest, error) {
	req := &spannerpb.ExecuteSqlRequest{}
	if err := ReadString(reader, &req.Sql); err != nil {
		return nil, err
	}
	params, err := ReadParams(reader)
	if err != nil {
		return nil, err
	}
	req.Params = params
	paramTypes, err := ReadParamTypes(reader)
	if err != nil {
		return nil, err
	}
	req.ParamTypes = paramTypes
	return req, nil
}

func WriteExecuteSqlRequest(writer *bufio.Writer, request *spannerpb.ExecuteSqlRequest) error {
	if err := WriteString(writer, request.Sql); err != nil {
		return err
	}
	if err := WriteParams(writer, request.Params); err != nil {
		return err
	}
	if err := WriteParamTypes(writer, request.ParamTypes); err != nil {
		return err
	}
	return nil
}
