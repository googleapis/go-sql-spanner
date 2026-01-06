package protocol

import (
	"bufio"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func ReadExecuteBatchRequest(reader *bufio.Reader) (*spannerpb.ExecuteBatchDmlRequest, error) {
	req := &spannerpb.ExecuteBatchDmlRequest{}
	var numStatements int32
	if err := ReadInt32(reader, &numStatements); err != nil {
		return nil, err
	}
	req.Statements = make([]*spannerpb.ExecuteBatchDmlRequest_Statement, numStatements)
	for i := int32(0); i < numStatements; i++ {
		statement := &spannerpb.ExecuteBatchDmlRequest_Statement{}
		if err := ReadString(reader, &statement.Sql); err != nil {
			return nil, err
		}
		params, err := ReadParams(reader)
		if err != nil {
			return nil, err
		}
		statement.Params = params
		paramTypes, err := ReadParamTypes(reader)
		if err != nil {
			return nil, err
		}
		statement.ParamTypes = paramTypes
		req.Statements[i] = statement
	}
	return req, nil
}

func WriteExecuteBatchRequest(writer *bufio.Writer, request *spannerpb.ExecuteBatchDmlRequest) error {
	if err := WriteInt32(writer, int32(len(request.Statements))); err != nil {
		return err
	}
	for _, statement := range request.Statements {
		if err := WriteString(writer, statement.Sql); err != nil {
			return err
		}
		if err := WriteParams(writer, statement.Params); err != nil {
			return err
		}
		if err := WriteParamTypes(writer, statement.ParamTypes); err != nil {
			return err
		}
	}
	return nil
}

func ReadExecuteBatchResponse(reader *bufio.Reader) (*spannerpb.ExecuteBatchDmlResponse, error) {
	res := &spannerpb.ExecuteBatchDmlResponse{}
	var code int32
	if err := ReadInt32(reader, &code); err != nil {
		return nil, err
	}
	if code != int32(codes.OK) {
		var b []byte
		if err := ReadBytes(reader, &b); err != nil {
			return nil, err
		}
		var status *spb.Status
		if err := proto.Unmarshal(b, status); err != nil {
			return nil, err
		}
		res.Status = status
	}
	var numStatements int32
	if err := ReadInt32(reader, &numStatements); err != nil {
		return nil, err
	}
	res.ResultSets = make([]*spannerpb.ResultSet, numStatements)
	for i := int32(0); i < numStatements; i++ {
		var c int64
		if err := ReadInt64(reader, &c); err != nil {
			return nil, err
		}
		res.ResultSets[i] = &spannerpb.ResultSet{
			Stats: &spannerpb.ResultSetStats{
				RowCount: &spannerpb.ResultSetStats_RowCountExact{
					RowCountExact: c,
				},
			},
		}
	}
	return res, nil
}

func WriteExecuteBatchResponse(writer *bufio.Writer, response *spannerpb.ExecuteBatchDmlResponse) error {
	if response.Status != nil {
		if err := WriteInt32(writer, response.Status.Code); err != nil {
			return err
		}
		if response.Status.Code != int32(codes.OK) {
			s, err := proto.Marshal(response.Status)
			if err != nil {
				return err
			}
			if err := WriteBytes(writer, s); err != nil {
				return err
			}
		}
	} else {
		if err := WriteInt32(writer, int32(codes.OK)); err != nil {
			return err
		}
	}
	if err := WriteInt32(writer, int32(len(response.ResultSets))); err != nil {
		return err
	}
	for _, resultSet := range response.ResultSets {
		if err := WriteInt64(writer, resultSet.Stats.GetRowCountExact()); err != nil {
			return err
		}
	}
	return nil
}
