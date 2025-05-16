package exported

import (
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"testing"
)

func TestExecute(t *testing.T) {
	pool := CreatePool()
	conn := CreateConnection(pool.ObjectId, "appdev-soda-spanner-staging", "knut-test-ycsb", "knut-test-db")
	stmt := spannerpb.ExecuteBatchDmlRequest_Statement{
		Sql: "select * from all_types where col_varchar=$1 /*and col_bigint=@id*/ limit 10",
		Params: &structpb.Struct{
			Fields: map[string]*structpb.Value{"p1": {Kind: &structpb.Value_StringValue{StringValue: "61763b0e7feb3ea8fc9e734a6700f6a4"}}},
		},
	}
	stmtBytes, _ := proto.Marshal(&stmt)
	rows := Execute(pool.ObjectId, conn.ObjectId, stmtBytes)
	for {
		row := Next(pool.ObjectId, conn.ObjectId, rows.ObjectId)
		rowValue := structpb.ListValue{}
		_ = proto.Unmarshal(row.Res, &rowValue)
		if row.Length() == 0 {
			break
		}
		fmt.Printf("row: %v\n", rowValue.Values)
	}
	CloseRows(pool.ObjectId, conn.ObjectId, rows.ObjectId)
	CloseConnection(pool.ObjectId, conn.ObjectId)
	ClosePool(pool.ObjectId)
}
