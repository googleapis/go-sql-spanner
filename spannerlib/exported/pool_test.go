package exported

import (
	"fmt"
	"testing"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestExecute(t *testing.T) {
	pool := CreatePool("projects/appdev-soda-spanner-staging/instances/knut-test-ycsb/databases/knut-test-db")
	conn := CreateConnection(pool.ObjectId)
	stmt := spannerpb.ExecuteBatchDmlRequest_Statement{
		Sql: "select * from all_types where col_varchar=$1 /*and col_bigint=@id*/ limit 10",
		Params: &structpb.Struct{
			Fields: map[string]*structpb.Value{"p1": {Kind: &structpb.Value_StringValue{StringValue: "61763b0e7feb3ea8fc9e734a6700f6a4"}}},
		},
	}
	stmtBytes, _ := proto.Marshal(&stmt)
	rows := Execute(pool.ObjectId, conn.ObjectId, stmtBytes)
	metadata := Metadata(pool.ObjectId, conn.ObjectId, rows.ObjectId)
	metadataValue := spannerpb.ResultSetMetadata{}
	_ = proto.Unmarshal(metadata.Res, &metadataValue)
	fmt.Printf("Row type: %v\n", metadataValue.RowType)
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

func TestExecuteDml(t *testing.T) {
	pool := CreatePool("projects/appdev-soda-spanner-staging/instances/knut-test-ycsb/databases/knut-test-db")
	conn := CreateConnection(pool.ObjectId)
	txOpts := &spannerpb.TransactionOptions{
		Mode: &spannerpb.TransactionOptions_ReadOnly_{
			ReadOnly: &spannerpb.TransactionOptions_ReadOnly{},
		},
	}
	txOptsBytes, _ := proto.Marshal(txOpts)
	BeginTransaction(pool.ObjectId, conn.ObjectId, txOptsBytes)
	stmt := spannerpb.ExecuteBatchDmlRequest_Statement{
		Sql: "update all_types set col_float8=$1 where col_varchar=$2",
		Params: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"p1": {Kind: &structpb.Value_NumberValue{NumberValue: 3.14}},
				"p2": {Kind: &structpb.Value_StringValue{StringValue: "61763b0e7feb3ea8fc9e734a6700f6a4"}},
			},
		},
	}
	stmtBytes, _ := proto.Marshal(&stmt)
	rows := Execute(pool.ObjectId, conn.ObjectId, stmtBytes)
	if rows.Code != 0 {
		t.Fatalf("failed to execute statement: %s", string(rows.Res))
	}
	metadata := Metadata(pool.ObjectId, conn.ObjectId, rows.ObjectId)
	metadataValue := spannerpb.ResultSetMetadata{}
	_ = proto.Unmarshal(metadata.Res, &metadataValue)
	if len(metadataValue.RowType.Fields) > 0 {
		fmt.Printf("Row type: %v\n", metadataValue.RowType)
	} else {
		rowCount := ResultSetStats(pool.ObjectId, conn.ObjectId, rows.ObjectId)
		fmt.Printf("Update count: %v\n", string(rowCount.Res))
	}
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
