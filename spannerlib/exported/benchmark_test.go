package exported

import (
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/proto"
)

func TestExecute(t *testing.T) {
	t.Skip("only for manual testing")

	pool := CreatePool("projects/appdev-soda-spanner-staging/instances/knut-test-probers/databases/prober")
	conn := CreateConnection(pool.ObjectId)
	request := spannerpb.ExecuteSqlRequest{Sql: "select * from all_types limit 10000"}
	requestBytes, err := proto.Marshal(&request)
	if err != nil {
		t.Fatal(err)
	}

	for range 50 {
		rowCount := 0
		start := time.Now()
		rows := Execute(pool.ObjectId, conn.ObjectId, requestBytes)
		for {
			row := Next(pool.ObjectId, conn.ObjectId, rows.ObjectId)
			if row.Length() == 0 {
				break
			}
			rowCount++
		}
		CloseRows(pool.ObjectId, conn.ObjectId, rows.ObjectId)
		end := time.Now()
		fmt.Printf("Execution took %s\n", end.Sub(start))
		if g, w := rowCount, 10000; g != w {
			t.Errorf("row count mismatch: got %d, want %d", g, w)
		}
	}
	CloseConnection(pool.ObjectId, conn.ObjectId)
	ClosePool(pool.ObjectId)
}
