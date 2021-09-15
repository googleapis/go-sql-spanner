package spannergorm

import (
	"reflect"
	"testing"

	"github.com/cloudspannerecosystem/go-sql-spanner/testutil"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	emptypb "github.com/golang/protobuf/ptypes/empty"
	longrunningpb "google.golang.org/genproto/googleapis/longrunning"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

func TestCreateTable(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	var expectedResponse = &emptypb.Empty{}
	any, _ := ptypes.MarshalAny(expectedResponse)
	server.TestDatabaseAdmin.SetResps([]proto.Message{
		&longrunningpb.Operation{
			Done:   true,
			Result: &longrunningpb.Operation_Response{Response: any},
			Name:   "test-operation",
		},
	})

	if err := db.AutoMigrate(&Singer{}); err != nil {
		t.Fatalf("failed to migrate Singer: %v", err)
	}

	requests := server.TestDatabaseAdmin.Reqs()
	if g, w := len(requests), 1; g != w {
		t.Fatalf("requests count mismatch\nGot: %v\nWant: %v", g, w)
	}
	if req, ok := requests[0].(*databasepb.UpdateDatabaseDdlRequest); ok {
		if g, w := len(req.Statements), 1; g != w {
			t.Fatalf("statement count mismatch\nGot: %v\nWant: %v", g, w)
		}
		query := "CREATE TABLE `singers` (`singer_id` INT64,`first_name` STRING(MAX),`last_name` STRING(MAX),`birth_date` DATE) PRIMARY KEY (`singer_id`)"
		if g, w := req.Statements[0], query; g != w {
			t.Fatalf("statement mismatch\nGot:  %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("request type mismatch, got %v", requests[0])
	}
	// Migrate should not use any transactions.
	reqs := drainRequestsFromServer(server.TestSpanner)
	commitRequests := requestsOfType(reqs, reflect.TypeOf(&sppb.CommitRequest{}))
	if g, w := len(commitRequests), 0; g != w {
		t.Fatalf("commit requests count mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func requestsOfType(requests []interface{}, t reflect.Type) []interface{} {
	res := make([]interface{}, 0)
	for _, req := range requests {
		if reflect.TypeOf(req) == t {
			res = append(res, req)
		}
	}
	return res
}

func drainRequestsFromServer(server testutil.InMemSpannerServer) []interface{} {
	var reqs []interface{}
loop:
	for {
		select {
		case req := <-server.ReceivedRequests():
			reqs = append(reqs, req)
		default:
			break loop
		}
	}
	return reqs
}
