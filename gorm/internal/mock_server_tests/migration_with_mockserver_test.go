package mock_server_tests

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

func TestCreateTables(t *testing.T) {
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

	if err := db.AutoMigrate(&Singer{}, &Album{}, &AllBasicTypes{}, &AllSpannerNullableTypes{}); err != nil {
		t.Fatalf("failed to migrate Singer: %v", err)
	}

	requests := server.TestDatabaseAdmin.Reqs()
	//TODO: Migrations should use DDL batches.
	if g, w := len(requests), 4; g != w {
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
	if req, ok := requests[1].(*databasepb.UpdateDatabaseDdlRequest); ok {
		if g, w := len(req.Statements), 1; g != w {
			t.Fatalf("statement count mismatch\nGot: %v\nWant: %v", g, w)
		}
		query := "CREATE TABLE `albums` (`album_id` INT64,`singer_id` INT64,`title` STRING(MAX),CONSTRAINT `fk_albums_singer` FOREIGN KEY (`singer_id`) REFERENCES `singers`(`singer_id`)) PRIMARY KEY (`album_id`)"
		if g, w := req.Statements[0], query; g != w {
			t.Fatalf("statement mismatch\nGot:  %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("request type mismatch, got %v", requests[0])
	}
	if req, ok := requests[2].(*databasepb.UpdateDatabaseDdlRequest); ok {
		if g, w := len(req.Statements), 1; g != w {
			t.Fatalf("statement count mismatch\nGot: %v\nWant: %v", g, w)
		}
		query := "CREATE TABLE `all_basic_types` (`id` INT64,`int64` INT64,`float64` FLOAT64,`numeric` NUMERIC,`string` STRING(MAX),`bytes` BYTES(MAX),`date` DATE,`timestamp` TIMESTAMP,`json` JSON) PRIMARY KEY (`id`)"
		if g, w := req.Statements[0], query; g != w {
			t.Fatalf("statement mismatch\nGot:  %v\nWant: %v", g, w)
		}
	} else {
		t.Fatalf("request type mismatch, got %v", requests[0])
	}
	if req, ok := requests[3].(*databasepb.UpdateDatabaseDdlRequest); ok {
		if g, w := len(req.Statements), 1; g != w {
			t.Fatalf("statement count mismatch\nGot: %v\nWant: %v", g, w)
		}
		query := "CREATE TABLE `all_spanner_nullable_types` (`id` INT64,`int64` INT64,`float64` FLOAT64,`numeric` NUMERIC,`string` STRING(MAX),`bytes` BYTES(MAX),`date` DATE,`timestamp` TIMESTAMP,`json` JSON) PRIMARY KEY (`id`)"
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
