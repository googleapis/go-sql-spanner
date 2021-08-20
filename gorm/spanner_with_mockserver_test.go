package spannergorm

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/go-sql-spanner/testutil"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"google.golang.org/api/option"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"gorm.io/gorm"
)

type Singer struct {
	SingerId  int64 `gorm:"primaryKey,autoIncrement:false"`
	FirstName *string
	LastName  string
	BirthDate spanner.NullDate `gorm:"type:DATE"`
}

type Album struct {
	AlbumId  int64 `gorm:"primaryKey,autoIncrement:false"`
	SingerId int64
	Singer   Singer
	Title    string
}

func TestSelectOneSinger(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	firstName := "Pete"
	server.TestSpanner.PutStatementResult(
		"SELECT * FROM `singers` WHERE `singers`.`singer_id` = @p1 LIMIT 1",
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: createSingersResultSet([]Singer{{1, &firstName, "Allison", spanner.NullDate{}}}),
		})

	var singer Singer
	if err := db.Take(&singer, 1).Error; err != nil {
		t.Fatalf("failed to fetch singer: %v", err)
	}
	if singer.SingerId != 1 {
		t.Fatalf("SingerId mismatch\nGot: %v\nWant: %v", singer.SingerId, 1)
	}
	if *singer.FirstName != "Pete" {
		t.Fatalf("Singer first name mismatch\nGot: %v\nWant: %v", singer.FirstName, "Pete")
	}
	if singer.LastName != "Allison" {
		t.Fatalf("Singer last name mismatch\nGot: %v\nWant: %v", singer.LastName, "Allison")
	}
	if singer.BirthDate.Valid {
		t.Fatalf("Singer birthdate is not null")
	}
}

func TestSelectMultipleSingers(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	firstName := "Pete"
	server.TestSpanner.PutStatementResult(
		"SELECT * FROM `singers` WHERE `singers`.`singer_id` IN (@p1,@p2)",
		&testutil.StatementResult{
			Type: testutil.StatementResultResultSet,
			ResultSet: createSingersResultSet([]Singer{
				{1, &firstName, "Allison", spanner.NullDate{}},
				{2, nil, "Anderson", spanner.NullDate{Valid: true, Date: civil.Date{2000, 1, 1}}},
			}),
		})

	var singers []Singer
	if err := db.Find(&singers, []int64{1, 2}).Error; err != nil {
		t.Fatalf("failed to fetch singers: %v", err)
	}
	if len(singers) != 2 {
		t.Fatalf("singer count mismatch\nGot: %v\nWant: %v", len(singers), 2)
	}
	for i, singer := range singers {
		if singer.SingerId != int64(i+1) {
			t.Fatalf("SingerId mismatch\nGot: %v\nWant: %v", singer.SingerId, i+1)
		}
		if i == 0 {
			if *singer.FirstName != "Pete" {
				t.Fatalf("Singer first name mismatch\nGot: %v\nWant: %v", singer.FirstName, "Pete")
			}
			if singer.BirthDate.Valid {
				t.Fatalf("Singer birthdate is not null")
			}
		} else {
			if singer.FirstName != nil {
				t.Fatalf("Singer first name mismatch\nGot: %v\nWant: %v", *singer.FirstName, nil)
			}
			if !singer.BirthDate.Valid {
				t.Fatalf("Singer birthdate is null")
			}
			if singer.BirthDate.Date.String() != "2000-01-01" {
				t.Fatalf("Singer birthdate mismatch\nGot: %v\nWant: %v", singer.BirthDate.Date, "2000-01-01")
			}
		}
	}
}

func TestCreateSinger(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	server.TestSpanner.PutStatementResult(
		"INSERT INTO `singers` (`singer_id`,`first_name`,`last_name`,`birth_date`) VALUES (@p1,@p2,@p3,@p4)",
		&testutil.StatementResult{
			Type:        testutil.StatementResultUpdateCount,
			UpdateCount: 1,
		})

	res := db.Create(&Singer{
		SingerId:  1,
		FirstName: strPointer("Pete"),
		LastName:  "Allison",
		BirthDate: spanner.NullDate{Date: civil.Date{Year: 1998, Month: 4, Day: 23}, Valid: true},
	})
	if res.Error != nil {
		t.Fatalf("failed to create new singer: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("affected rows count mismatch\nGot: %v\nWant: %v", res.RowsAffected, 1)
	}
}

func strPointer(val string) *string {
	return &val
}

func setupTestDBConnection(t *testing.T) (db *gorm.DB, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	return setupTestDBConnectionWithParams(t, "")
}

func setupTestDBConnectionWithParams(t *testing.T, params string) (db *gorm.DB, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, _, serverTeardown := setupMockedTestServer(t)

	db, err := gorm.Open(New(Config{
		DriverName: "spanner",
		DSN:        fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true;%s", server.Address, params),
	}))
	if err != nil {
		serverTeardown()
		t.Fatal(err)
	}
	return db, server, func() {
		serverTeardown()
	}
}

func setupMockedTestServer(t *testing.T) (server *testutil.MockedSpannerInMemTestServer, client *spanner.Client, teardown func()) {
	return setupMockedTestServerWithConfig(t, spanner.ClientConfig{})
}

func setupMockedTestServerWithConfig(t *testing.T, config spanner.ClientConfig) (server *testutil.MockedSpannerInMemTestServer, client *spanner.Client, teardown func()) {
	return setupMockedTestServerWithConfigAndClientOptions(t, config, []option.ClientOption{})
}

func setupMockedTestServerWithConfigAndClientOptions(t *testing.T, config spanner.ClientConfig, clientOptions []option.ClientOption) (server *testutil.MockedSpannerInMemTestServer, client *spanner.Client, teardown func()) {
	server, opts, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	opts = append(opts, clientOptions...)
	ctx := context.Background()
	formattedDatabase := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "[PROJECT]", "[INSTANCE]", "[DATABASE]")
	client, err := spanner.NewClientWithConfig(ctx, formattedDatabase, config, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return server, client, func() {
		client.Close()
		serverTeardown()
	}
}

func createSingersResultSet(singers []Singer) *spannerpb.ResultSet {
	fields := make([]*spannerpb.StructType_Field, 4)
	fields[0] = &spannerpb.StructType_Field{
		Name: "singer_id",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	fields[1] = &spannerpb.StructType_Field{
		Name: "first_name",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
	}
	fields[2] = &spannerpb.StructType_Field{
		Name: "last_name",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
	}
	fields[3] = &spannerpb.StructType_Field{
		Name: "birth_date",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_DATE},
	}
	rowType := &spannerpb.StructType{
		Fields: fields,
	}
	metadata := &spannerpb.ResultSetMetadata{
		RowType: rowType,
	}
	rows := make([]*structpb.ListValue, len(singers))
	for i := 0; i < len(singers); i++ {
		rowValue := make([]*structpb.Value, len(fields))
		rowValue[0] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%d", singers[i].SingerId)}}
		if singers[i].FirstName == nil {
			rowValue[1] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		} else {
			rowValue[1] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: *singers[i].FirstName}}
		}
		rowValue[2] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: singers[i].LastName}}
		if !singers[i].BirthDate.Valid {
			rowValue[3] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		} else {
			rowValue[3] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: singers[i].BirthDate.String()}}
		}
		rows[i] = &structpb.ListValue{
			Values: rowValue,
		}
	}
	return &spannerpb.ResultSet{
		Metadata: metadata,
		Rows:     rows,
	}
}
