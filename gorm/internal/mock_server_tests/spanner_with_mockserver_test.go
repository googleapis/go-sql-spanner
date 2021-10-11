// Copyright 2021 Google LLC All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mock_server_tests

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/go-sql-spanner/testutil"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/option"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"gorm.io/gorm"
	"spannergorm"
)

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
		t.Fatalf("failed to fetch Singer: %v", err)
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
		t.Fatalf("Singer count mismatch\nGot: %v\nWant: %v", len(singers), 2)
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

func TestForceIndexHint(t *testing.T) {
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
	if err := db.Clauses(spannergorm.ForceIndex("LastName")).Take(&singer, 1).Error; err != nil {
		t.Fatalf("failed to fetch Singer: %v", err)
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
		t.Fatalf("failed to create new Singer: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("affected rows count mismatch\nGot: %v\nWant: %v", res.RowsAffected, 1)
	}
}

func TestCreateMultipleSingers(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	server.TestSpanner.PutStatementResult(
		"INSERT INTO `singers` (`singer_id`,`first_name`,`last_name`,`birth_date`) VALUES (@p1,@p2,@p3,@p4),(@p5,@p6,@p7,@p8)",
		&testutil.StatementResult{
			Type:        testutil.StatementResultUpdateCount,
			UpdateCount: 2,
		})

	// TODO: Support using mutations.
	res := db.Create([]*Singer{
		{
			SingerId:  1,
			FirstName: strPointer("Pete"),
			LastName:  "Allison",
			BirthDate: spanner.NullDate{Date: civil.Date{Year: 1998, Month: 4, Day: 23}, Valid: true},
		},
		{
			SingerId:  2,
			FirstName: strPointer("Alice"),
			LastName:  "Peterson",
			BirthDate: spanner.NullDate{Date: civil.Date{Year: 2001, Month: 12, Day: 2}, Valid: true},
		},
	})
	if res.Error != nil {
		t.Fatalf("failed to create new singers: %v", res.Error)
	}
	if res.RowsAffected != 2 {
		t.Fatalf("affected rows count mismatch\nGot: %v\nWant: %v", res.RowsAffected, 2)
	}
}

func TestUpdateSinger(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	server.TestSpanner.PutStatementResult(
		"SELECT * FROM `singers` WHERE `singers`.`singer_id` = @p1 LIMIT 1",
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: createSingersResultSet([]Singer{{1, nil, "Allison", spanner.NullDate{}}}),
		})
	server.TestSpanner.PutStatementResult(
		"UPDATE `singers` SET `first_name`=@p1,`last_name`=@p2,`birth_date`=@p3 WHERE `singer_id` = @p4",
		&testutil.StatementResult{
			Type:        testutil.StatementResultUpdateCount,
			UpdateCount: 1,
		})

	var singer Singer
	if err := db.Take(&singer, 1).Error; err != nil {
		t.Fatalf("failed to get Singer: %v", err)
	}
	singer.FirstName = strPointer("Pete")
	singer.BirthDate = spanner.NullDate{Valid: true, Date: civil.Date{2003, 2, 27}}
	res := db.Save(singer)
	if res.Error != nil {
		t.Fatalf("failed to update Singer: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("affected rows count mismatch\nGot: %v\nWant: %v", res.RowsAffected, 1)
	}
}

func TestDeleteSinger(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	server.TestSpanner.PutStatementResult(
		"SELECT * FROM `singers` WHERE `singers`.`singer_id` = @p1 LIMIT 1",
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: createSingersResultSet([]Singer{{1, nil, "Allison", spanner.NullDate{}}}),
		})
	server.TestSpanner.PutStatementResult(
		"DELETE FROM `singers` WHERE `singers`.`singer_id` = @p1",
		&testutil.StatementResult{
			Type:        testutil.StatementResultUpdateCount,
			UpdateCount: 1,
		})

	var singer Singer
	if err := db.Take(&singer, 1).Error; err != nil {
		t.Fatalf("failed to get Singer: %v", err)
	}
	res := db.Delete(&singer)
	if res.Error != nil {
		t.Fatalf("failed to delete Singer: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("affected rows count mismatch\nGot: %v\nWant: %v", res.RowsAffected, 1)
	}
}

func TestSelectOneAlbumWithPreload(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	server.TestSpanner.PutStatementResult(
		"SELECT * FROM `albums` WHERE `albums`.`album_id` = @p1 LIMIT 1",
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: createAlbumsResultSet([]Album{{1, int64Pointer(1), nil, "Title 1"}}),
		})
	server.TestSpanner.PutStatementResult(
		"SELECT * FROM `singers` WHERE `singers`.`singer_id` = @p1",
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: createSingersResultSet([]Singer{{1, strPointer("Alice"), "Peterson", spanner.NullDate{}}}),
		})

	var album Album
	if err := db.Preload("Singer").Take(&album, 1).Error; err != nil {
		t.Fatalf("failed to fetch Album: %v", err)
	}
	if album.AlbumId != 1 {
		t.Fatalf("AlbumId mismatch\nGot: %v\nWant: %v", album.AlbumId, 1)
	}
	if *album.SingerId != 1 {
		t.Fatalf("SingerId mismatch\nGot: %v\nWant: %v", *album.SingerId, 1)
	}
	if album.Singer == nil {
		t.Fatalf("Singer not preloaded")
	}
	if g, w := *album.Singer.FirstName, "Alice"; g != w {
		t.Fatalf("Singer FirstName mismatch\nGot: %s\nWant: %s", g, w)
	}
	if g, w := album.Singer.LastName, "Peterson"; g != w {
		t.Fatalf("Singer LastName mismatch\nGot: %s\nWant: %s", g, w)
	}
}

func TestSelectAllBasicTypes(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	w := AllBasicTypes{1, 100, 3.14, nullNumeric(true, "6.626"), "test", []byte{1, 2, 3}, nullDate(true, "2021-10-01"), timestamp("2021-07-22T10:26:17.123Z"), nullJson(true, `{"key":"value","other-key":["value1","value2"]}`), true}
	server.TestSpanner.PutStatementResult(
		"SELECT * FROM `all_basic_types` WHERE `all_basic_types`.`id` = @p1 LIMIT 1",
		&testutil.StatementResult{
			Type:      testutil.StatementResultResultSet,
			ResultSet: createAllBasicTypesResultSet([]AllBasicTypes{w}),
		})

	var g AllBasicTypes
	if err := db.Take(&g, 1).Error; err != nil {
		t.Fatalf("failed to fetch record: %v", err)
	}
	if !cmp.Equal(g, w, cmp.AllowUnexported(big.Rat{}, big.Int{})) {
		t.Fatalf("record value mismatch\n  Got: %v\nWant: %v", g, w)
	}
}

func TestCreateAllBasicTypes(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	server.TestSpanner.PutStatementResult(
		"INSERT INTO `all_basic_types` (`id`,`int64`,`float64`,`numeric`,`string`,`bytes`,`date`,`timestamp`,`json`,`bool`) VALUES (@p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10)",
		&testutil.StatementResult{
			Type:        testutil.StatementResultUpdateCount,
			UpdateCount: 1,
		})
	// Drain any SELECT 1 and other startup requests from the server.
	drainRequestsFromServer(server.TestSpanner)

	record := &AllBasicTypes{
		Id:        1,
		Int64:     200,
		Float64:   3.14,
		Numeric:   nullNumeric(true, "6.626"),
		String:    "test",
		Bytes:     []byte{3, 2, 1},
		Date:      nullDate(true, "2021-10-01"),
		Timestamp: timestamp("2021-10-01T14:40:00Z"),
		Json:      nullJson(true, `{"key":"value","other-key":["value1","value2"]}`),
		Bool:      true,
	}
	res := db.Create(record)
	if res.Error != nil {
		t.Fatalf("failed to create new record: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("affected rows count mismatch\nGot: %v\nWant: %v", res.RowsAffected, 1)
	}
	reqs := drainRequestsFromServer(server.TestSpanner)
	sqlReqs := requestsOfType(reqs, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
	if g, w := len(sqlReqs), 1; g != w {
		t.Fatalf("execute sql request count mismatch\n  Got: %v\nWant: %v", g, w)
	}
	sqlReq := sqlReqs[0].(*spannerpb.ExecuteSqlRequest)
	if g, w := len(sqlReq.ParamTypes), 10; g != w {
		t.Fatalf("param types length mismatch\n  Got: %v\nWant: %v", g, w)
	}
	wantParamTypes := []spannerpb.TypeCode{
		spannerpb.TypeCode_INT64,
		spannerpb.TypeCode_INT64,
		spannerpb.TypeCode_FLOAT64,
		spannerpb.TypeCode_NUMERIC,
		spannerpb.TypeCode_STRING,
		spannerpb.TypeCode_BYTES,
		spannerpb.TypeCode_DATE,
		spannerpb.TypeCode_TIMESTAMP,
		spannerpb.TypeCode_JSON,
		spannerpb.TypeCode_BOOL,
	}
	for i, w := range wantParamTypes {
		g := sqlReq.ParamTypes[fmt.Sprintf("p%d", i+1)].Code
		if g != w {
			t.Fatalf("param type mismatch for param %d\n  Got: %v\nWant: %v", i+1, g, w)
		}
	}
	wantValues := []interface{}{
		fmt.Sprintf("%d", record.Id),
		fmt.Sprintf("%d", record.Int64),
		record.Float64,
		spanner.NumericString(&record.Numeric.Numeric),
		record.String,
		base64.StdEncoding.EncodeToString(record.Bytes),
		record.Date.String(),
		record.Timestamp.UTC().Format(time.RFC3339Nano),
		record.Json.String(),
		record.Bool,
	}
	for i, w := range wantValues {
		g := sqlReq.Params.Fields[fmt.Sprintf("p%d", i+1)].AsInterface()
		if g != w {
			t.Fatalf("param value mismatch for param %d\n  Got: %v\nWant: %v", i+1, g, w)
		}
	}
}

func TestSelectAllSpannerNullableTypes(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	for i, w := range []AllSpannerNullableTypes{
		{
			Id:        1,
			Int64:     spanner.NullInt64{Int64: 100, Valid: true},
			Float64:   spanner.NullFloat64{Float64: 3.14, Valid: true},
			Numeric:   nullNumeric(true, "6.626"),
			String:    spanner.NullString{StringVal: "test", Valid: true},
			Bytes:     []byte{1, 2, 3},
			Date:      nullDate(true, "2021-10-01"),
			Timestamp: nullTimestamp(true, "2021-07-22T10:26:17.123Z"),
			Json:      nullJson(true, `{"key":"value","other-key":["value1","value2"]}`),
			Bool:      spanner.NullBool{Valid: true, Bool: false},
		},
		{
			Id:        1,
			Int64:     spanner.NullInt64{},
			Float64:   spanner.NullFloat64{},
			Numeric:   spanner.NullNumeric{},
			String:    spanner.NullString{},
			Bytes:     []byte(nil),
			Date:      spanner.NullDate{},
			Timestamp: spanner.NullTime{},
			Json:      spanner.NullJSON{},
			Bool:      spanner.NullBool{},
		},
	} {
		server.TestSpanner.PutStatementResult(
			"SELECT * FROM `all_spanner_nullable_types` WHERE `all_spanner_nullable_types`.`id` = @p1 LIMIT 1",
			&testutil.StatementResult{
				Type:      testutil.StatementResultResultSet,
				ResultSet: createAllSpannerNullableTypesResultSet([]AllSpannerNullableTypes{w}),
			})

		var g AllSpannerNullableTypes
		if err := db.Take(&g, 1).Error; err != nil {
			t.Fatalf("%d: failed to fetch record: %v", i, err)
		}
		if !cmp.Equal(g, w, cmp.AllowUnexported(big.Rat{}, big.Int{})) {
			t.Fatalf("%d: record value mismatch\n Got: %v\nWant: %v", i, g, w)
		}
	}
}

func TestCreateAllSpannerNullableTypes(t *testing.T) {
	t.Parallel()

	db, server, teardown := setupTestDBConnection(t)
	defer teardown()
	server.TestSpanner.PutStatementResult(
		"INSERT INTO `all_spanner_nullable_types` (`id`,`int64`,`float64`,`numeric`,`string`,`bytes`,`date`,`timestamp`,`json`,`bool`) VALUES (@p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10)",
		&testutil.StatementResult{
			Type:        testutil.StatementResultUpdateCount,
			UpdateCount: 1,
		})
	// Drain any SELECT 1 and other startup requests from the server.
	drainRequestsFromServer(server.TestSpanner)

	for i, record := range []*AllSpannerNullableTypes{
		{
			Id:        1,
			Int64:     spanner.NullInt64{Int64: 100, Valid: true},
			Float64:   spanner.NullFloat64{Float64: 3.14, Valid: true},
			Numeric:   nullNumeric(true, "6.626"),
			String:    spanner.NullString{StringVal: "test", Valid: true},
			Bytes:     []byte{1, 2, 3},
			Date:      nullDate(true, "2021-10-01"),
			Timestamp: nullTimestamp(true, "2021-07-22T10:26:17.123Z"),
			Json:      nullJson(true, `{"key":"value","other-key":["value1","value2"]}`),
			Bool:      spanner.NullBool{Valid: true, Bool: false},
		},
		{
			Id:        1,
			Int64:     spanner.NullInt64{},
			Float64:   spanner.NullFloat64{},
			Numeric:   spanner.NullNumeric{},
			String:    spanner.NullString{},
			Bytes:     []byte(nil),
			Date:      spanner.NullDate{},
			Timestamp: spanner.NullTime{},
			Json:      spanner.NullJSON{},
			Bool:      spanner.NullBool{},
		},
	} {
		res := db.Create(record)
		if res.Error != nil {
			t.Fatalf("%d: failed to create new record: %v", i, res.Error)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("%d: affected rows count mismatch\n Got: %v\nWant: %v", i, res.RowsAffected, 1)
		}
		reqs := drainRequestsFromServer(server.TestSpanner)
		sqlReqs := requestsOfType(reqs, reflect.TypeOf(&spannerpb.ExecuteSqlRequest{}))
		if g, w := len(sqlReqs), 1; g != w {
			t.Fatalf("%d: execute sql request count mismatch\n Got: %v\nWant: %v", i, g, w)
		}
		sqlReq := sqlReqs[0].(*spannerpb.ExecuteSqlRequest)
		if g, w := len(sqlReq.ParamTypes), 10; g != w {
			t.Fatalf("%d: param types length mismatch\n Got: %v\nWant: %v", i, g, w)
		}
		wantParamTypes := []spannerpb.TypeCode{
			spannerpb.TypeCode_INT64,
			spannerpb.TypeCode_INT64,
			spannerpb.TypeCode_FLOAT64,
			spannerpb.TypeCode_NUMERIC,
			spannerpb.TypeCode_STRING,
			spannerpb.TypeCode_BYTES,
			spannerpb.TypeCode_DATE,
			spannerpb.TypeCode_TIMESTAMP,
			spannerpb.TypeCode_JSON,
			spannerpb.TypeCode_BOOL,
		}
		for p, w := range wantParamTypes {
			g := sqlReq.ParamTypes[fmt.Sprintf("p%d", p+1)].Code
			if g != w {
				t.Fatalf("%d: param type mismatch for param %d\n Got: %v\nWant: %v", i, p+1, g, w)
			}
		}
		var wantValues []interface{}
		if record.Int64.Valid {
			wantValues = []interface{}{
				fmt.Sprintf("%d", record.Id),
				fmt.Sprintf("%d", record.Int64.Int64),
				record.Float64.Float64,
				spanner.NumericString(&record.Numeric.Numeric),
				record.String.StringVal,
				base64.StdEncoding.EncodeToString(record.Bytes),
				record.Date.Date.String(),
				record.Timestamp.Time.UTC().Format(time.RFC3339Nano),
				record.Json.String(),
				record.Bool.Bool,
			}
		} else {
			wantValues = []interface{}{fmt.Sprintf("%d", record.Id), nil, nil, nil, nil, nil, nil, nil, nil, nil}
		}
		for p, w := range wantValues {
			g := sqlReq.Params.Fields[fmt.Sprintf("p%d", p+1)].AsInterface()
			if g != w {
				t.Fatalf("%d: param value mismatch for param %d\n Got: %v\nWant: %v", i, p+1, g, w)
			}
		}
	}
}

func strPointer(val string) *string {
	return &val
}

func int64Pointer(val int64) *int64 {
	return &val
}

func numeric(v string) big.Rat {
	res, _ := big.NewRat(1, 1).SetString(v)
	return *res
}

func nullNumeric(valid bool, v string) spanner.NullNumeric {
	if !valid {
		return spanner.NullNumeric{}
	}
	return spanner.NullNumeric{Valid: true, Numeric: numeric(v)}
}

func date(v string) civil.Date {
	res, _ := civil.ParseDate(v)
	return res
}

func nullDate(valid bool, v string) spanner.NullDate {
	if !valid {
		return spanner.NullDate{}
	}
	return spanner.NullDate{Valid: true, Date: date(v)}
}

func nullTimestamp(valid bool, v string) spanner.NullTime {
	if !valid {
		return spanner.NullTime{}
	}
	return spanner.NullTime{Valid: true, Time: timestamp(v)}
}

func timestamp(v string) time.Time {
	res, _ := time.Parse(time.RFC3339Nano, v)
	return res
}

func nullJson(valid bool, v string) spanner.NullJSON {
	if !valid {
		return spanner.NullJSON{}
	}
	var m map[string]interface{}
	_ = json.Unmarshal([]byte(v), &m)
	return spanner.NullJSON{Valid: true, Value: m}
}

func setupTestDBConnection(t *testing.T) (db *gorm.DB, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	return setupTestDBConnectionWithParams(t, "")
}

func setupTestDBConnectionWithParams(t *testing.T, params string) (db *gorm.DB, server *testutil.MockedSpannerInMemTestServer, teardown func()) {
	server, _, serverTeardown := setupMockedTestServer(t)

	db, err := gorm.Open(spannergorm.New(spannergorm.Config{
		DriverName: "spanner",
		DSN:        fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true;%s", server.Address, params),
	}), &gorm.Config{PrepareStmt: true})
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

func createAlbumsResultSet(albums []Album) *spannerpb.ResultSet {
	fields := make([]*spannerpb.StructType_Field, 3)
	fields[0] = &spannerpb.StructType_Field{
		Name: "album_id",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	fields[1] = &spannerpb.StructType_Field{
		Name: "title",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
	}
	fields[2] = &spannerpb.StructType_Field{
		Name: "singer_id",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	rowType := &spannerpb.StructType{
		Fields: fields,
	}
	metadata := &spannerpb.ResultSetMetadata{
		RowType: rowType,
	}
	rows := make([]*structpb.ListValue, len(albums))
	for i := 0; i < len(albums); i++ {
		rowValue := make([]*structpb.Value, len(fields))
		rowValue[0] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%d", albums[i].AlbumId)}}
		rowValue[1] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: albums[i].Title}}
		if albums[i].SingerId == nil {
			rowValue[2] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		} else {
			rowValue[2] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%d", *albums[i].SingerId)}}
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

func createAllBasicTypesResultSet(records []AllBasicTypes) *spannerpb.ResultSet {
	fields := make([]*spannerpb.StructType_Field, 10)
	fields[0] = &spannerpb.StructType_Field{
		Name: "id",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	fields[1] = &spannerpb.StructType_Field{
		Name: "int64",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	fields[2] = &spannerpb.StructType_Field{
		Name: "float64",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT64},
	}
	fields[3] = &spannerpb.StructType_Field{
		Name: "numeric",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_NUMERIC},
	}
	fields[4] = &spannerpb.StructType_Field{
		Name: "string",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
	}
	fields[5] = &spannerpb.StructType_Field{
		Name: "bytes",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_BYTES},
	}
	fields[6] = &spannerpb.StructType_Field{
		Name: "date",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_DATE},
	}
	fields[7] = &spannerpb.StructType_Field{
		Name: "timestamp",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP},
	}
	fields[8] = &spannerpb.StructType_Field{
		Name: "json",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_JSON},
	}
	fields[9] = &spannerpb.StructType_Field{
		Name: "bool",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_BOOL},
	}
	rowType := &spannerpb.StructType{
		Fields: fields,
	}
	metadata := &spannerpb.ResultSetMetadata{
		RowType: rowType,
	}

	rows := make([]*structpb.ListValue, len(records))
	for i := 0; i < len(records); i++ {
		rowValue := make([]*structpb.Value, len(fields))
		rowValue[0] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%d", records[i].Id)}}
		rowValue[1] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%d", records[i].Int64)}}
		rowValue[2] = &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: records[i].Float64}}
		rowValue[3] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: spanner.NumericString(&records[i].Numeric.Numeric)}}
		rowValue[4] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: records[i].String}}
		rowValue[5] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString(records[i].Bytes)}}
		rowValue[6] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: records[i].Date.Date.String()}}
		rowValue[7] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: records[i].Timestamp.UTC().Format(time.RFC3339Nano)}}
		rowValue[8] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: records[i].Json.String()}}
		rowValue[9] = &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: records[i].Bool}}
		rows[i] = &structpb.ListValue{
			Values: rowValue,
		}
	}
	return &spannerpb.ResultSet{
		Metadata: metadata,
		Rows:     rows,
	}
}

func createAllSpannerNullableTypesResultSet(records []AllSpannerNullableTypes) *spannerpb.ResultSet {
	fields := make([]*spannerpb.StructType_Field, 10)
	fields[0] = &spannerpb.StructType_Field{
		Name: "id",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	fields[1] = &spannerpb.StructType_Field{
		Name: "int64",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
	}
	fields[2] = &spannerpb.StructType_Field{
		Name: "float64",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT64},
	}
	fields[3] = &spannerpb.StructType_Field{
		Name: "numeric",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_NUMERIC},
	}
	fields[4] = &spannerpb.StructType_Field{
		Name: "string",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
	}
	fields[5] = &spannerpb.StructType_Field{
		Name: "bytes",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_BYTES},
	}
	fields[6] = &spannerpb.StructType_Field{
		Name: "date",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_DATE},
	}
	fields[7] = &spannerpb.StructType_Field{
		Name: "timestamp",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP},
	}
	fields[8] = &spannerpb.StructType_Field{
		Name: "json",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_JSON},
	}
	fields[9] = &spannerpb.StructType_Field{
		Name: "bool",
		Type: &spannerpb.Type{Code: spannerpb.TypeCode_BOOL},
	}
	rowType := &spannerpb.StructType{
		Fields: fields,
	}
	metadata := &spannerpb.ResultSetMetadata{
		RowType: rowType,
	}

	rows := make([]*structpb.ListValue, len(records))
	for i := 0; i < len(records); i++ {
		rowValue := make([]*structpb.Value, len(fields))
		rowValue[0] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%d", records[i].Id)}}
		if records[i].Int64.Valid {
			rowValue[1] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%d", records[i].Int64.Int64)}}
		} else {
			rowValue[1] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
		if records[i].Float64.Valid {
			rowValue[2] = &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: records[i].Float64.Float64}}
		} else {
			rowValue[2] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
		if records[i].Numeric.Valid {
			rowValue[3] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: spanner.NumericString(&records[i].Numeric.Numeric)}}
		} else {
			rowValue[3] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
		if records[i].String.Valid {
			rowValue[4] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: records[i].String.StringVal}}
		} else {
			rowValue[4] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
		if records[i].Bytes != nil {
			rowValue[5] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString(records[i].Bytes)}}
		} else {
			rowValue[5] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
		if records[i].Date.Valid {
			rowValue[6] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: records[i].Date.Date.String()}}
		} else {
			rowValue[6] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
		if records[i].Timestamp.Valid {
			rowValue[7] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: records[i].Timestamp.Time.UTC().Format(time.RFC3339Nano)}}
		} else {
			rowValue[7] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
		if records[i].Json.Valid {
			rowValue[8] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: records[i].Json.String()}}
		} else {
			rowValue[8] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
		}
		if records[i].Bool.Valid {
			rowValue[9] = &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: records[i].Bool.Bool}}
		} else {
			rowValue[9] = &structpb.Value{Kind: &structpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
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
