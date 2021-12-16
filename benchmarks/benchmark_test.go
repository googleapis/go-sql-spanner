// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/cloudspannerecosystem/go-sql-spanner"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc/codes"
)

var createTableStatements = []string{
	`CREATE TABLE Singers (
		SingerId  INT64 NOT NULL,
		FirstName STRING(100),
		LastName  STRING(200) NOT NULL,
		FullName  STRING(300) NOT NULL AS (COALESCE(FirstName || ' ', '') || LastName) STORED,
		BirthDate DATE,
		Picture   BYTES(MAX),
	) PRIMARY KEY (SingerId)`,
	`CREATE TABLE Albums (
		AlbumId     INT64 NOT NULL,
		Title       STRING(200) NOT NULL,
		ReleaseDate DATE,
		SingerId    INT64 NOT NULL,
		CONSTRAINT FK_Albums_Singers FOREIGN KEY (SingerId) REFERENCES Singers (SingerId),
	) PRIMARY KEY (AlbumId)`,
}

var benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId string
var allIds []int64

func TestMain(m *testing.M) {
	fmt.Printf("Initializing benchmarks...\n")
	if err := setup(); err != nil {
		fmt.Printf("failed to initialize benchmark: %v", err)
		os.Exit(1)
	}
	res := m.Run()
	os.Exit(res)
}

func BenchmarkSelectSingleRecordConnection(b *testing.B) {
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database connection: %v\n", err)
	}
	defer db.Close()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := selectRandomSinger(db, allIds, rnd); err != nil {
			b.Fatalf("failed to select singer: %v", err)
		}
	}
}

func BenchmarkSelectSingleRecordClient(b *testing.B) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database client: %v\n", err)
	}
	defer client.Close()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := selectRandomSingerWithClient(client.Single(), allIds, rnd); err != nil {
			b.Fatalf("failed to select singer: %v", err)
		}
	}
}

func BenchmarkSelectAndUpdateUsingMutationConnection(b *testing.B) {
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database connection: %v\n", err)
	}
	defer db.Close()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()

	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		conn, _ := db.Conn(ctx)
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			b.Fatalf("failed to begin transaction: %v", err)
		}
		singer, err := selectRandomSinger(tx, allIds, rnd)
		if err != nil {
			b.Fatalf("failed to query singer: %v", err)
		}
		singer.LastName = uuid.New().String()
		if err := updateSingerUsingMutation(conn, singer); err != nil {
			b.Fatalf("failed to update singer: %v", err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("failed to commit: %v", err)
		}
		_ = conn.Close()
	}
}

func BenchmarkSelectAndUpdateUsingMutationClient(b *testing.B) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database client: %v\n", err)
	}
	defer client.Close()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, transaction *spanner.ReadWriteTransaction) error {
			singer, err := selectRandomSingerWithClient(transaction, allIds, rnd)
			if err != nil {
				b.Fatalf("failed to select singer: %v", err)
			}
			singer.LastName = uuid.New().String()
			return updateSingerUsingMutationWithClient(transaction, singer)
		}); err != nil {
			b.Fatalf("failed to execute transaction: %v", err)
		}
	}
}

func BenchmarkSelectAndUpdateUsingDmlConnection(b *testing.B) {
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database connection: %v\n", err)
	}
	defer db.Close()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()

	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			b.Fatalf("failed to begin transaction: %v", err)
		}
		singer, err := selectRandomSinger(tx, allIds, rnd)
		if err != nil {
			b.Fatalf("failed to query singer: %v", err)
		}
		singer.LastName = uuid.New().String()
		if err := updateSingerUsingDml(tx, singer); err != nil {
			b.Fatalf("failed to update singer: %v", err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("failed to commit: %v", err)
		}
	}
}

func BenchmarkSelectAndUpdateUsingDmlClient(b *testing.B) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database client: %v\n", err)
	}
	defer client.Close()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, transaction *spanner.ReadWriteTransaction) error {
			singer, err := selectRandomSingerWithClient(transaction, allIds, rnd)
			if err != nil {
				b.Fatalf("failed to select singer: %v", err)
			}
			singer.LastName = uuid.New().String()
			return updateSingerUsingDmlWithClient(transaction, singer)
		}); err != nil {
			b.Fatalf("failed to execute transaction: %v", err)
		}
	}
}

func BenchmarkCreateAndReloadConnection(b *testing.B) {
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database connection: %v\n", err)
	}
	defer db.Close()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ids, err := createRandomSingers(db, 1)
		if err != nil {
			b.Fatalf("failed to create singer: %v", err)
		}
		if _, err := selectRandomSinger(db, ids, rnd); err != nil {
			b.Fatalf("failed to reload singer: %v", err)
		}
	}
}

func BenchmarkCreateAndReloadClient(b *testing.B) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database client: %v\n", err)
	}
	defer client.Close()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ids, err := createRandomSingersWithClient(client, 1)
		if err != nil {
			b.Fatalf("failed to create singer: %v", err)
		}
		if _, err := selectRandomSingerWithClient(client.Single(), ids, rnd); err != nil {
			b.Fatalf("failed to reload singer: %v", err)
		}
	}
}

func BenchmarkCreateAlbumsUsingMutationsConnection(b *testing.B) {
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database connection: %v\n", err)
	}
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := createRandomAlbums(db, 100); err != nil {
			b.Fatalf("failed to create albums: %v", err)
		}
	}
}

func BenchmarkCreateAlbumsUsingMutationsClient(b *testing.B) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database client: %v\n", err)
	}
	defer client.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := createRandomAlbumsWithClient(client, 100); err != nil {
			b.Fatalf("failed to craete albums: %v", err)
		}
	}
}

func BenchmarkSelect100SingersConnection(b *testing.B) {
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database connection: %v\n", err)
	}
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := selectRandomSingers(db, 100); err != nil {
			b.Fatalf("failed to select 100 singers: %v", err)
		}
	}
}

func BenchmarkSelect100SingersClient(b *testing.B) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database client: %v\n", err)
	}
	defer client.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := selectRandomSingersWithClient(client.Single(), 100); err != nil {
			b.Fatalf("failed to select 100 singers: %v", err)
		}
	}
}

func BenchmarkSelect100SingersReadOnlyTxConnection(b *testing.B) {
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database connection: %v\n", err)
	}
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			b.Fatalf("failed to begin read-only transaction: %v", err)
		}
		if err := selectRandomSingers(tx, 100); err != nil {
			b.Fatalf("failed to select 100 singers: %v", err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("failed to commit tx: %v", err)
		}
	}
}

func BenchmarkSelect100SingersReadOnlyTxClient(b *testing.B) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database client: %v\n", err)
	}
	defer client.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tx := client.ReadOnlyTransaction()
		if err := selectRandomSingersWithClient(tx, 100); err != nil {
			b.Fatalf("failed to select 100 singers: %v", err)
		}
		tx.Close()
	}
}

func BenchmarkSelect100SingersReadWriteTxConnection(b *testing.B) {
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database connection: %v\n", err)
	}
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			b.Fatalf("failed to begin read/write transaction: %v", err)
		}
		if err := selectRandomSingers(tx, 100); err != nil {
			b.Fatalf("failed to select 100 singers: %v", err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("failed to commit tx: %v", err)
		}
	}
}

func BenchmarkSelect100SingersReadWriteTxClient(b *testing.B) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		b.Fatalf("failed to open database client: %v\n", err)
	}
	defer client.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, transaction *spanner.ReadWriteTransaction) error {
			return selectRandomSingersWithClient(transaction, 100)
		}); err != nil {
			b.Fatalf("failed to select 100 singers: %v", err)
		}
	}
}

type singer struct {
	SingerId  int64
	FirstName string
	LastName  string
	FullName  string `spanner:"-"`
	BirthDate spanner.NullDate
	Picture   []byte
}

func (s *singer) toMutation() (*spanner.Mutation, error) {
	return spanner.InsertOrUpdateStruct("Singers", s)
}

type queryerSql interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row

	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func selectRandomSinger(db queryerSql, ids []int64, rnd *rand.Rand) (*singer, error) {
	var s singer
	row := db.QueryRowContext(context.Background(), "SELECT * FROM Singers WHERE SingerId=@id", ids[rnd.Intn(len(ids))])
	return &s, row.Scan(&s.SingerId, &s.FirstName, &s.LastName, &s.FullName, &s.BirthDate, &s.Picture)
}

func selectRandomSingers(db queryerSql, count int) error {
	rows, err := db.QueryContext(context.Background(), "SELECT * FROM Singers TABLESAMPLE RESERVOIR (100 ROWS)")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var s singer
		if err := rows.Scan(&s.SingerId, &s.FirstName, &s.LastName, &s.FullName, &s.BirthDate, &s.Picture); err != nil {
			return err
		}
	}
	return rows.Err()
}

func updateSingerUsingMutation(conn *sql.Conn, s *singer) error {
	m, err := s.toMutation()
	if err != nil {
		return err
	}
	return conn.Raw(func(driverConn interface{}) error {
		return driverConn.(spannerdriver.SpannerConn).BufferWrite([]*spanner.Mutation{m})
	})
}

var updateSql = "UPDATE Singers SET LastName=@last WHERE SingerId=@id"

func updateSingerUsingDml(tx *sql.Tx, s *singer) error {
	_, err := tx.ExecContext(context.Background(), updateSql, s.LastName, s.SingerId)
	return err
}

type queryerSpanner interface {
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
}

func selectRandomSingerWithClient(tx queryerSpanner, ids []int64, rnd *rand.Rand) (*singer, error) {
	rows := tx.Query(context.Background(), spanner.Statement{
		SQL: "SELECT * FROM Singers WHERE SingerId=@id",
		Params: map[string]interface{}{
			"id": ids[rnd.Intn(len(ids))],
		},
	})
	defer rows.Stop()
	var s singer
	for {
		row, err := rows.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if err := row.Columns(&s.SingerId, &s.FirstName, &s.LastName, &s.FullName, &s.BirthDate, &s.Picture); err != nil {
			return nil, err
		}
	}
	return &s, nil
}

func selectRandomSingersWithClient(tx queryerSpanner, count int) error {
	rows := tx.Query(context.Background(), spanner.Statement{
		SQL: "SELECT * FROM Singers TABLESAMPLE RESERVOIR (100 ROWS)",
	})
	defer rows.Stop()

	for {
		row, err := rows.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		var s singer
		if err := row.Columns(&s.SingerId, &s.FirstName, &s.LastName, &s.FullName, &s.BirthDate, &s.Picture); err != nil {
			return err
		}
	}
	return nil
}

func updateSingerUsingMutationWithClient(tx *spanner.ReadWriteTransaction, s *singer) error {
	m, err := s.toMutation()
	if err != nil {
		return err
	}
	return tx.BufferWrite([]*spanner.Mutation{m})
}

func updateSingerUsingDmlWithClient(tx *spanner.ReadWriteTransaction, s *singer) error {
	_, err := tx.Update(context.Background(), spanner.Statement{
		SQL: updateSql,
		Params: map[string]interface{}{
			"last": s.LastName,
			"id":   s.SingerId,
		},
	})
	return err
}

func setup() error {
	ctx := context.Background()
	benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId = os.Getenv("BENCHMARK_PROJECT_ID"), os.Getenv("BENCHMARK_INSTANCE_ID"), os.Getenv("BENCHMARK_DATABASE_ID")
	if benchmarkProjectId == "" {
		return fmt.Errorf("missing BENCHMARK_PROJECT_ID")
	}
	if benchmarkInstanceId == "" {
		return fmt.Errorf("missing BENCHMARK_INSTANCE_ID")
	}
	if benchmarkDatabaseId == "" {
		return fmt.Errorf("missing BENCHMARK_DATABASE_ID")
	}
	if err := createDb(benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId, createTableStatements); err != nil {
		return fmt.Errorf("Failed to create benchmark database: %v\n", err)
	}

	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", benchmarkProjectId, benchmarkInstanceId, benchmarkDatabaseId))
	if err != nil {
		return fmt.Errorf("Failed to open database connection: %v\n", err)
	}
	defer db.Close()

	fmt.Print("Deleting existing albums\n")
	if err := deleteAlbums(db); err != nil {
		return fmt.Errorf("failed to delete albums: %v", err)
	}

	batches := 10
	count := 1000
	total := batches * count
	var c int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM Singers").Scan(&c); err != nil {
		return err
	}
	if c == int64(total) {
		return selectAllSingerIds(db, total)
	}

	fmt.Print("Deleting existing singers\n")
	if err := deleteSingers(db); err != nil {
		return fmt.Errorf("Failed to delete existing singers from database: %v\n", err)
	}

	// Insert 10,000 test records.
	fmt.Printf("Inserting %v test records in %v batches\n", total, batches)
	for batch := 0; batch < batches; batch++ {
		ids, err := createRandomSingers(db, count)
		if err != nil {
			return fmt.Errorf("Failed to insert a batch of %v singers: %v\n", count, err)
		}
		allIds = append(allIds, ids...)
		fmt.Printf("Inserted %v singers\n", (batch+1)*count)
	}
	return nil
}

func selectAllSingerIds(db *sql.DB, count int) error {
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "SELECT SingerId FROM Singers")
	if err != nil {
		return err
	}
	defer rows.Close()

	allIds = make([]int64, count)
	for i := 0; rows.Next(); i++ {
		if err := rows.Scan(&allIds[i]); err != nil {
			return err
		}
	}
	return rows.Err()
}

func deleteSingers(db *sql.DB) error {
	return deleteAll(db, "Singers")
}

func deleteAlbums(db *sql.DB) error {
	return deleteAll(db, "Albums")
}

func deleteAll(db *sql.DB, table string) error {
	ctx := context.Background()
	conn, _ := db.Conn(ctx)
	defer conn.Close()

	_ = conn.Raw(func(driverConn interface{}) error {
		return driverConn.(spannerdriver.SpannerConn).SetAutocommitDMLMode(spannerdriver.PartitionedNonAtomic)
	})
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DELETE FROM `%s` WHERE TRUE", table))
	return err
}

func createRandomSingerMutations(count int) ([]int64, []*spanner.Mutation) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	firstNames := []string{"Pete", "Alice", "John", "Ethel", "Trudy", "Naomi", "Wendy", "Ruben", "Thomas", "Elly", "Cora", "Elise", "April", "Libby", "Alexandra", "Shania"}
	lastNames := []string{"Wendelson", "Allison", "Peterson", "Johnson", "Henderson", "Ericsson", "Aronson", "Tennet", "Courtou", "Mcdonald", "Berry", "Ramirez"}

	mutations := make([]*spanner.Mutation, count)
	ids := make([]int64, count)
	for i := 0; i < count; i++ {
		ids[i] = rnd.Int63()
		picture := make([]byte, rnd.Intn(2000)+50)
		rnd.Read(picture)
		mutations[i] = spanner.InsertOrUpdateMap(
			"Singers",
			map[string]interface{}{
				"SingerId":  ids[i],
				"FirstName": firstNames[rnd.Intn(len(firstNames))],
				"LastName":  lastNames[rnd.Intn(len(lastNames))],
				"BirthDate": civil.Date{Year: rnd.Intn(85) + 1920, Month: time.Month(rnd.Intn(12) + 1), Day: rnd.Intn(28) + 1},
				"Picture":   picture,
			})
	}
	return ids, mutations
}

func createRandomAlbumMutations(count int) []*spanner.Mutation {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	mutations := make([]*spanner.Mutation, count)
	for i := 0; i < count; i++ {
		picture := make([]byte, rnd.Intn(2000)+50)
		rnd.Read(picture)
		mutations[i] = spanner.InsertOrUpdateMap(
			"Albums",
			map[string]interface{}{
				"AlbumId":     rnd.Int63(),
				"Title":       "Some random title",
				"ReleaseDate": civil.Date{Year: 2021, Month: time.July, Day: 1},
				"SingerId":    allIds[rnd.Intn(len(allIds))],
			})
	}
	return mutations
}

func createRandomSingers(db *sql.DB, count int) ([]int64, error) {
	ctx := context.Background()

	conn, _ := db.Conn(ctx)
	defer conn.Close()
	ids, mutations := createRandomSingerMutations(count)
	if err := conn.Raw(func(driverConn interface{}) error {
		_, err := driverConn.(spannerdriver.SpannerConn).Apply(ctx, mutations)
		return err
	}); err != nil {
		return nil, err
	}

	return ids, nil
}

func createRandomSingersWithClient(client *spanner.Client, count int) ([]int64, error) {
	ids, mutations := createRandomSingerMutations(count)
	if _, err := client.Apply(context.Background(), mutations); err != nil {
		return nil, err
	}

	return ids, nil
}

func createRandomAlbums(db *sql.DB, count int) error {
	ctx := context.Background()

	conn, _ := db.Conn(ctx)
	defer conn.Close()
	mutations := createRandomAlbumMutations(count)
	return conn.Raw(func(driverConn interface{}) error {
		_, err := driverConn.(spannerdriver.SpannerConn).Apply(ctx, mutations)
		return err
	})
}

func createRandomAlbumsWithClient(client *spanner.Client, count int) error {
	ctx := context.Background()

	mutations := createRandomAlbumMutations(count)
	_, err := client.Apply(ctx, mutations)
	return err
}

func createDb(projectId, instanceId, databaseId string, statements []string) error {
	ctx := context.Background()
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer databaseAdminClient.Close()

	if _, err := databaseAdminClient.GetDatabase(ctx, &databasepb.GetDatabaseRequest{
		Name: fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId),
	}); spanner.ErrCode(err) != codes.NotFound {
		return err
	}

	fmt.Print("Database does not yet exists\n")
	fmt.Printf("Creating database %s\n", databaseId)
	opDB, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseId),
		ExtraStatements: statements,
	})
	if err != nil {
		return err
	}
	// Wait for the database creation to finish.
	if _, err := opDB.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for database creation to finish failed: %v", err)
	}

	fmt.Printf("Finished creating database %s\n", databaseId)
	return nil
}
