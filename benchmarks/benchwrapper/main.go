// Copyright 2022 Google LLC
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

// Package main wraps the client library in a gRPC interface that a benchmarker
// can communicate through.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc"

	spannerdriver "github.com/googleapis/go-sql-spanner"
	pb "github.com/googleapis/go-sql-spanner/benchmarks/benchwrapper/proto"
)

var port = flag.String("port", "", "specify a port to run on")

type server struct {
	pb.UnimplementedSpannerBenchWrapperServer
	db *sql.DB
}

func main() {
	flag.Parse()
	if *port == "" {
		log.Fatalf("usage: %s --port=8081", os.Args[0])
	}

	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		log.Fatal("This benchmarking server only works when connected to an emulator. Please set SPANNER_EMULATOR_HOST.")
	}
	db, err := sql.Open("spanner", "projects/someproject/instances/someinstance/databases/somedatabase")
	if err != nil {
		log.Fatalf("failed to open database connection: %v\n", err)
	}
	defer db.Close()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterSpannerBenchWrapperServer(s, &server{
		db: db,
	})
	log.Printf("Running on localhost:%s\n", *port)
	log.Fatal(s.Serve(lis))
}

func (s *server) Read(ctx context.Context, req *pb.ReadQuery) (*pb.EmptyResponse, error) {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		log.Fatalf("failed to begin read-only transaction: %v", err)
	}
	it, err := tx.Query(req.Query)
	if err != nil {
		log.Fatal(err)
	}
	defer it.Close()
	for it.Next() {
	}
	if err = it.Err(); err != nil {
		log.Fatalf("error iterating over the rows: %v", err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatalf("failed to commit tx: %v", err)
	}
	return &pb.EmptyResponse{}, nil
}

func (s *server) Insert(ctx context.Context, req *pb.InsertQuery) (*pb.EmptyResponse, error) {
	var mutations []*spanner.Mutation
	for _, i := range req.Singers {
		mutations = append(mutations, spanner.Insert("Singers", []string{"SingerId", "FirstName", "LastName"}, []interface{}{i.Id, i.FirstName, i.LastName}))
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		log.Fatalf("failed to call db.Conn: %v", err)
	}
	defer conn.Close()
	if err := conn.Raw(func(driverConn interface{}) error {
		_, err := driverConn.(spannerdriver.SpannerConn).Apply(ctx, mutations)
		return err
	}); err != nil {
		log.Fatal(err)
	}
	// Do nothing with the data.
	return &pb.EmptyResponse{}, nil
}

func (s *server) Update(ctx context.Context, req *pb.UpdateQuery) (*pb.EmptyResponse, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		log.Fatalf("failed to begin transaction: %v", err)
	}
	// A DML batch can be executed using custom SQL statements.
	// Start a DML batch on the transaction.
	if _, err := tx.ExecContext(ctx, "START BATCH DML"); err != nil {
		log.Fatalf("failed to execute START BATCH DML: %v", err)
	}
	for _, q := range req.Queries {
		_, err = tx.ExecContext(ctx, q)
		if err != nil {
			log.Fatalf("failed to execute query: v", err)
		}
	}
	// Run the active DML batch.
	if _, err := tx.ExecContext(ctx, "RUN BATCH"); err != nil {
		log.Fatalf("failed to execute RUN BATCH: %v", err)
	}
	// Commit the transaction.
	if err := tx.Commit(); err != nil {
		log.Fatalf("failed to commit transaction: %v", err)
	}
	// Do nothing with the data.
	return &pb.EmptyResponse{}, nil
}
