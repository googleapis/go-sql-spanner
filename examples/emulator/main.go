// Copyright 2025 Google LLC All Rights Reserved.
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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/docker/docker/api/types/container"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Sample application that shows how to use the Spanner Go sql driver to connect
// to the Spanner Emulator and automatically create the instance and database
// from the connection string on the Emulator.
//
// Execute the sample with the command `go run main.go` from this directory.
func emulator(projectId, instanceId, databaseId string) error {
	ctx := context.Background()

	// Start the Spanner emulator in a Docker container.
	emulator, host, err := startEmulator()
	if err != nil {
		return err
	}
	defer func() { _ = emulator.Terminate(context.Background()) }()

	config := spannerdriver.ConnectorConfig{
		// AutoConfigEmulator instructs the driver to:
		// 1. Connect to the emulator using plain text.
		// 2. Create the instance and database if they do not already exist.
		AutoConfigEmulator: true,

		// You only have to set the host if it is different from the default
		// 'localhost:9010' host for the Spanner emulator.
		Host: host,

		// The instance and database will automatically be created on the Emulator.
		Project:  projectId,
		Instance: instanceId,
		Database: databaseId,
	}
	connector, err := spannerdriver.CreateConnector(config)
	if err != nil {
		return err
	}
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, "SELECT 'Hello World!'")
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	var msg string
	for rows.Next() {
		if err := rows.Scan(&msg); err != nil {
			return fmt.Errorf("failed to scan row values: %v", err)
		}
		fmt.Printf("%s\n", msg)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate over query results: %v", err)
	}
	return nil
}

func main() {
	if err := emulator("emulator-project", "test-instance", "test-database"); err != nil {
		log.Fatal(err)
	}
}

// startEmulator starts the Spanner Emulator in a Docker container.
func startEmulator() (testcontainers.Container, string, error) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		AlwaysPullImage: true,
		Image:           "gcr.io/cloud-spanner-emulator/emulator",
		ExposedPorts:    []string{"9010/tcp"},
		WaitingFor:      wait.ForAll(wait.ForListeningPort("9010/tcp"), wait.ForLog("gRPC server listening")),
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			hostConfig.AutoRemove = true
		},
	}
	emulator, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return emulator, "", fmt.Errorf("failed to start the emulator: %v", err)
	}
	host, err := emulator.Host(ctx)
	if err != nil {
		return emulator, "", fmt.Errorf("failed to get host: %v", err)
	}
	mappedPort, err := emulator.MappedPort(ctx, "9010/tcp")
	if err != nil {
		return emulator, "", fmt.Errorf("failed to get mapped port: %v", err)
	}
	port := mappedPort.Int()

	return emulator, fmt.Sprintf("%s:%v", host, port), nil
}
