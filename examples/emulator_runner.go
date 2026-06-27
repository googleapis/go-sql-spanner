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

package examples

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var cli *client.Client
var containerId string

// RunSampleOnEmulator will run a sample function against a Spanner emulator instance in a Docker container.
// It requires Docker to be installed your local system to work.
//
// It will execute the following steps:
// 1. Start a Spanner emulator in a Docker container.
// 2. Create a sample instance and database on the emulator.
// 3. Execute the sample function against the emulator.
// 4. Stop the Docker container with the emulator.
func RunSampleOnEmulator(sample func(string, string, string) error, ddlStatements ...string) {
	RunSampleOnEmulatorWithDialect(sample, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, ddlStatements...)
}

func RunSampleOnEmulatorWithDialect(sample func(string, string, string) error, dialect databasepb.DatabaseDialect, ddlStatements ...string) {
	var err error
	var startedEmulatorSelf bool
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		if err = startEmulator(); err != nil {
			log.Fatalf("failed to start emulator: %v", err)
		}
		startedEmulatorSelf = true
	}
	projectId, instanceId, databaseId := "my-project", "my-instance", "my-database"
	if err = createInstance(projectId, instanceId); err != nil {
		if startedEmulatorSelf {
			stopEmulator()
		}
		log.Fatalf("failed to create instance on emulator: %v", err)
	}
	if err = createSampleDB(projectId, instanceId, databaseId, dialect, ddlStatements...); err != nil {
		if startedEmulatorSelf {
			stopEmulator()
		}
		log.Fatalf("failed to create database on emulator: %v", err)
	}
	err = sample(projectId, instanceId, databaseId)
	if startedEmulatorSelf {
		stopEmulator()
	}
	if err != nil {
		log.Fatal(err)
	}
}

func startEmulator() error {
	ctx := context.Background()
	if err := os.Setenv("SPANNER_EMULATOR_HOST", "127.0.0.1:9010"); err != nil {
		return err
	}

	err := startEmulatorInternal(ctx)
	if err != nil {
		_ = os.Unsetenv("SPANNER_EMULATOR_HOST")
	}
	return err
}

func startEmulatorInternal(ctx context.Context) error {
	// Initialize a Docker client.
	var err error
	cli, err = client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	var success bool
	defer func() {
		if !success {
			if cli != nil {
				if containerId != "" {
					timeout := 10
					_ = cli.ContainerStop(ctx, containerId, container.StopOptions{Timeout: &timeout})
					containerId = ""
				}
				_ = cli.Close()
				cli = nil
			}
		}
	}()

	// Pull the Spanner Emulator docker image.
	reader, err := cli.ImagePull(ctx, "gcr.io/cloud-spanner-emulator/emulator", image.PullOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()
	// cli.ImagePull is asynchronous.
	// The reader needs to be read completely for the pull operation to complete.
	if _, err := io.Copy(io.Discard, reader); err != nil {
		return err
	}

	// Create and start a container with the emulator.
	resp, err := cli.ContainerCreate(ctx, &container.Config{
		Image:        "gcr.io/cloud-spanner-emulator/emulator",
		ExposedPorts: nat.PortSet{"9010": {}},
	}, &container.HostConfig{
		AutoRemove:   true,
		PortBindings: map[nat.Port][]nat.PortBinding{"9010": {{HostIP: "0.0.0.0", HostPort: "9010"}}},
	}, nil, nil, "")
	if err != nil {
		return err
	}
	containerId = resp.ID

	if err := cli.ContainerStart(ctx, containerId, container.StartOptions{}); err != nil {
		return err
	}
	// Wait max 10 seconds or until the emulator is running and port 9010 is open.
	var portReady bool
	for c := 0; c < 40; c++ {
		resp, err := cli.ContainerInspect(ctx, containerId)
		if err != nil {
			return fmt.Errorf("failed to inspect container state: %v", err)
		}
		if resp.State.Running {
			// Try to connect to 127.0.0.1:9010
			conn, err := net.DialTimeout("tcp", "127.0.0.1:9010", 250*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				portReady = true
				break
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !portReady {
		return fmt.Errorf("emulator did not start listening on port 9010 in time")
	}
	if err := waitForGRPC(ctx); err != nil {
		return err
	}
	success = true
	return nil
}

func waitForGRPC(ctx context.Context) error {
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create instance admin client: %v", err)
	}
	defer instanceAdmin.Close()

	for c := 0; c < 40; c++ {
		_, err := instanceAdmin.GetInstance(ctx, &instancepb.GetInstanceRequest{
			Name: "projects/p/instances/i",
		})
		if err == nil || status.Code(err) == codes.NotFound {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("emulator gRPC server did not start in time")
}

func createInstance(projectId, instanceId string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer instanceAdmin.Close()

	inst, err := instanceAdmin.GetInstance(ctx, &instancepb.GetInstanceRequest{
		Name: fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId),
	})
	if err == nil && inst != nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("failed to check if instance exists: %w", err)
	}

	op, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectId),
		InstanceId: instanceId,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/%s", projectId, "emulator-config"),
			DisplayName: instanceId,
			NodeCount:   1,
		},
	})
	if err != nil {
		return fmt.Errorf("could not create instance %s: %v", fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId), err)
	}
	// Wait for the instance creation to finish.
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for instance creation to finish failed: %v", err)
	}
	return nil
}

func createSampleDB(projectId, instanceId, databaseId string, dialect databasepb.DatabaseDialect, statements ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer databaseAdminClient.Close()

	// Drop the database if it already exists.
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId)
	_ = databaseAdminClient.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: dbPath})

	var createStatement string
	if dialect == databasepb.DatabaseDialect_POSTGRESQL {
		createStatement = fmt.Sprintf(`CREATE DATABASE "%s"`, databaseId)
	} else {
		createStatement = fmt.Sprintf("CREATE DATABASE `%s`", databaseId)
	}
	opDB, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId),
		CreateStatement: createStatement,
		ExtraStatements: statements,
		DatabaseDialect: dialect,
	})
	if err != nil {
		return err
	}
	// Wait for the database creation to finish.
	if _, err := opDB.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for database creation to finish failed: %v", err)
	}
	return nil
}

func stopEmulator() {
	if cli == nil || containerId == "" {
		return
	}
	ctx := context.Background()
	timeout := 10
	if err := cli.ContainerStop(ctx, containerId, container.StopOptions{Timeout: &timeout}); err != nil {
		log.Printf("failed to stop emulator: %v\n", err)
	}
	_ = cli.Close()
	cli = nil
	containerId = ""
	_ = os.Unsetenv("SPANNER_EMULATOR_HOST")
}
