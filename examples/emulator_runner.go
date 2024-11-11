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
	"os"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
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
	var err error
	if err = startEmulator(); err != nil {
		log.Fatalf("failed to start emulator: %v", err)
	}
	projectId, instanceId, databaseId := "my-project", "my-instance", "my-database"
	if err = createInstance(projectId, instanceId); err != nil {
		stopEmulator()
		log.Fatalf("failed to create instance on emulator: %v", err)
	}
	if err = createSampleDB(projectId, instanceId, databaseId, ddlStatements...); err != nil {
		stopEmulator()
		log.Fatalf("failed to create database on emulator: %v", err)
	}
	err = sample(projectId, instanceId, databaseId)
	stopEmulator()
	if err != nil {
		log.Fatal(err)
	}
}

func startEmulator() error {
	ctx := context.Background()
	if err := os.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010"); err != nil {
		return err
	}

	// Initialize a Docker client.
	var err error
	cli, err = client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	// Pull the Spanner Emulator docker image.
	reader, err := cli.ImagePull(ctx, "gcr.io/cloud-spanner-emulator/emulator", types.ImagePullOptions{})
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
	if err := cli.ContainerStart(ctx, containerId, types.ContainerStartOptions{}); err != nil {
		return err
	}
	// Wait max 10 seconds or until the emulator is running.
	for c := 0; c < 20; c++ {
		// Always wait at least 500 milliseconds to ensure that the emulator is actually ready, as the
		// state can be reported as ready, while the emulator (or network interface) is actually not ready.
		<-time.After(500 * time.Millisecond)
		resp, err := cli.ContainerInspect(ctx, containerId)
		if err != nil {
			return fmt.Errorf("failed to inspect container state: %v", err)
		}
		if resp.State.Running {
			break
		}
	}

	return nil
}

func createInstance(projectId, instanceId string) error {
	ctx := context.Background()
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer instanceAdmin.Close()
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

func createSampleDB(projectId, instanceId, databaseId string, statements ...string) error {
	ctx := context.Background()
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer databaseAdminClient.Close()
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
}
