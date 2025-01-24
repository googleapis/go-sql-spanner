package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/docker/docker/api/types/container"
	"github.com/googleapis/go-sql-spanner/examples/samples"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestSamples(t *testing.T) {
	projectID := "emulator-project"
	instanceID := "test-instance"
	databaseID := "test-database"
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)

	emulator, err := startEmulator(projectID, instanceID, databaseID)
	if err != nil {
		if emulator != nil {
			emulator.Terminate(context.Background())
		}
		t.Fatalf("failed to start emulator: %v", err)

	}
	defer emulator.Terminate(context.Background())

	ctx := context.Background()
	var b bytes.Buffer

	testSample(t, ctx, &b, databaseName, samples.CreateTables, "CreateTables", fmt.Sprintf("Created Singers & Albums tables in database: [%s]\n", databaseName))
	testSample(t, ctx, &b, databaseName, samples.CreateConnection, "CreateConnection", "Greeting from Spanner: Hello world!\n")
	testSample(t, ctx, &b, databaseName, samples.WriteDataWithDml, "WriteDataWithDml", "4 records inserted\n")
	testSample(t, ctx, &b, databaseName, samples.WriteDataWithDmlBatch, "WriteDataWithDmlBatch", "3 records inserted\n")
	testSample(t, ctx, &b, databaseName, samples.WriteDataWithMutations, "WriteDataWithMutations", "Inserted 10 rows\n")
	testSample(t, ctx, &b, databaseName, samples.QueryData, "QueryData", "1 1 Total Junk\n1 2 Go, Go, Go\n2 1 Green\n2 2 Forever Hold Your Peace\n2 3 Terrified\n")
	testSample(t, ctx, &b, databaseName, samples.QueryDataWithParameter, "QueryData", "12 Melissa Garcia\n")
}

func testSample(t *testing.T, ctx context.Context, b *bytes.Buffer, databaseName string, sample func(ctx context.Context, w io.Writer, databaseName string) error, sampleName, want string) {
	if err := sample(ctx, b, databaseName); err != nil {
		t.Fatalf("failed to run %s: %v", sampleName, err)
	}
	if g, w := b.String(), want; g != w {
		t.Fatalf("%s output mismatch\n Got: %v\nWant: %v", sampleName, g, w)
	}
	b.Reset()
}

func startEmulator(projectID, instanceID, databaseID string) (testcontainers.Container, error) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		AlwaysPullImage: true,
		Image:           "gcr.io/cloud-spanner-emulator/emulator",
		ExposedPorts:    []string{"9010/tcp"},
		WaitingFor:      wait.ForListeningPort("9010/tcp"),
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			hostConfig.AutoRemove = true
		},
	}
	emulator, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return emulator, fmt.Errorf("failed to start PGAdapter: %v", err)
	}
	host, err := emulator.Host(ctx)
	if err != nil {
		return emulator, fmt.Errorf("failed to get host: %v", err)
	}
	mappedPort, err := emulator.MappedPort(ctx, "9010/tcp")
	if err != nil {
		return emulator, fmt.Errorf("failed to get mapped port: %v", err)
	}
	port := mappedPort.Int()
	// Set the env var to connec to the emulator.
	if err := os.Setenv("SPANNER_EMULATOR_HOST", fmt.Sprintf("%s:%v", host, port)); err != nil {
		return emulator, fmt.Errorf("failed to set env var for emulator: %v", err)
	}
	if err := createInstance(projectID, instanceID); err != nil {
		return emulator, fmt.Errorf("failed to create instance: %v", err)
	}
	if err := createDatabase(projectID, instanceID, databaseID); err != nil {
		return emulator, fmt.Errorf("failed to create database: %v", err)
	}
	return emulator, nil
}

func createInstance(projectID, instanceID string) error {
	ctx := context.Background()
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer instanceAdmin.Close()

	op, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/%s", projectID, "regional-us-central1"),
			DisplayName: instanceID,
			NodeCount:   1,
			Labels:      map[string]string{"cloud_spanner_samples": "true"},
			Edition:     instancepb.Instance_STANDARD,
		},
	})
	if err != nil {
		return fmt.Errorf("could not create instance %s: %w", fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID), err)
	}
	// Wait for the instance creation to finish.
	if _, err = op.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for instance creation to finish failed: %w", err)
	}
	return nil
}

func createDatabase(projectID, instanceID, databaseID string) error {
	ctx := context.Background()
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer adminClient.Close()

	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(ctx); err != nil {
		return err
	}
	return nil
}
