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

package spannerdriver

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
)

func autoConfigEmulator(ctx context.Context, host, project, instance, database string, dialect databasepb.DatabaseDialect, opts []option.ClientOption) error {
	if err := createInstance(project, instance, opts); err != nil {
		if spanner.ErrCode(err) != codes.AlreadyExists {
			return err
		}
	}
	if err := createDatabase(project, instance, database, dialect, opts); err != nil {
		if spanner.ErrCode(err) != codes.AlreadyExists {
			return err
		}
	}
	return nil
}

func createInstance(projectId, instanceId string, opts []option.ClientOption) error {
	ctx := context.Background()
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		return err
	}
	defer func() { _ = instanceAdmin.Close() }()
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
		return fmt.Errorf("could not create instance %s: %w", fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId), err)
	}
	// Wait for the instance creation to finish.
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for instance creation to finish failed: %v", err)
	}
	return nil
}

func createDatabase(projectId, instanceId, databaseId string, dialect databasepb.DatabaseDialect, opts []option.ClientOption) error {
	ctx := context.Background()
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return err
	}
	defer func() { _ = databaseAdminClient.Close() }()
	createStatement := fmt.Sprintf("CREATE DATABASE `%s`", databaseId)
	if dialect == databasepb.DatabaseDialect_POSTGRESQL {
		createStatement = fmt.Sprintf(`CREATE DATABASE "%s"`, databaseId)
	}
	opDB, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId),
		CreateStatement: createStatement,
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
