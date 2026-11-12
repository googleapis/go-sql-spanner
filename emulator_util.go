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
	"regexp"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultEmulatorTimeout = 60 * time.Second

var emulatorSchemeRegex = regexp.MustCompile("^(http://|https://|passthrough:///)")

func configureEmulator(connectorConfig *ConnectorConfig, state *connectionstate.ConnectionState, options []option.ClientOption) ([]option.ClientOption, error) {
	if connectorConfig.Host == "" {
		connectorConfig.Host = "localhost:9010"
	}
	schemeRemoved := emulatorSchemeRegex.ReplaceAllString(connectorConfig.Host, "")
	emulatorOptions := []option.ClientOption{
		option.WithEndpoint("passthrough:///" + schemeRemoved),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		internaloption.SkipDialSettingsValidation(),
	}
	options = append(emulatorOptions, options...)
	emulatorTimeout := defaultEmulatorTimeout
	if duration := propertyConnectTimeout.GetValueOrDefault(state); duration > 0 {
		emulatorTimeout = duration
	}
	emulatorContext, cancel := context.WithTimeout(context.Background(), emulatorTimeout)
	defer cancel()
	if err := autoConfigEmulator(emulatorContext, connectorConfig.Project, connectorConfig.Instance, connectorConfig.Database, propertyDialect.GetValueOrDefault(state), options); err != nil {
		return nil, err
	}
	return options, nil
}

func autoConfigEmulator(ctx context.Context, project, instance, database string, dialect databasepb.DatabaseDialect, opts []option.ClientOption) error {
	if err := createInstance(ctx, project, instance, opts); err != nil {
		if spanner.ErrCode(err) != codes.AlreadyExists {
			return err
		}
	}
	if err := createDatabase(ctx, project, instance, database, dialect, opts); err != nil {
		if spanner.ErrCode(err) != codes.AlreadyExists {
			return err
		}
	}
	return nil
}

func createInstance(ctx context.Context, projectId, instanceId string, opts []option.ClientOption) error {
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
		return fmt.Errorf("waiting for instance creation to finish failed: %w", err)
	}
	return nil
}

func createDatabase(ctx context.Context, projectId, instanceId, databaseId string, dialect databasepb.DatabaseDialect, opts []option.ClientOption) error {
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
		return fmt.Errorf("waiting for database creation to finish failed: %w", err)
	}
	return nil
}
