// Copyright 2026 Google LLC
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

package spannerdriver

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/googleapis/go-sql-spanner/connectionstate"
	"github.com/googleapis/go-sql-spanner/testutil"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestShowSensitiveProperties_GoogleSQL(t *testing.T) {
	t.Parallel()

	database, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}
	defer silentClose(connection)

	testCases := []struct {
		name      string
		statement string
		wantCode  codes.Code
	}{
		{
			name:      "SHOW VARIABLE credentials_json",
			statement: "SHOW VARIABLE credentials_json",
			wantCode:  codes.PermissionDenied,
		},
		{
			name:      "SHOW VARIABLE credentials",
			statement: "SHOW VARIABLE credentials",
			wantCode:  codes.PermissionDenied,
		},
		{
			name:      "SHOW VARIABLE ca_cert_file",
			statement: "SHOW VARIABLE ca_cert_file",
			wantCode:  codes.PermissionDenied,
		},
		{
			name:      "SHOW VARIABLE client_cert_file",
			statement: "SHOW VARIABLE client_cert_file",
			wantCode:  codes.PermissionDenied,
		},
		{
			name:      "SHOW VARIABLE client_cert_key",
			statement: "SHOW VARIABLE client_cert_key",
			wantCode:  codes.PermissionDenied,
		},
		{
			name:      "SHOW VARIABLE CREDENTIALS_JSON uppercase",
			statement: "SHOW VARIABLE CREDENTIALS_JSON",
			wantCode:  codes.PermissionDenied,
		},
		{
			name:      "SHOW VARIABLE Credentials mixed case",
			statement: "SHOW VARIABLE Credentials",
			wantCode:  codes.PermissionDenied,
		},
		{
			name:      "SHOW VARIABLE CLIENT_CERT_KEY uppercase",
			statement: "SHOW VARIABLE CLIENT_CERT_KEY",
			wantCode:  codes.PermissionDenied,
		},
		{
			name:      "SHOW VARIABLE retry_aborts_internally non-sensitive succeeds",
			statement: "SHOW VARIABLE retry_aborts_internally",
			wantCode:  codes.OK,
		},
		{
			name:      "SHOW VARIABLE non_existent_property returns InvalidArgument",
			statement: "SHOW VARIABLE non_existent_property",
			wantCode:  codes.InvalidArgument,
		},
		{
			name:      "SHOW VARIABLE credentialsjson returns InvalidArgument",
			statement: "SHOW VARIABLE credentialsjson",
			wantCode:  codes.InvalidArgument,
		},
		{
			name:      "SHOW VARIABLE three-part identifier returns InvalidArgument",
			statement: "SHOW VARIABLE a.b.c",
			wantCode:  codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rows, err := connection.QueryContext(ctx, testCase.statement)
			if rows != nil {
				_ = rows.Close()
			}
			if g, w := status.Code(err), testCase.wantCode; g != w {
				t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestShowSensitiveProperties_GoogleSQL_ExecContextRejected(t *testing.T) {
	t.Parallel()

	database, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}
	defer silentClose(connection)

	_, err = connection.ExecContext(ctx, "SHOW VARIABLE credentials_json")
	if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestShowSensitiveProperties_PostgreSQL(t *testing.T) {
	t.Parallel()

	database, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}
	defer silentClose(connection)

	testCases := []struct {
		name         string
		statement    string
		wantCode     codes.Code
		wantSQLState string
	}{
		{
			name:         "SHOW credentials_json",
			statement:    "SHOW credentials_json",
			wantCode:     codes.PermissionDenied,
			wantSQLState: "42501",
		},
		{
			name:         "SHOW credentials",
			statement:    "SHOW credentials",
			wantCode:     codes.PermissionDenied,
			wantSQLState: "42501",
		},
		{
			name:         "SHOW ca_cert_file",
			statement:    "SHOW ca_cert_file",
			wantCode:     codes.PermissionDenied,
			wantSQLState: "42501",
		},
		{
			name:         "SHOW client_cert_file",
			statement:    "SHOW client_cert_file",
			wantCode:     codes.PermissionDenied,
			wantSQLState: "42501",
		},
		{
			name:         "SHOW client_cert_key",
			statement:    "SHOW client_cert_key",
			wantCode:     codes.PermissionDenied,
			wantSQLState: "42501",
		},
		{
			name:         "SHOW CREDENTIALS_JSON uppercase",
			statement:    "SHOW CREDENTIALS_JSON",
			wantCode:     codes.PermissionDenied,
			wantSQLState: "42501",
		},
		{
			name:         "SHOW Credentials mixed case",
			statement:    "SHOW Credentials",
			wantCode:     codes.PermissionDenied,
			wantSQLState: "42501",
		},
		{
			name:         "SHOW CLIENT_CERT_KEY uppercase",
			statement:    "SHOW CLIENT_CERT_KEY",
			wantCode:     codes.PermissionDenied,
			wantSQLState: "42501",
		},
		{
			name:         "SHOW server_version non-sensitive succeeds",
			statement:    "SHOW server_version",
			wantCode:     codes.OK,
			wantSQLState: "",
		},
		{
			name:         "SHOW non_existent_property returns InvalidArgument",
			statement:    "SHOW non_existent_property",
			wantCode:     codes.InvalidArgument,
			wantSQLState: "XX000",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rows, err := connection.QueryContext(ctx, testCase.statement)
			if rows != nil {
				_ = rows.Close()
			}
			if g, w := status.Code(err), testCase.wantCode; g != w {
				t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if testCase.wantSQLState != "" {
				if g, w := ToPGSQLState(err), testCase.wantSQLState; g != w {
					t.Errorf("SQLState mismatch\nGot:  %v\nWant: %v", g, w)
				}
			}
		})
	}
}

func TestShowSensitiveProperties_PostgreSQL_ExecContextRejected(t *testing.T) {
	t.Parallel()

	database, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}
	defer silentClose(connection)

	_, err = connection.ExecContext(ctx, "SHOW credentials_json")
	if g, w := status.Code(err), codes.InvalidArgument; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

type setSensitiveTestCase struct {
	name      string
	statement string
}

func commonSetSensitivePropertyTestCases() []setSensitiveTestCase {
	return []setSensitiveTestCase{
		{
			name:      "SET credentials_json value",
			statement: "SET credentials_json = '{\"private_key\":\"abc\"}'",
		},
		{
			name:      "SET credentials_json DEFAULT",
			statement: "SET credentials_json = DEFAULT",
		},
		{
			name:      "RESET credentials_json",
			statement: "RESET credentials_json",
		},
		{
			name:      "SET credentials value",
			statement: "SET credentials = '/path/to/creds.json'",
		},
		{
			name:      "RESET credentials",
			statement: "RESET credentials",
		},
		{
			name:      "SET ca_cert_file value",
			statement: "SET ca_cert_file = '/path/to/ca.crt'",
		},
		{
			name:      "RESET ca_cert_file",
			statement: "RESET ca_cert_file",
		},
		{
			name:      "SET client_cert_file value",
			statement: "SET client_cert_file = '/path/to/client.crt'",
		},
		{
			name:      "RESET client_cert_file",
			statement: "RESET client_cert_file",
		},
		{
			name:      "SET client_cert_key value",
			statement: "SET client_cert_key = '/path/to/client.key'",
		},
		{
			name:      "RESET client_cert_key",
			statement: "RESET client_cert_key",
		},
		{
			name:      "SET CREDENTIALS_JSON uppercase",
			statement: "SET CREDENTIALS_JSON = '{\"private_key\":\"abc\"}'",
		},
		{
			name:      "RESET CREDENTIALS_JSON uppercase",
			statement: "RESET CREDENTIALS_JSON",
		},
	}
}

func runSetSensitivePropertiesTests(t *testing.T, dialect databasepb.DatabaseDialect, testCases []setSensitiveTestCase) {
	database, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", dialect)
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}
	defer silentClose(connection)

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := connection.ExecContext(ctx, testCase.statement)
			if g, w := status.Code(err), codes.PermissionDenied; g != w {
				t.Errorf("ExecContext error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if dialect == databasepb.DatabaseDialect_POSTGRESQL {
				if g, w := ToPGSQLState(err), "42501"; g != w {
					t.Errorf("ExecContext SQLState mismatch\nGot:  %v\nWant: %v", g, w)
				}
			}

			rows, err := connection.QueryContext(ctx, testCase.statement)
			if rows != nil {
				_ = rows.Close()
			}
			if g, w := status.Code(err), codes.PermissionDenied; g != w {
				t.Errorf("QueryContext error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
			if dialect == databasepb.DatabaseDialect_POSTGRESQL {
				if g, w := ToPGSQLState(err), "42501"; g != w {
					t.Errorf("QueryContext SQLState mismatch\nGot:  %v\nWant: %v", g, w)
				}
			}
		})
	}
}

func TestSetSensitiveProperties_GoogleSQL(t *testing.T) {
	t.Parallel()

	testCases := append(commonSetSensitivePropertyTestCases(), setSensitiveTestCase{
		name:      "SET credentials_json NULL",
		statement: "SET credentials_json = NULL",
	})
	runSetSensitivePropertiesTests(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, testCases)
}

func TestSetSensitiveProperties_GoogleSQL_SetLocalInsideTransaction(t *testing.T) {
	t.Parallel()

	database, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}
	defer silentClose(connection)

	transaction, err := connection.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()

	_, err = transaction.ExecContext(ctx, "SET LOCAL credentials_json = 'secret'")
	if g, w := status.Code(err), codes.PermissionDenied; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSetSensitiveProperties_PostgreSQL(t *testing.T) {
	t.Parallel()

	runSetSensitivePropertiesTests(t, databasepb.DatabaseDialect_POSTGRESQL, commonSetSensitivePropertyTestCases())
}

func TestSetSensitiveProperties_PostgreSQL_SetLocalInsideTransaction(t *testing.T) {
	t.Parallel()

	database, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}
	defer silentClose(connection)

	transaction, err := connection.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()

	_, err = transaction.ExecContext(ctx, "SET LOCAL credentials_json = 'secret'")
	if g, w := status.Code(err), codes.PermissionDenied; g != w {
		t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := ToPGSQLState(err), "42501"; g != w {
		t.Errorf("SQLState mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestMemoryHygiene_SensitivePropertiesScrubbedFromConnection(t *testing.T) {
	t.Parallel()

	sensitiveKeys := []string{
		"credentials",
		"credentials_json",
		"ca_cert_file",
		"client_cert_file",
		"client_cert_key",
	}

	// Verify all 5 sensitive properties (including credentials & credentials_json) are scrubbed
	// from initialPropertyValues while non-sensitive properties are preserved.
	allSensitiveConfig := ConnectorConfig{
		Host:     "localhost:9010",
		Project:  "test-project",
		Instance: "test-instance",
		Database: "test-database",
		Params: map[string]string{
			"credentials":             "/nonexistent/credentials.json",
			"credentials_json":        `{"type":"service_account","private_key":"secret-key"}`,
			"ca_cert_file":            "/etc/ssl/ca.crt",
			"client_cert_file":        "/etc/ssl/client.crt",
			"client_cert_key":         "/etc/ssl/client.key",
			"retry_aborts_internally": "false",
		},
	}

	allSensitiveConnector, err := createConnector(&Driver{}, allSensitiveConfig)
	if err != nil {
		t.Fatalf("failed to create connector with all sensitive params: %v", err)
	}
	for _, key := range sensitiveKeys {
		_, ok := allSensitiveConnector.initialPropertyValues[key]
		if g, w := ok, false; g != w {
			t.Errorf("sensitive property %q presence in initialPropertyValues mismatch\nGot:  %v\nWant: %v", key, g, w)
		}
	}
	_, ok := allSensitiveConnector.initialPropertyValues["retry_aborts_internally"]
	if g, w := ok, true; g != w {
		t.Errorf("non-sensitive property retry_aborts_internally presence mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Verify the state constructed from scrubbed initialPropertyValues holds no sensitive values.
	scrubbedState := createInitialConnectionState(connectionstate.TypeNonTransactional, allSensitiveConnector.initialPropertyValues)
	for _, key := range sensitiveKeys {
		val, hasValue, err := scrubbedState.GetValue("", key)
		if err != nil {
			t.Errorf("scrubbedState.GetValue(%q) error mismatch\nGot:  %v\nWant: nil", key, err)
		}
		if g, w := hasValue, false; g != w {
			t.Errorf("scrubbedState.GetValue(%q) hasValue mismatch\nGot:  %v\nWant: %v", key, g, w)
		}
		if g, w := val, ""; g != w {
			t.Errorf("scrubbedState.GetValue(%q) value mismatch\nGot:  %v\nWant: %v", key, g, w)
		}
	}

	// Connect with useplaintext=true (WithoutAuthentication cannot be combined with WithCredentials*
	// on gRPC dial) and TLS key/cert paths to verify live pooled connection state.
	server, _, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	defer serverTeardown()
	server.SetupSelectDialectResult(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)

	connectableConfig := ConnectorConfig{
		Host:     server.Address,
		Project:  "test-project",
		Instance: "test-instance",
		Database: "test-database",
		Params: map[string]string{
			"useplaintext":            "true",
			"ca_cert_file":            "/etc/ssl/ca.crt",
			"client_cert_file":        "/etc/ssl/client.crt",
			"client_cert_key":         "/etc/ssl/client.key",
			"retry_aborts_internally": "false",
		},
	}

	spannerConnector, err := createConnector(&Driver{}, connectableConfig)
	if err != nil {
		t.Fatalf("failed to create connectable connector: %v", err)
	}
	defer func() { _ = spannerConnector.Close() }()

	rawConnection, err := spannerConnector.Connect(context.Background())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer silentClose(rawConnection)

	spannerConnection, ok := rawConnection.(*conn)
	if !ok {
		t.Fatalf("unexpected connection type: %T", rawConnection)
	}

	for _, key := range sensitiveKeys {
		prop, err := spannerConnection.state.FindProperty("", key)
		if err != nil {
			t.Fatalf("FindProperty(%q) failed: %v", key, err)
		}
		if g, w := prop.IsSensitive(), true; g != w {
			t.Errorf("prop.IsSensitive() mismatch for %q\nGot:  %v\nWant: %v", key, g, w)
		}

		_, hasValue, err := spannerConnection.state.GetValue("", key)
		if err != nil {
			t.Errorf("state.GetValue(%q) error mismatch\nGot:  %v\nWant: nil", key, err)
		}
		if g, w := hasValue, false; g != w {
			t.Errorf("state.GetValue(%q) hasValue mismatch\nGot:  %v\nWant: %v", key, g, w)
		}

		rows, err := spannerConnection.QueryContext(context.Background(), "SHOW VARIABLE "+key, nil)
		if rows != nil {
			_ = rows.Close()
		}
		if g, w := status.Code(err), codes.PermissionDenied; g != w {
			t.Errorf("SHOW VARIABLE %s error code mismatch\nGot:  %v\nWant: %v", key, g, w)
		}
	}
}

func TestMemoryHygiene_UnderscoreStrippedSensitivePropertiesScrubbed(t *testing.T) {
	t.Parallel()

	connectorConfig := ConnectorConfig{
		Host:     "localhost:9010",
		Project:  "test-project",
		Instance: "test-instance",
		Database: "test-database",
		Params: map[string]string{
			"useplaintext":    "true",
			"credentialsjson": `{"type":"service_account","private_key":"secret-key"}`,
			"cacertfile":      "/etc/ssl/ca.crt",
			"clientcertfile":  "/etc/ssl/client.crt",
			"clientcertkey":   "/etc/ssl/client.key",
		},
	}

	spannerConnector, err := createConnector(&Driver{}, connectorConfig)
	if err != nil {
		t.Fatalf("failed to create connector: %v", err)
	}

	canonicalKeys := []string{
		"credentials_json",
		"ca_cert_file",
		"client_cert_file",
		"client_cert_key",
	}
	for _, key := range canonicalKeys {
		_, ok := spannerConnector.initialPropertyValues[key]
		if g, w := ok, false; g != w {
			t.Errorf("sensitive property %q presence in initialPropertyValues mismatch\nGot:  %v\nWant: %v", key, g, w)
		}
	}
	// Verify credentials_json was still consumed into client options before being scrubbed.
	if g, w := len(spannerConnector.options) > 0, true; g != w {
		t.Errorf("expected client options to be populated before scrubbing\nGot:  %v\nWant: %v", g, w)
	}
}

func TestMemoryHygiene_OmniSensitivePropertiesPreservedInClientConfig(t *testing.T) {
	t.Parallel()

	connectorConfig := ConnectorConfig{
		Host:     "localhost:9010",
		Project:  "test-project",
		Instance: "test-instance",
		Database: "test-database",
		Params: map[string]string{
			"type":             "OMNI",
			"ca_cert_file":     "/etc/ssl/ca.crt",
			"client_cert_file": "/etc/ssl/client.crt",
			"client_cert_key":  "/etc/ssl/client.key",
		},
	}

	spannerConnector, err := createConnector(&Driver{}, connectorConfig)
	if err != nil {
		t.Fatalf("failed to create OMNI connector: %v", err)
	}

	if g, w := spannerConnector.spannerClientConfig.CaCertificateFile, "/etc/ssl/ca.crt"; g != w {
		t.Errorf("CaCertificateFile mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := spannerConnector.spannerClientConfig.ClientCertificateFile, "/etc/ssl/client.crt"; g != w {
		t.Errorf("ClientCertificateFile mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := spannerConnector.spannerClientConfig.ClientKeyFile, "/etc/ssl/client.key"; g != w {
		t.Errorf("ClientKeyFile mismatch\nGot:  %v\nWant: %v", g, w)
	}

	for _, key := range []string{"ca_cert_file", "client_cert_file", "client_cert_key"} {
		_, ok := spannerConnector.initialPropertyValues[key]
		if g, w := ok, false; g != w {
			t.Errorf("sensitive property %q presence in initialPropertyValues mismatch\nGot:  %v\nWant: %v", key, g, w)
		}
	}
}

func TestSensitiveProperties_DSNRedactionAndLogger(t *testing.T) {
	t.Parallel()

	secretJSON := `{"private_key":"SUPER_SECRET_PRIVATE_KEY"}`
	secretCredsPath := "/secret/path/to/creds.json"
	secretKeyPath := "/secret/path/to/client.key"
	pgPassword := "pg_secret_password"

	tests := []struct {
		name        string
		dsn         string
		wantSecrets []string
		wantString  string
	}{
		{
			name: "GoogleSQL DSN emits target path in String/logger and omits Params in JSON",
			dsn: "spanner.googleapis.com/projects/p/instances/i/databases/d" +
				"?credentials_json=" + secretJSON +
				";credentials=" + secretCredsPath +
				";client_cert_key=" + secretKeyPath +
				";readonly=true",
			wantSecrets: []string{secretJSON, "SUPER_SECRET_PRIVATE_KEY", secretCredsPath, secretKeyPath},
			wantString:  "spanner.googleapis.com/projects/p/instances/i/databases/d",
		},
		{
			name: "GoogleSQL DSN with underscore-stripped params",
			dsn: "localhost:9010/projects/p/instances/i/databases/d" +
				";credentialsjson=" + secretJSON +
				";cacertfile=" + secretCredsPath +
				";usePlainText=true",
			wantSecrets: []string{secretJSON, "SUPER_SECRET_PRIVATE_KEY", secretCredsPath},
			wantString:  "localhost:9010/projects/p/instances/i/databases/d",
		},
		{
			name: "PostgreSQL URL excludes password and query params from String/logger",
			dsn: "postgresql://postgres:" + pgPassword + "@localhost:5432/projects/p/instances/i/databases/d" +
				"?credentials_json=" + secretJSON +
				"&client_cert_key=" + secretKeyPath +
				"&sslmode=disable",
			wantSecrets: []string{pgPassword, secretJSON, "SUPER_SECRET_PRIVATE_KEY", secretKeyPath},
			wantString:  "localhost:5432/projects/p/instances/i/databases/d",
		},
		{
			name: "PostgreSQL URL excludes fragment secrets",
			dsn: "postgresql://localhost:5432/projects/p/instances/i/databases/d" +
				"#credentials_json=SUPER_SECRET_PRIVATE_KEY",
			wantSecrets: []string{"SUPER_SECRET_PRIVATE_KEY"},
			wantString:  "localhost:5432/projects/p/instances/i/databases/d",
		},
		{
			name: "GoogleSQL DSN without host emits projects/instances/databases path",
			dsn: "projects/p/instances/i/databases/d" +
				"?credentials_json=" + secretJSON,
			wantSecrets: []string{secretJSON, "SUPER_SECRET_PRIVATE_KEY"},
			wantString:  "projects/p/instances/i/databases/d",
		},
		{
			name: "OMNI DSN with database only",
			dsn: "localhost:9010/databases/test-db?type=OMNI" +
				";ca_cert_file=" + secretCredsPath,
			wantSecrets: []string{secretCredsPath},
			wantString:  "localhost:9010/projects/default/instances/default/databases/test-db",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			connectorConfig, err := ExtractConnectorConfig(testCase.dsn)
			if err != nil {
				t.Fatalf("ExtractConnectorConfig failed: %v", err)
			}

			var textLogBuffer bytes.Buffer
			var jsonLogBuffer bytes.Buffer
			connectorConfig.logger = slog.New(slog.NewTextHandler(&textLogBuffer, &slog.HandlerOptions{Level: slog.LevelDebug}))

			spannerConnector, err := createConnector(&Driver{}, connectorConfig)
			if err != nil {
				t.Fatalf("createConnector failed: %v", err)
			}
			spannerConnector.logger.Info("testing text log output", "config_val", connectorConfig)

			// Verify raw slog.Attr stored on logger does not hold *ConnectorConfig or Params map.
			attrHandler := &capturingSlogHandler{}
			capturedConfigLogger := slog.New(attrHandler).With("config", connectorConfig.String())
			capturedConfigLogger.Info("checking config attr")
			if attrHandler.configAttrKind != slog.KindString {
				t.Errorf("logger config attr kind mismatch\nGot:  %v\nWant: %v", attrHandler.configAttrKind, slog.KindString)
			}

			jsonLogger := slog.New(slog.NewJSONHandler(&jsonLogBuffer, &slog.HandlerOptions{Level: slog.LevelDebug}))
			jsonLogger.Info("testing json log output", "config_ptr", &connectorConfig, "config_val", connectorConfig)

			jsonBytes, err := json.Marshal(connectorConfig)
			if err != nil {
				t.Fatalf("json.Marshal(ConnectorConfig) failed: %v", err)
			}

			configStr := fmt.Sprint(connectorConfig)
			textLoggedOutput := textLogBuffer.String()
			jsonLoggedOutput := jsonLogBuffer.String()
			marshaledJSON := string(jsonBytes)

			for _, secret := range testCase.wantSecrets {
				if g, w := strings.Contains(configStr, secret), false; g != w {
					t.Errorf("fmt.Sprint(ConnectorConfig) leaked secret %q in %q\nGot:  %v\nWant: %v", secret, configStr, g, w)
				}
				if g, w := strings.Contains(textLoggedOutput, secret), false; g != w {
					t.Errorf("TextHandler output leaked secret %q in %q\nGot:  %v\nWant: %v", secret, textLoggedOutput, g, w)
				}
				if g, w := strings.Contains(jsonLoggedOutput, secret), false; g != w {
					t.Errorf("JSONHandler output leaked secret %q in %q\nGot:  %v\nWant: %v", secret, jsonLoggedOutput, g, w)
				}
				if g, w := strings.Contains(marshaledJSON, secret), false; g != w {
					t.Errorf("json.Marshal(ConnectorConfig) leaked secret %q in %q\nGot:  %v\nWant: %v", secret, marshaledJSON, g, w)
				}
				if g, w := strings.Contains(attrHandler.configAttrStr, secret), false; g != w {
					t.Errorf("slog.Attr string leaked secret %q in %q\nGot:  %v\nWant: %v", secret, attrHandler.configAttrStr, g, w)
				}
			}
			if g, w := configStr, testCase.wantString; g != w {
				t.Errorf("ConnectorConfig.String() mismatch\nGot:  %q\nWant: %q", g, w)
			}
			if g, w := strings.Contains(marshaledJSON, `"Params"`), false; g != w {
				t.Errorf("json.Marshal(ConnectorConfig) unexpectedly included Params in %q\nGot:  %v\nWant: %v", marshaledJSON, g, w)
			}
		})
	}

	t.Run("partial ConnectorConfig String formats cleanly", func(t *testing.T) {
		if g, w := (ConnectorConfig{Host: "localhost:9010", Database: "d"}).String(), "localhost:9010/databases/d"; g != w {
			t.Errorf("partial database ConnectorConfig.String() mismatch\nGot:  %q\nWant: %q", g, w)
		}
		if g, w := (ConnectorConfig{Host: "localhost:9010"}).String(), "localhost:9010"; g != w {
			t.Errorf("host-only ConnectorConfig.String() mismatch\nGot:  %q\nWant: %q", g, w)
		}
		if g, w := (ConnectorConfig{}).String(), ""; g != w {
			t.Errorf("empty ConnectorConfig.String() mismatch\nGot:  %q\nWant: %q", g, w)
		}
	})
}

type capturingSlogHandler struct {
	configAttrKind slog.Kind
	configAttrStr  string
}

func (h *capturingSlogHandler) Enabled(_ context.Context, _ slog.Level) bool  { return true }
func (h *capturingSlogHandler) Handle(_ context.Context, _ slog.Record) error { return nil }
func (h *capturingSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	for _, a := range attrs {
		if a.Key == "config" {
			h.configAttrKind = a.Value.Kind()
			h.configAttrStr = fmt.Sprint(a.Value.Any())
		}
	}
	return h
}
func (h *capturingSlogHandler) WithGroup(_ string) slog.Handler { return h }

func TestSensitiveProperties_InvalidDSNErrorRedaction(t *testing.T) {
	t.Parallel()

	secret := "SUPER_SECRET_PRIVATE_KEY"
	tests := []struct {
		name           string
		dsn            string
		callCreateConn bool
	}{
		{
			name: "malformed GoogleSQL DSN redacts sensitive param",
			dsn:  "invalid/path/only?credentials_json={\"private_key\":\"" + secret + "\"}",
		},
		{
			name: "malformed GoogleSQL param with semicolon does not echo secret",
			dsn:  "localhost:9010/projects/p/instances/i/databases/d?credentials_json={\"key\":\"a;" + secret + "\"}",
		},
		{
			name: "malformed PostgreSQL URL path redacts sensitive query param",
			dsn:  "postgresql://user:pgsecret@localhost:5432/badpath?credentials_json=" + secret,
		},
		{
			name: "malformed PostgreSQL URL RawPath with %20 and semicolon redacts secret",
			dsn:  "postgresql://user:pgsecret@localhost:5432/projects/p/instances/i/databases/d%20;credentials_json=" + secret,
		},
		{
			name: "unparseable PostgreSQL URL redacts sensitive query param",
			dsn:  "postgresql://user:pgsecret@localhost:5432/projects/p/instances/i/databases/d%zz?credentials_json=" + secret,
		},
		{
			name: "unparseable PostgreSQL URL with semicolon path param and no query redacts secret",
			dsn:  "postgresql://user:pgsecret@localhost:5432/projects/p/instances/i/databases/d%zz;credentials_json=" + secret,
		},
		{
			name: "unparseable PostgreSQL URL with fragment redacts fragment and query secrets",
			dsn:  "postgresql://user:pgsecret@localhost:5432/projects/p/instances/i/databases/d%zz?credentials=" + secret + "#credentials_json=" + secret,
		},
		{
			name: "unparseable PostgreSQL URL without password in userinfo redacts query secret",
			dsn:  "postgresql://user@localhost:5432/projects/p/instances/i/databases/d%zz?credentials_json=" + secret,
		},
		{
			name: "unparseable PostgreSQL URL without @ handles email query and secret cleanly",
			dsn:  "postgresql://localhost:5432/projects/p/instances/i/databases/d%zz?email=user@example.com&credentials_json=" + secret,
		},
		{
			name: "unparseable PostgreSQL URL without any @ redacts query secret",
			dsn:  "postgresql://localhost:5432/projects/p/instances/i/databases/d%zz?credentials_json=" + secret,
		},
		{
			name: "unparseable PostgreSQL URL without query redacts password",
			dsn:  "postgresql://user:pgsecret@localhost:5432/projects/p/instances/i/databases/d%zz",
		},
		{
			name: "unparseable PostgreSQL URL with ? inside password redacts password",
			dsn:  "postgresql://user:pg?pgsecret@localhost:5432/projects/p/instances/i/databases/d%zz?credentials_json=" + secret,
		},
		{
			name: "GoogleSQL invalid type OMNI with ampersand + sensitive param redacts error",
			dsn:  "localhost:9010/projects/p/instances/i/databases/d?type=OMNI&credentials_json=" + secret,
		},
		{
			name: "GoogleSQL invalid is_experimental_host with ampersand + sensitive param redacts error",
			dsn:  "localhost:9010/projects/p/instances/i/databases/d?is_experimental_host=true&credentials_json=" + secret,
		},
		{
			name:           "GoogleSQL typed param with ampersand + sensitive param redacts ExtractValues error",
			dsn:            "localhost:9010/projects/p/instances/i/databases/d?useplaintext=true&credentials_json=" + secret,
			callCreateConn: true,
		},
		{
			name:           "GoogleSQL typed param with ampersand + JSON containing ? and & redacts ExtractValues error",
			dsn:            `localhost:9010/projects/p/instances/i/databases/d?useplaintext=true&credentials_json={"token_uri":"https://oauth2.googleapis.com/token?a=1&secret=` + secret + `"}`,
			callCreateConn: true,
		},
		{
			name:           "GoogleSQL typed param with ? separator + sensitive param redacts ExtractValues error",
			dsn:            "localhost:9010/projects/p/instances/i/databases/d?useplaintext=true?credentials_json=" + secret,
			callCreateConn: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			cfg, err := ExtractConnectorConfig(testCase.dsn)
			if testCase.callCreateConn {
				if err != nil {
					t.Fatalf("ExtractConnectorConfig failed unexpectedly: %v", err)
				}
				if g, w := strings.Contains(cfg.String(), secret), false; g != w {
					t.Errorf("ConnectorConfig.String() leaked secret %q in %q\nGot:  %v\nWant: %v", secret, cfg.String(), g, w)
				}
				_, err = createConnector(&Driver{}, cfg)
			}
			if err == nil {
				t.Fatalf("expected error for malformed DSN, got nil")
			}
			if g, w := strings.Contains(err.Error(), secret), false; g != w {
				t.Errorf("error message leaked secret %q in %q\nGot:  %v\nWant: %v", secret, err.Error(), g, w)
			}
			if g, w := strings.Contains(err.Error(), "pgsecret"), false; g != w {
				t.Errorf("error message leaked password in %q\nGot:  %v\nWant: %v", err.Error(), g, w)
			}
		})
	}
}

func TestSensitiveProperties_DriverCacheKeyHashed(t *testing.T) {
	t.Parallel()

	secret1 := "SUPER_SECRET_PRIVATE_KEY_1"
	secret2 := "SUPER_SECRET_PRIVATE_KEY_2"
	dsn1 := "localhost:9010/projects/p/instances/i/databases/d?credentials_json={\"private_key\":\"" + secret1 + "\"};usePlainText=true"
	dsn2 := "localhost:9010/projects/p/instances/i/databases/d?credentials_json={\"private_key\":\"" + secret2 + "\"};usePlainText=true"
	testDriver := &Driver{
		connectors:          make(map[string]*connector),
		connectorUsageCount: make(map[string]int),
	}

	firstConnector, err := newOrCachedConnector(testDriver, dsn1)
	if err != nil {
		t.Fatalf("first newOrCachedConnector failed: %v", err)
	}
	secondConnector, err := newOrCachedConnector(testDriver, dsn1)
	if err != nil {
		t.Fatalf("second newOrCachedConnector failed: %v", err)
	}
	if g, w := firstConnector == secondConnector, true; g != w {
		t.Errorf("cached connector reuse mismatch\nGot:  %v\nWant: %v", g, w)
	}
	distinctConnector, err := newOrCachedConnector(testDriver, dsn2)
	if err != nil {
		t.Fatalf("distinct newOrCachedConnector failed: %v", err)
	}
	if g, w := firstConnector != distinctConnector, true; g != w {
		t.Errorf("distinct connector for differing secret mismatch\nGot:  %v\nWant: %v", g, w)
	}

	if g, w := strings.Contains(firstConnector.cacheKey, secret1), false; g != w {
		t.Errorf("cacheKey leaked secret\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := firstConnector.cacheKey == dsn1, false; g != w {
		t.Errorf("cacheKey must not store raw DSN\nGot:  %v\nWant: %v", g, w)
	}
	for key := range testDriver.connectors {
		if g, w := strings.Contains(key, secret1) || strings.Contains(key, secret2), false; g != w {
			t.Errorf("driver connectors map key leaked secret\nGot:  %v\nWant: %v", g, w)
		}
	}

	// Verify multi-handle refcounted Close: first Close() decrements usageCount while keeping connector cached;
	// second Close() closes the connector and scrubs its secrets.
	if err := firstConnector.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if g, w := testDriver.connectorUsageCount[firstConnector.cacheKey], 1; g != w {
		t.Errorf("usageCount after first Close mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := len(firstConnector.options) > 0, true; g != w {
		t.Errorf("expected connector options to remain while usageCount > 0\nGot:  %v\nWant: %v", g, w)
	}
	if err := secondConnector.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
	if g, w := len(firstConnector.options), 0; g != w {
		t.Errorf("expected connector options to be cleared after final Close\nGot:  %v\nWant: %v", g, w)
	}
	_ = distinctConnector.Close()
}

func TestSensitiveProperties_ExecOptionsPropertyValuesRejected(t *testing.T) {
	t.Parallel()

	database, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}
	defer silentClose(connection)

	for _, tc := range []struct {
		key      string
		wantCode codes.Code
	}{
		{key: "credentials", wantCode: codes.PermissionDenied},
		{key: "credentials_json", wantCode: codes.PermissionDenied},
		{key: "credentialsjson", wantCode: codes.InvalidArgument},
		{key: "ca_cert_file", wantCode: codes.PermissionDenied},
		{key: "cacertfile", wantCode: codes.InvalidArgument},
		{key: "client_cert_file", wantCode: codes.PermissionDenied},
		{key: "client_cert_key", wantCode: codes.PermissionDenied},
	} {
		opts := ExecOptions{
			PropertyValues: []PropertyValue{
				CreatePropertyValue(tc.key, "attacker-secret"),
			},
		}
		rows, queryErr := connection.QueryContext(ctx, "SELECT 1", opts)
		if rows != nil {
			_ = rows.Close()
		}
		if g, w := status.Code(queryErr), tc.wantCode; g != w {
			t.Errorf("QueryContext ExecOptions(%q) error code mismatch\nGot:  %v\nWant: %v", tc.key, g, w)
		}
	}

	options := ExecOptions{
		PropertyValues: []PropertyValue{
			CreatePropertyValue("credentials_json", `{"private_key":"attacker-key"}`),
		},
	}

	// ExecContext with ExecOptions
	_, execErr := connection.ExecContext(ctx, testutil.UpdateBarSetFoo, options)
	if g, w := status.Code(execErr), codes.PermissionDenied; g != w {
		t.Errorf("ExecContext ExecOptions error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Prepared statement QueryContext with ExecOptions value and *ExecOptions pointer.
	stmt, err := connection.PrepareContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("PrepareContext failed: %v", err)
	}
	defer silentClose(stmt)
	stmtRows, err := stmt.QueryContext(ctx, options)
	if stmtRows != nil {
		_ = stmtRows.Close()
	}
	if g, w := status.Code(err), codes.PermissionDenied; g != w {
		t.Errorf("Stmt.QueryContext ExecOptions error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	stmtPtrRows, err := stmt.QueryContext(ctx, &options)
	if stmtPtrRows != nil {
		_ = stmtPtrRows.Close()
	}
	if g, w := status.Code(err), codes.PermissionDenied; g != w {
		t.Errorf("Stmt.QueryContext *ExecOptions error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Prepared statement ExecContext with ExecOptions
	execStmt, err := connection.PrepareContext(ctx, testutil.UpdateBarSetFoo)
	if err != nil {
		t.Fatalf("PrepareContext(UPDATE) failed: %v", err)
	}
	defer silentClose(execStmt)
	_, stmtExecErr := execStmt.ExecContext(ctx, options)
	if g, w := status.Code(stmtExecErr), codes.PermissionDenied; g != w {
		t.Errorf("Stmt.ExecContext ExecOptions error code mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Verify PostgreSQL dialect maps ExecOptions sensitivity rejection to SQLSTATE 42501.
	pgDatabase, _, pgTeardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer pgTeardown()
	pgConn, err := pgDatabase.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open PG database connection: %v", err)
	}
	defer silentClose(pgConn)

	pgRows, pgErr := pgConn.QueryContext(ctx, "SELECT 1", options)
	if pgRows != nil {
		_ = pgRows.Close()
	}
	if g, w := status.Code(pgErr), codes.PermissionDenied; g != w {
		t.Errorf("PG ExecOptions error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := ToPGSQLState(pgErr), "42501"; g != w {
		t.Errorf("PG ExecOptions SQLSTATE mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSensitiveProperties_PreparedAndMultiStatements(t *testing.T) {
	t.Parallel()

	database, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}
	defer silentClose(connection)

	tests := []struct {
		name      string
		statement string
		useQuery  bool
		prepare   bool
		isMulti   bool
	}{
		{
			name:      "prepared SHOW VARIABLE credentials_json",
			statement: "SHOW VARIABLE credentials_json",
			useQuery:  true,
			prepare:   true,
		},
		{
			name:      "prepared SET credentials_json",
			statement: "SET credentials_json = 'secret'",
			useQuery:  false,
			prepare:   true,
		},
		{
			name:      "multi-statement with SHOW VARIABLE credentials_json",
			statement: "SELECT 1; SHOW VARIABLE credentials_json",
			isMulti:   true,
		},
		{
			name:      "multi-statement with SET credentials_json",
			statement: "SELECT 1; SET credentials_json = 'secret'",
			isMulti:   true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var execErr error
			if testCase.prepare {
				stmt, prepErr := connection.PrepareContext(ctx, testCase.statement)
				if prepErr != nil {
					t.Fatalf("PrepareContext failed: %v", prepErr)
				}
				defer silentClose(stmt)
				if testCase.useQuery {
					rows, err := stmt.QueryContext(ctx)
					if rows != nil {
						_ = rows.Close()
					}
					execErr = err
				} else {
					_, execErr = stmt.ExecContext(ctx)
				}
			} else if testCase.isMulti {
				rows, err := connection.QueryContext(ctx, testCase.statement)
				if err != nil {
					t.Fatalf("initial QueryContext failed unexpectedly: %v", err)
				}
				defer silentClose(rows)
				_ = rows.NextResultSet()
				execErr = rows.Err()
			}
			if g, w := status.Code(execErr), codes.PermissionDenied; g != w {
				t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestSensitiveProperties_QuotedAndSyntaxVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		dialect   databasepb.DatabaseDialect
		statement string
		useQuery  bool
	}{
		{
			name:      "GoogleSQL backtick-quoted SHOW VARIABLE credentials_json",
			dialect:   databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			statement: "SHOW VARIABLE `credentials_json`",
			useQuery:  true,
		},
		{
			name:      "GoogleSQL backtick-quoted SET credentials_json",
			dialect:   databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			statement: "SET `credentials_json` = 'secret'",
			useQuery:  false,
		},
		{
			name:      "GoogleSQL backtick-quoted RESET credentials_json",
			dialect:   databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			statement: "RESET `credentials_json`",
			useQuery:  false,
		},
		{
			name:      "PostgreSQL double-quoted SHOW credentials_json",
			dialect:   databasepb.DatabaseDialect_POSTGRESQL,
			statement: `SHOW "credentials_json"`,
			useQuery:  true,
		},
		{
			name:      "PostgreSQL double-quoted SET credentials_json",
			dialect:   databasepb.DatabaseDialect_POSTGRESQL,
			statement: `SET "credentials_json" = 'secret'`,
			useQuery:  false,
		},
		{
			name:      "PostgreSQL double-quoted RESET credentials_json",
			dialect:   databasepb.DatabaseDialect_POSTGRESQL,
			statement: `RESET "credentials_json"`,
			useQuery:  false,
		},
		{
			name:      "PostgreSQL SET SESSION credentials_json",
			dialect:   databasepb.DatabaseDialect_POSTGRESQL,
			statement: "SET SESSION credentials_json = 'secret'",
			useQuery:  false,
		},
		{
			name:      "PostgreSQL SET credentials_json TO value",
			dialect:   databasepb.DatabaseDialect_POSTGRESQL,
			statement: "SET credentials_json TO 'secret'",
			useQuery:  false,
		},
		{
			name:      "PostgreSQL SET credentials_json = NULL",
			dialect:   databasepb.DatabaseDialect_POSTGRESQL,
			statement: "SET credentials_json = NULL",
			useQuery:  false,
		},
		{
			name:      "GoogleSQL block comment SHOW VARIABLE credentials_json",
			dialect:   databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			statement: "/* leading comment */ SHOW VARIABLE credentials_json",
			useQuery:  true,
		},
		{
			name:      "PostgreSQL line comment SHOW credentials_json",
			dialect:   databasepb.DatabaseDialect_POSTGRESQL,
			statement: "-- leading comment\nSHOW credentials_json",
			useQuery:  true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			database, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", testCase.dialect)
			defer teardown()
			ctx := context.Background()

			connection, err := database.Conn(ctx)
			if err != nil {
				t.Fatalf("failed to open database connection: %v", err)
			}
			defer silentClose(connection)

			if testCase.useQuery {
				rows, queryErr := connection.QueryContext(ctx, testCase.statement)
				if rows != nil {
					_ = rows.Close()
				}
				err = queryErr
			} else {
				_, err = connection.ExecContext(ctx, testCase.statement)
			}
			if g, w := status.Code(err), codes.PermissionDenied; g != w {
				t.Errorf("error code mismatch\nGot:  %v\nWant: %v", g, w)
			}
		})
	}
}

func TestSensitiveProperties_PGEnforceInErrorState(t *testing.T) {
	t.Parallel()

	database, _, teardown := setupTestDBConnectionWithParamsAndDialect(t, "", databasepb.DatabaseDialect_POSTGRESQL)
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}
	defer silentClose(connection)

	if err := connection.Raw(func(driverConn any) error {
		spannerConn := driverConn.(SpannerConn)
		spannerConn.SetInErrorTxBehavior(connectionstate.InErrorTxBehaviorEnforceInErrorState)
		return nil
	}); err != nil {
		t.Fatalf("failed to set InErrorTxBehavior: %v", err)
	}

	transaction, err := connection.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()

	rows, err := transaction.QueryContext(ctx, "SHOW credentials_json")
	if rows != nil {
		_ = rows.Close()
	}
	if g, w := status.Code(err), codes.PermissionDenied; g != w {
		t.Fatalf("SHOW credentials_json error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := ToPGSQLState(err), "42501"; g != w {
		t.Errorf("SQLState mismatch\nGot:  %v\nWant: %v", g, w)
	}

	// Subsequent statement in the same transaction must fail with FailedPrecondition (25P02).
	_, err = transaction.ExecContext(ctx, "SELECT 1")
	if g, w := status.Code(err), codes.FailedPrecondition; g != w {
		t.Errorf("subsequent statement error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := ToPGSQLState(err), "25P02"; g != w {
		t.Errorf("subsequent statement SQLState mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSensitiveProperties_ResetSessionRemainsScrubbed(t *testing.T) {
	t.Parallel()

	server, _, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	defer serverTeardown()
	server.SetupSelectDialectResult(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)

	connectorConfig := ConnectorConfig{
		Host:     server.Address,
		Project:  "test-project",
		Instance: "test-instance",
		Database: "test-database",
		Params: map[string]string{
			"useplaintext":     "true",
			"ca_cert_file":     "/etc/ssl/ca.crt",
			"client_cert_file": "/etc/ssl/client.crt",
			"client_cert_key":  "/etc/ssl/client.key",
		},
	}
	spannerConnector, err := createConnector(&Driver{}, connectorConfig)
	if err != nil {
		t.Fatalf("createConnector failed: %v", err)
	}
	defer func() { _ = spannerConnector.Close() }()
	ctx := context.Background()
	rawConn, err := spannerConnector.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer silentClose(rawConn)

	resetter, ok := rawConn.(driver.SessionResetter)
	if !ok {
		t.Fatalf("expected connection to implement driver.SessionResetter")
	}
	if err := resetter.ResetSession(ctx); err != nil {
		t.Fatalf("ResetSession failed: %v", err)
	}

	spannerConnection := rawConn.(*conn)
	for _, key := range []string{"credentials", "credentials_json", "ca_cert_file", "client_cert_file", "client_cert_key"} {
		val, hasValue, err := spannerConnection.state.GetValue("", key)
		if err != nil {
			t.Fatalf("GetValue(%q) failed: %v", key, err)
		}
		if g, w := hasValue, false; g != w {
			t.Errorf("hasValue(%q) after ResetSession mismatch\nGot:  %v\nWant: %v", key, g, w)
		}
		if g, w := val, ""; g != w {
			t.Errorf("value(%q) after ResetSession mismatch\nGot:  %v\nWant: %v", key, g, w)
		}

		rows, err := spannerConnection.QueryContext(ctx, "SHOW VARIABLE "+key, []driver.NamedValue{})
		if rows != nil {
			_ = rows.Close()
		}
		if g, w := status.Code(err), codes.PermissionDenied; g != w {
			t.Errorf("SHOW VARIABLE %s after ResetSession error code mismatch\nGot:  %v\nWant: %v", key, g, w)
		}
	}
}

func TestSensitiveProperties_SqlOpenEndToEnd(t *testing.T) {
	t.Parallel()

	database, _, teardown := setupTestDBConnectionWithParams(t, "ca_cert_file=/etc/ssl/ca.crt;client_cert_file=/etc/ssl/client.crt;client_cert_key=/etc/ssl/client.key")
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open connection via sql.Open: %v", err)
	}
	defer silentClose(connection)

	for _, key := range []string{"ca_cert_file", "client_cert_file", "client_cert_key"} {
		rows, err := connection.QueryContext(ctx, "SHOW VARIABLE "+key)
		if rows != nil {
			_ = rows.Close()
		}
		if g, w := status.Code(err), codes.PermissionDenied; g != w {
			t.Errorf("SHOW VARIABLE %s error code mismatch\nGot:  %v\nWant: %v", key, g, w)
		}

		if err := connection.Raw(func(driverConn any) error {
			spannerConn := driverConn.(*conn)
			_, hasValue, err := spannerConn.state.GetValue("", key)
			if err != nil {
				return err
			}
			if g, w := hasValue, false; g != w {
				t.Errorf("conn.state.GetValue(%q) hasValue mismatch\nGot:  %v\nWant: %v", key, g, w)
			}
			return nil
		}); err != nil {
			t.Fatalf("connection.Raw check failed: %v", err)
		}
	}
}

func TestSensitiveProperties_ShowAlias(t *testing.T) {
	t.Parallel()

	database, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}
	defer silentClose(connection)

	if err := connection.Raw(func(driverConn any) error {
		spannerConn := driverConn.(*conn)
		spannerConn.state.AddAlias("secret_creds_alias", "credentials_json", true /* readOnly */)
		return nil
	}); err != nil {
		t.Fatalf("failed to add alias: %v", err)
	}

	rows, err := connection.QueryContext(ctx, "SHOW VARIABLE secret_creds_alias")
	if rows != nil {
		_ = rows.Close()
	}
	if g, w := status.Code(err), codes.PermissionDenied; g != w {
		t.Errorf("SHOW VARIABLE alias error code mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSensitiveProperties_ConnectorCloseClearsSecrets(t *testing.T) {
	t.Parallel()

	connectorConfig := ConnectorConfig{
		Host:         "localhost:9010",
		Project:      "test-project",
		Instance:     "test-instance",
		Database:     "test-database",
		Configurator: func(*spanner.ClientConfig, *[]option.ClientOption) {},
		Params: map[string]string{
			"type":             "OMNI",
			"credentials_json": `{"type":"service_account","private_key":"secret-key"}`,
			"ca_cert_file":     "/etc/ssl/ca.crt",
			"client_cert_file": "/etc/ssl/client.crt",
			"client_cert_key":  "/etc/ssl/client.key",
		},
	}

	spannerConnector, err := createConnector(&Driver{}, connectorConfig)
	if err != nil {
		t.Fatalf("createConnector failed: %v", err)
	}
	if err := spannerConnector.Close(); err != nil {
		t.Fatalf("connector.Close() failed: %v", err)
	}

	if g, w := len(spannerConnector.options), 0; g != w {
		t.Errorf("connector.options length after Close mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := spannerConnector.spannerClientConfig.CaCertificateFile, ""; g != w {
		t.Errorf("CaCertificateFile after Close mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := spannerConnector.spannerClientConfig.ClientCertificateFile, ""; g != w {
		t.Errorf("ClientCertificateFile after Close mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := spannerConnector.spannerClientConfig.ClientKeyFile, ""; g != w {
		t.Errorf("ClientKeyFile after Close mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := spannerConnector.connectorConfig.Params == nil, true; g != w {
		t.Errorf("connectorConfig.Params nil after Close mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := spannerConnector.connectorConfig.Configurator == nil, true; g != w {
		t.Errorf("connectorConfig.Configurator nil after Close mismatch\nGot:  %v\nWant: %v", g, w)
	}
	if g, w := spannerConnector.initialPropertyValues == nil, true; g != w {
		t.Errorf("initialPropertyValues nil after Close mismatch\nGot:  %v\nWant: %v", g, w)
	}
}

func TestSensitiveProperties_ClientRecreationStaysScrubbed(t *testing.T) {
	t.Parallel()

	server, _, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	defer serverTeardown()
	server.SetupSelectDialectResult(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)

	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true;ca_cert_file=/etc/ssl/ca.crt;client_cert_file=/etc/ssl/client.crt;client_cert_key=/etc/ssl/client.key", server.Address)
	cfg, err := ExtractConnectorConfig(dsn)
	if err != nil {
		t.Fatalf("ExtractConnectorConfig failed: %v", err)
	}
	spannerConnector, err := createConnector(&Driver{}, cfg)
	if err != nil {
		t.Fatalf("createConnector failed: %v", err)
	}
	defer func() { _ = spannerConnector.Close() }()

	ctx := context.Background()
	firstRawConn, err := spannerConnector.Connect(ctx)
	if err != nil {
		t.Fatalf("first Connect failed: %v", err)
	}
	// Closing the only open connection drops connCount to 0 and closes the underlying Spanner client.
	if err := firstRawConn.Close(); err != nil {
		t.Fatalf("first connection Close failed: %v", err)
	}

	// Connecting again must lazily recreate the Spanner client while keeping sensitive state scrubbed.
	secondRawConn, err := spannerConnector.Connect(ctx)
	if err != nil {
		t.Fatalf("second Connect failed after client closed: %v", err)
	}
	defer silentClose(secondRawConn)

	spannerConn := secondRawConn.(*conn)
	rows, err := spannerConn.QueryContext(ctx, "SELECT 1", nil)
	if err != nil {
		t.Fatalf("SELECT 1 after client recreation failed: %v", err)
	}
	_ = rows.Close()

	for _, key := range []string{"ca_cert_file", "client_cert_file", "client_cert_key"} {
		_, hasValue, err := spannerConn.state.GetValue("", key)
		if err != nil {
			t.Fatalf("GetValue(%q) failed: %v", key, err)
		}
		if g, w := hasValue, false; g != w {
			t.Errorf("hasValue(%q) mismatch\nGot:  %v\nWant: %v", key, g, w)
		}

		showRows, err := spannerConn.QueryContext(ctx, "SHOW VARIABLE "+key, nil)
		if showRows != nil {
			_ = showRows.Close()
		}
		if g, w := status.Code(err), codes.PermissionDenied; g != w {
			t.Errorf("SHOW VARIABLE %s error code mismatch\nGot:  %v\nWant: %v", key, g, w)
		}
	}
}

func TestSensitiveProperties_DriverOpen(t *testing.T) {
	t.Parallel()

	server, _, serverTeardown := testutil.NewMockedSpannerInMemTestServer(t)
	defer serverTeardown()
	server.SetupSelectDialectResult(databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL)

	dsn := fmt.Sprintf("%s/projects/p/instances/i/databases/d?useplaintext=true;ca_cert_file=/etc/ssl/ca.crt;client_cert_file=/etc/ssl/client.crt;client_cert_key=/etc/ssl/client.key", server.Address)
	rawDriver := &Driver{}
	rawConn, err := rawDriver.Open(dsn)
	if err != nil {
		t.Fatalf("Driver.Open failed: %v", err)
	}
	defer silentClose(rawConn)

	spannerConn := rawConn.(*conn)
	ctx := context.Background()
	for _, key := range []string{"ca_cert_file", "client_cert_file", "client_cert_key"} {
		_, hasValue, err := spannerConn.state.GetValue("", key)
		if err != nil {
			t.Fatalf("GetValue(%q) failed: %v", key, err)
		}
		if g, w := hasValue, false; g != w {
			t.Errorf("hasValue(%q) mismatch\nGot:  %v\nWant: %v", key, g, w)
		}

		showRows, err := spannerConn.QueryContext(ctx, "SHOW VARIABLE "+key, nil)
		if showRows != nil {
			_ = showRows.Close()
		}
		if g, w := status.Code(err), codes.PermissionDenied; g != w {
			t.Errorf("SHOW VARIABLE %s error code mismatch\nGot:  %v\nWant: %v", key, g, w)
		}
	}
}

func TestSensitiveProperties_InvalidDSNErrorsNeverLeakSecrets(t *testing.T) {
	t.Parallel()

	invalidDSNs := []struct {
		name        string
		dsn         string
		createConn  bool
		wantSecrets []string
	}{
		{
			name:        "GoogleSQL missing database path never echoes credentials",
			dsn:         "localhost:9010/invalid-path?credentials_json=TOPSECRET",
			wantSecrets: []string{"TOPSECRET", "credentials_json"},
		},
		{
			name:        "PG URL missing database path never echoes password or query",
			dsn:         "postgresql://postgres:PG_SECRET_PW@localhost:5432/badpath?credentials_json=TOPSECRET#ca_cert_file=CA_SECRET",
			wantSecrets: []string{"PG_SECRET_PW", "TOPSECRET", "CA_SECRET", "credentials_json"},
		},
		{
			name:        "PG URL parse error never echoes password or secrets",
			dsn:         "postgresql://postgres:PG_SECRET_PW@localhost:invalidport/projects/p/instances/i/databases/d?credentials_json=TOPSECRET",
			wantSecrets: []string{"PG_SECRET_PW", "TOPSECRET", "credentials_json"},
		},
		{
			name:        "mis-delimited type option with &credentials_json never echoes secret",
			dsn:         "localhost:9010/projects/p/instances/i/databases/d?type=CLOUD&credentials_json=TOPSECRET",
			wantSecrets: []string{"TOPSECRET"},
		},
		{
			name:        "mis-delimited isexperimentalhost option with &credentials_json never echoes secret",
			dsn:         "localhost:9010/projects/p/instances/i/databases/d?isexperimentalhost=true&credentials_json=TOPSECRET",
			wantSecrets: []string{"TOPSECRET"},
		},
		{
			name:        "mis-delimited useplaintext option with &credentials_json never echoes secret in ExtractValues",
			dsn:         "localhost:9010/projects/p/instances/i/databases/d?useplaintext=true&credentials_json=TOPSECRET",
			createConn:  true,
			wantSecrets: []string{"TOPSECRET"},
		},
	}

	for _, testCase := range invalidDSNs {
		t.Run(testCase.name, func(t *testing.T) {
			cfg, err := ExtractConnectorConfig(testCase.dsn)
			if testCase.createConn && err == nil {
				_, err = createConnector(&Driver{}, cfg)
			}
			if g, w := status.Code(err), codes.InvalidArgument; g != w {
				t.Errorf("error code mismatch for %q\nGot:  %v\nWant: %v (err=%v)", testCase.dsn, g, w, err)
			}
			for _, secret := range testCase.wantSecrets {
				if g, w := strings.Contains(err.Error(), secret), false; g != w {
					t.Errorf("error leaked secret %q\nGot:  %v\nWant: %v\nError: %s", secret, g, w, err.Error())
				}
			}
		})
	}
}

func TestSensitiveProperties_RepeatedStatementCacheHit(t *testing.T) {
	t.Parallel()

	database, _, teardown := setupTestDBConnection(t)
	defer teardown()
	ctx := context.Background()

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open database connection: %v", err)
	}
	defer silentClose(connection)

	// Run identical SHOW statements twice on the same connection to exercise StatementParser LRU cache hits.
	for attempt := 0; attempt < 2; attempt++ {
		rows, queryErr := connection.QueryContext(ctx, "SHOW VARIABLE credentials_json")
		if rows != nil {
			_ = rows.Close()
		}
		if g, w := status.Code(queryErr), codes.PermissionDenied; g != w {
			t.Errorf("attempt %d SHOW VARIABLE error code mismatch\nGot:  %v\nWant: %v", attempt, g, w)
		}
	}
}
