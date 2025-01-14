// Copyright 2025 Google LLC
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

package main

import (
	"context"
	"database/sql"
	"fmt"

	"cloud.google.com/go/spanner"
	spannerdriver "github.com/googleapis/go-sql-spanner"
	"github.com/googleapis/go-sql-spanner/examples"
)

// Example for getting the underlying protobuf objects from a query result
// instead of decoding the values into Go types. This can be used for
// advanced use cases where you want to have full control over how data
// is decoded, or where you want to skip the decode step for performance
// reasons.
func decodeOptions(projectId, instanceId, databaseId string) error {
	ctx := context.Background()
	db, err := sql.Open("spanner", fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId))
	if err != nil {
		return fmt.Errorf("failed to open database connection: %v", err)
	}
	defer db.Close()

	// Pass an ExecOptions value with DecodeOption set to DecodeOptionProto
	// as an argument to QueryContext to instruct the Spanner driver to skip
	// decoding the data into Go types.
	rows, err := db.QueryContext(ctx,
		`SELECT JSON '{"key1": "value1", "key2": 2, "key3": ["value1", "value2"]}'`,
		spannerdriver.ExecOptions{DecodeOption: spannerdriver.DecodeOptionProto})
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		// As we are using DecodeOptionProto, all values must be scanned
		// into spanner.GenericColumnValue.
		var value spanner.GenericColumnValue

		if err := rows.Scan(&value); err != nil {
			return fmt.Errorf("failed to scan row values: %v", err)
		}
		fmt.Printf("Received value %v\n", value.Value.GetStringValue())
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	return nil
}

func main() {
	examples.RunSampleOnEmulator(decodeOptions)
}
