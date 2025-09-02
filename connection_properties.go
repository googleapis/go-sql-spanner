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

package spannerdriver

import "github.com/googleapis/go-sql-spanner/connectionstate"

// connectionProperties contains all supported connection properties for Spanner.
// These properties are added to all connectionstate.ConnectionState instances that are created for Spanner connections.
var connectionProperties = map[string]connectionstate.ConnectionProperty{}

// The following variables define the various connectionstate.ConnectionProperty instances that are supported and used
// by the Spanner database/sql driver. They are defined as global variables, so they can be used directly in the driver
// to get/set the state of exactly that property.

var propertyConnectionStateType = createConnectionProperty(
	"connection_state_type",
	"The type of connection state to use for this connection. Can only be set at start up. "+
		"If no value is set, then the database dialect default will be used, "+
		"which is NON_TRANSACTIONAL for GoogleSQL and TRANSACTIONAL for PostgreSQL.",
	connectionstate.TypeDefault,
	[]connectionstate.Type{connectionstate.TypeDefault, connectionstate.TypeTransactional, connectionstate.TypeNonTransactional},
	connectionstate.ContextStartup,
)
var propertyAutocommitDmlMode = createConnectionProperty(
	"autocommit_dml_mode",
	"Determines the transaction type that is used to execute DML statements when the connection is in auto-commit mode.",
	Transactional,
	[]AutocommitDMLMode{Transactional, PartitionedNonAtomic},
	connectionstate.ContextUser,
)

func createConnectionProperty[T comparable](name, description string, defaultValue T, validValues []T, context connectionstate.Context) *connectionstate.TypedConnectionProperty[T] {
	prop := connectionstate.CreateConnectionProperty(name, description, defaultValue, validValues, context)
	connectionProperties[prop.Key()] = prop
	return prop
}

func createInitialConnectionState(connectionStateType connectionstate.Type, initialValues map[string]connectionstate.ConnectionPropertyValue) *connectionstate.ConnectionState {
	state, _ := connectionstate.NewConnectionState(connectionStateType, connectionProperties, initialValues)
	return state
}
