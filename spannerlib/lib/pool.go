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

package lib

import "C"
import (
	"context"
	"fmt"

	"spannerlib/api"
)

// CreatePool creates a new Pool with the given connection string and returns the ID of the pool in a Message instance.
func CreatePool(ctx context.Context, userAgentSuffix, connectionString string) *Message {
	// Copy the connection string into Go managed memory, as it is stored in the connector config that is created.
	connectionString = fmt.Sprintf("%s", connectionString)

	id, err := api.CreatePool(ctx, userAgentSuffix, connectionString)
	if err != nil {
		return errMessage(err)
	}
	return idMessage(id)
}

// ClosePool closes the Pool with the given ID and returns a Message that indicates whether the operation was
// successful.
func ClosePool(ctx context.Context, id int64) *Message {
	if err := api.ClosePool(ctx, id); err != nil {
		return errMessage(err)
	}
	return &Message{}
}

// CreateConnection creates a new connection in the given pool and returns a Message that contains the ID of the new
// Connection.
func CreateConnection(ctx context.Context, poolId int64) *Message {
	conn, err := api.CreateConnection(ctx, poolId)
	if err != nil {
		return errMessage(err)
	}
	return idMessage(conn)
}
