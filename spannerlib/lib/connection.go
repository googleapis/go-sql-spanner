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

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/proto"
	"spannerlib/api"
)

// CloseConnection closes the Connection with the given ID and returns a Message that indicates whether the operation
// was successful.
func CloseConnection(ctx context.Context, poolId, connId int64) *Message {
	err := api.CloseConnection(ctx, poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	return &Message{}
}

// BeginTransaction starts a new transaction on the given connection. A connection can have at most one active
// transaction at any time. This function therefore returns an error if the connection has an active transaction.
func BeginTransaction(ctx context.Context, poolId, connId int64, txOptsBytes []byte) *Message {
	txOpts := spannerpb.TransactionOptions{}
	if err := proto.Unmarshal(txOptsBytes, &txOpts); err != nil {
		return errMessage(err)
	}
	err := api.BeginTransaction(ctx, poolId, connId, &txOpts)
	if err != nil {
		return errMessage(err)
	}
	return &Message{}
}

// Commit commits the current transaction on the given connection.
func Commit(ctx context.Context, poolId, connId int64) *Message {
	response, err := api.Commit(ctx, poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	if response == nil {
		return &Message{}
	}
	res, err := proto.Marshal(response)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: res}
}

// Rollback rollbacks the current transaction on the given connection.
func Rollback(ctx context.Context, poolId, connId int64) *Message {
	err := api.Rollback(ctx, poolId, connId)
	if err != nil {
		return errMessage(err)
	}
	return &Message{}
}

func Execute(ctx context.Context, poolId, connId int64, executeSqlRequestBytes []byte) (msg *Message) {
	defer func() {
		if r := recover(); r != nil {
			msg = errMessage(fmt.Errorf("panic for message with size %d: %v", len(executeSqlRequestBytes), r))
		}
	}()
	statement := spannerpb.ExecuteSqlRequest{}
	if err := proto.Unmarshal(executeSqlRequestBytes, &statement); err != nil {
		return errMessage(err)
	}
	id, err := api.Execute(ctx, poolId, connId, &statement)
	if err != nil {
		return errMessage(err)
	}
	return idMessage(id)
}
