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
