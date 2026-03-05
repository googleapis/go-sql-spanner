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

import (
	"context"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/proto"
	"spannerlib/api"
)

func Metadata(ctx context.Context, poolId, connId, rowsId int64) *Message {
	metadata, err := api.Metadata(ctx, poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	return encodeMetadata(metadata)
}

func encodeMetadata(metadata *spannerpb.ResultSetMetadata) *Message {
	metadataBytes, err := proto.Marshal(metadata)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: metadataBytes}
}

func ResultSetStats(ctx context.Context, poolId, connId, rowsId int64) *Message {
	stats, err := api.ResultSetStats(ctx, poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	statsBytes, err := proto.Marshal(stats)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: statsBytes}
}

func NextResultSet(ctx context.Context, poolId, connId, rowsId int64) *Message {
	metadata, err := api.NextResultSet(ctx, poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	if metadata == nil {
		return &Message{}
	}
	return encodeMetadata(metadata)
}

func Next(ctx context.Context, poolId, connId, rowsId int64) *Message {
	valuesBytes, err := api.NextEncoded(ctx, poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	return &Message{Res: valuesBytes}
}

func CloseRows(ctx context.Context, poolId, connId, rowsId int64) *Message {
	err := api.CloseRows(ctx, poolId, connId, rowsId)
	if err != nil {
		return errMessage(err)
	}
	return &Message{}
}
