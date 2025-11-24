// Copyright 2021 Google LLC
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

package testutil

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// InMemDatabaseAdminServer contains the DatabaseAdminServer interface plus a couple
// of specific methods for setting mocked results.
type InMemDatabaseAdminServer interface {
	databasepb.DatabaseAdminServer
	Stop()
	Resps() []proto.Message
	SetResps([]proto.Message)
	Reqs() []proto.Message
	SetReqs([]proto.Message)
	SetErr(error)
	AddDdlResponse(key string, result *longrunningpb.Operation)
}

// inMemDatabaseAdminServer implements InMemDatabaseAdminServer interface. Note that
// there is no mutex protecting the data structures, so it is not safe for
// concurrent use.
type inMemDatabaseAdminServer struct {
	databasepb.DatabaseAdminServer
	reqs []proto.Message
	// If set, all calls return this error
	err error
	// responses to return if err == nil
	resps []proto.Message

	// Specific results for UpdateDatabaseDdl calls.
	// These results are returned if an UpdateDatabaseDdlRequest corresponds exactly to the key in this map.
	// The key is calculated by concatenating all statements in the UpdateDatabaseDdlRequest into one string separated
	// by semicolons.
	ddlResults map[string]*longrunningpb.Operation
}

// NewInMemDatabaseAdminServer creates a new in-mem test server.
func NewInMemDatabaseAdminServer() InMemDatabaseAdminServer {
	res := &inMemDatabaseAdminServer{ddlResults: make(map[string]*longrunningpb.Operation)}
	return res
}

func (s *inMemDatabaseAdminServer) CreateDatabase(ctx context.Context, req *databasepb.CreateDatabaseRequest) (*longrunningpb.Operation, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*longrunningpb.Operation), nil
}

func (s *inMemDatabaseAdminServer) DropDatabase(ctx context.Context, req *databasepb.DropDatabaseRequest) (*emptypb.Empty, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	return s.resps[0].(*emptypb.Empty), nil
}

func (s *inMemDatabaseAdminServer) UpdateDatabaseDdl(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest) (*longrunningpb.Operation, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if xg := md["x-goog-api-client"]; len(xg) == 0 || !strings.Contains(xg[0], "gl-go/") {
		return nil, fmt.Errorf("x-goog-api-client = %v, expected gl-go key", xg)
	}
	s.reqs = append(s.reqs, req)
	if s.err != nil {
		return nil, s.err
	}
	key := toKey(req)
	if resp, ok := s.ddlResults[key]; ok {
		return resp, nil
	}
	return s.resps[0].(*longrunningpb.Operation), nil
}

func toKey(req *databasepb.UpdateDatabaseDdlRequest) string {
	result := ""
	for i, s := range req.Statements {
		if i > 0 {
			result += ";"
		}
		result += s
	}
	return result
}

func (s *inMemDatabaseAdminServer) Stop() {
	// do nothing
}

func (s *inMemDatabaseAdminServer) Resps() []proto.Message {
	return s.resps
}

func (s *inMemDatabaseAdminServer) SetResps(resps []proto.Message) {
	s.resps = resps
}

func (s *inMemDatabaseAdminServer) Reqs() []proto.Message {
	return s.reqs
}

func (s *inMemDatabaseAdminServer) SetReqs(reqs []proto.Message) {
	s.reqs = reqs
}

func (s *inMemDatabaseAdminServer) SetErr(err error) {
	s.err = err
}

func (s *inMemDatabaseAdminServer) AddDdlResponse(key string, result *longrunningpb.Operation) {
	s.ddlResults[key] = result
}
