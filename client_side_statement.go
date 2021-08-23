// Copyright 2021 Google LLC All Rights Reserved.
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
	"database/sql/driver"
)

type statementExecutor struct {
}

func (s *statementExecutor) ShowRetryAbortsInternally(query string) error {
	return nil
}

func (s *statementExecutor) ShowAutocommitDmlMode(query string) error {
	return nil
}

func (s *statementExecutor) StartBatchDdl(ctx context.Context, c *conn, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.startBatchDdl()
}

func (s *statementExecutor) StartBatchDml(query string) error {
	return nil
}

func (s *statementExecutor) RunBatch(ctx context.Context, c *conn, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.runBatch(ctx)
}

func (s *statementExecutor) AbortBatch(query string) error {
	return nil
}

func (s *statementExecutor) SetRetryAbortsInternally(query string) error {
	return nil
}

func (s *statementExecutor) SetAutocommitDmlMode(query string) error {
	return nil
}
