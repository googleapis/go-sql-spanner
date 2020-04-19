// Copyright 2020 Google Inc. All Rights Reserved.
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

package internal

import (
	"context"
	"errors"

	"cloud.google.com/go/spanner"
)

// RWConnector starts a Cloud Spanner read-write
// transaction and provides blocking APIs to
// query, exec, rollback and commit.
type RWConnector struct {
	QueryIn  chan *RWQueryMessage
	QueryOut chan *RWQueryMessage

	ExecIn  chan *RWExecMessage
	ExecOut chan *RWExecMessage

	RollbackIn chan struct{}
	CommitIn   chan struct{}
	Errors     chan error // only for starting, commit and rollback

	Ready chan struct{}
}

func NewRWConnector(ctx context.Context, c *spanner.Client) *RWConnector {
	connector := &RWConnector{
		QueryIn:    make(chan *RWQueryMessage),
		QueryOut:   make(chan *RWQueryMessage),
		ExecIn:     make(chan *RWExecMessage),
		ExecOut:    make(chan *RWExecMessage),
		RollbackIn: make(chan struct{}),
		CommitIn:   make(chan struct{}),
		Errors:     make(chan error),
		Ready:      make(chan struct{}),
	}

	fn := func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		connector.Ready <- struct{}{}
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case msg := <-connector.QueryIn:
				msg.It = tx.Query(msg.Ctx, msg.Stmt)
				connector.QueryOut <- msg
			case msg := <-connector.ExecIn:
				msg.Rows, msg.Error = tx.Update(msg.Ctx, msg.Stmt)
				connector.ExecOut <- msg
			case <-connector.RollbackIn:
				return ErrAborted
			case <-connector.CommitIn:
				return nil
			}
		}
	}
	go func() {
		_, err := c.ReadWriteTransaction(ctx, fn)
		connector.Errors <- err
	}()
	return connector
}

type RWQueryMessage struct {
	Ctx  context.Context   // in
	Stmt spanner.Statement // in

	It *spanner.RowIterator // out
}

type RWExecMessage struct {
	Ctx  context.Context   // in
	Stmt spanner.Statement // in

	Rows  int64 // out
	Error error // out
}

var ErrAborted = errors.New("aborted")
