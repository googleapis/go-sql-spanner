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

package samples

// [START spanner_read_only_transaction]
import (
	"context"
	"database/sql"
	"fmt"
	"io"

	_ "github.com/googleapis/go-sql-spanner"
)

func ReadOnlyTransactionPostgreSQL(ctx context.Context, w io.Writer, databaseName string) error {
	db, err := sql.Open("spanner", databaseName)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Start a read-only transaction by supplying additional transaction options.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}

	albumsOrderedById, err := tx.QueryContext(ctx,
		`select singer_id, album_id, album_title
		from albums
		order by singer_id, album_id`)
	defer func() { _ = albumsOrderedById.Close() }()
	if err != nil {
		return err
	}
	for albumsOrderedById.Next() {
		var singerId, albumId int64
		var title string
		err = albumsOrderedById.Scan(&singerId, &albumId, &title)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(w, "%v %v %v\n", singerId, albumId, title)
	}

	albumsOrderedTitle, err := tx.QueryContext(ctx,
		`select singer_id, album_id, album_title
		from albums
		order by album_title`)
	defer func() { _ = albumsOrderedTitle.Close() }()
	if err != nil {
		return err
	}
	for albumsOrderedTitle.Next() {
		var singerId, albumId int64
		var title string
		err = albumsOrderedTitle.Scan(&singerId, &albumId, &title)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(w, "%v %v %v\n", singerId, albumId, title)
	}

	// End the read-only transaction by calling Commit.
	return tx.Commit()
}

// [END spanner_read_only_transaction]
