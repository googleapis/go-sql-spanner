# go-sql-spanner

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/cloudspannerecosystem/go-sql-spanner)

[Google Cloud Spanner](https://cloud.google.com/spanner) driver for
Go's [database/sql](https://golang.org/pkg/database/sql/) package.


THIS IS A WORK-IN-PROGRESS, DON'T USE IT IN PRODUCTION YET.

``` go
import _ "github.com/cloudspannerecosystem/go-sql-spanner"

db, err := sql.Open("spanner", "projects/PROJECT/instances/INSTANCE/databases/DATABASE")
if err != nil {
    log.Fatal(err)
}

// Print tweets with more than 500 likes.
rows, err := db.QueryContext(ctx, "SELECT id, text FROM tweets WHERE likes > @likes", 500)
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

var (
    id   int64
    text string
)
for rows.Next() {
    if err := rows.Scan(&id, &text); err != nil {
        log.Fatal(err)
    }
    fmt.Println(id, text)
}
```

## Statements

Statements support follows the official [Google Cloud Spanner Go](https://pkg.go.dev/cloud.google.com/go/spanner) client style arguments.

```go
db.QueryContext(ctx, "SELECT id, text FROM tweets WHERE likes > @likes", 500)

db.ExecContext(ctx, "INSERT INTO tweets (id, text, rts) VALUES (@id, @text, @rts)", id, text, 10000)

db.ExecContext(ctx, "DELETE FROM tweets WHERE id = @id", 14544498215374)
```

## Transactions

- Read-only transactions do strong-reads only. Read-only transactions must be ended by calling
either Commit or Rollback. Calling either of these methods will end the current read-only
transaction and return the session that is used to the session pool.
- Read-write transactions always uses the strongest isolation  level and ignore the user-specified level.

``` go
tx, err := db.BeginTx(ctx, &sql.TxOptions{
    ReadOnly: true, // Read-only transaction.
})

tx, err := db.BeginTx(ctx, &sql.TxOptions{}) // Read-write transaction.
```

## Emulator

See the [Google Cloud Spanner Emulator](https://cloud.google.com/spanner/docs/emulator) support to learn how to start the emulator.
Once the emulator is started and the host environmental flag is set, the driver should just work.

```
$ gcloud beta emulators spanner start
$ export SPANNER_EMULATOR_HOST=localhost:9010
```

## Troubleshooting

The driver will retry any Aborted error that is returned by Cloud Spanner
during a read/write transaction. If the driver detects that the data that
was used by the transaction was changed by another transaction between the
initial attempt and the retry attempt, the Aborted error will be propagated
to the client application as an `spannerdriver.ErrAbortedDueToConcurrentModification`
error.

---

[DDLs](https://cloud.google.com/spanner/docs/data-definition-language)
are not supported in transactions per Cloud Spanner restriction.
Instead, run them against the database:

```go
db.ExecContext(ctx, "CREATE TABLE ...")
```

## Disclaimer

This is not an officially supported Google Cloud product.
