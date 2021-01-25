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

- Read-only transactions do strong-reads only.
- Read-write transactions always uses the strongest isolation
level and ignore the user-specified level.

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

This driver shouldn't automatically retry the transactions but it does.
It causes unwanted results. Don't use this library in production yet.

---

gorm cannot use the driver as it-is but @rakyll has been working on a dialect.
She doesn't have bandwidth to ship a fully featured dialect right now but contact
her if you would like to contribute.

---

`error = <use T(nil), not nil>`: Use a typed nil, instead of just nil.

The following query returns rows with NULL likes:

``` go
var nilInt64 *int64
db.QueryContext(ctx, "SELECT id, text FROM tweets WHERE likes = @likes LIMIT 10", nilInt64)
```

---

When querying and executing with emails, pass them as arguments and don't hardcode
them in the query:

``` go
db.QueryContext(ctx, "SELECT id, name ... WHERE email = @email", "jbd@google.com")
```

The driver will relax this requirement in the future but it is a work-in-progress for now.

---

[DDLs](https://cloud.google.com/spanner/docs/data-definition-language)
are not supported in the transactions per Cloud Spanner restriction.
Instead, run them against the database:

```go
db.ExecContext(ctx, "CREATE TABLE ...")
```

## Disclaimer

This is not an officially supported Google Cloud product.
