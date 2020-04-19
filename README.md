# go-sql-driver-spanner

[Google Cloud Spanner](https://cloud.google.com/spanner) driver for database/sql.


``` go
import _ "github.com/rakyll/go-sql-driver-spanner"

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

## Troubleshooting

`error = <use T(nil), not nil>`: Use a typed nil, instead of just nil.

The following query returns rows with NULL likes:

``` go
var nilInt64 *int64
db.QueryContext(ctx, "SELECT id, text FROM tweets WHERE likes > @likes LIMIT 10", nilInt64)
```

---

When querying and executing with emails, pass them as arguments and don't hardcode
them in the query:

``` go
db.QueryContext(ctx, "SELECT id, name ... WHERE email = @email", "jbd@google.com")
```

## Disclaimer

This is not an officially supported Google Cloud product.
