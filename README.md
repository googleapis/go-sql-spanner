# go-sql-spanner

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/googleapis/go-sql-spanner)

[Google Cloud Spanner](https://cloud.google.com/spanner) driver for
Go's [database/sql](https://golang.org/pkg/database/sql/) package.

This driver can be used with both Spanner GoogleSQL and Spanner PostgreSQL databases.

```go
import _ "github.com/googleapis/go-sql-spanner"

db, err := sql.Open("spanner", "projects/PROJECT/instances/INSTANCE/databases/DATABASE")
if err != nil {
    log.Fatal(err)
}

// Print singers with more than 500 likes.
rows, err := db.QueryContext(ctx, "SELECT id, name FROM singers WHERE likes > ?", 500)
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

Statements support follows the official [Google Cloud Spanner Go](https://pkg.go.dev/cloud.google.com/go/spanner) client
style arguments as well as positional parameters. It is highly recommended to use either positional parameters in
combination with positional arguments, __or__ named parameters in combination with named arguments.

### Using positional parameters with positional arguments

```go
db.QueryContext(ctx, "SELECT id, text FROM tweets WHERE likes > ?", 500)

db.ExecContext(ctx, "INSERT INTO tweets (id, text, rts) VALUES (?, ?, ?)", id, text, 10000)
```

### Using named parameters with named arguments

```go
db.ExecContext(ctx, "DELETE FROM tweets WHERE id = @id", sql.Named("id", 14544498215374))

db.ExecContext(ctx, "INSERT INTO tweets (id, text, rts) VALUES (@id, @text, @rts)",
	sql.Named("id", id), sql.Named("text", text), sql.Named("rts", 10000))
```

### Using PostgreSQL-style positional arguments

When connected to a PostgreSQL-dialect Spanner database, you can also use PostgreSQL-style
query parameters `$1, $2, ..., $n`. These query parameters are handled as positional parameters.

```go
db.QueryContext(ctx, "SELECT id, text FROM tweets WHERE likes > $1", 500)

db.ExecContext(ctx, "INSERT INTO tweets (id, text, rts) VALUES ($1, $2, $3)", id, text, 10000)
```

### Using named parameters with positional arguments (not recommended)
Named parameters can also be used in combination with positional arguments,
but this is __not recommended__, as the behavior can be hard to predict if
the same named query parameter is used in multiple places in the statement.

```go
// Possible, but not recommended.
db.ExecContext(ctx, "DELETE FROM tweets WHERE id = @id", 14544498215374)
```

### Query Options
Query options can be passed in as arguments to a query. Pass in a value of
type `spannerdriver.ExecOptions` to supply additional execution options for
a statement. The `spanner.QueryOptions` will be passed through to the Spanner
client as the query options to use for the query or DML statement.

```go
tx.ExecContext(ctx, "INSERT INTO Singers (SingerId, Name) VALUES (@id, @name)",
	spannerdriver.ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "insert_singer"}},
    123, "Bruce Allison")
tx.QueryContext(ctx, "SELECT SingerId, Name FROM Singers WHERE SingerId = ?",
    spannerdriver.ExecOptions{QueryOptions: spanner.QueryOptions{RequestTag: "select_singer"}},
    123)
```

Statement tags (request tags) can also be set using the custom SQL statement
`set statement_tag='my_tag'`:

```go
tx.ExecContext(ctx, "set statement_tag = 'select_singer'")
tx.QueryContext(ctx, "SELECT SingerId, Name FROM Singers WHERE SingerId = ?", 123)
```

## Transactions

- Read-write transactions support isolation levels `Serializable`, `Snapshot` and `RepeatableRead`.
- Read-only transactions do strong-reads by default. Read-only transactions must be ended by calling
  either Commit or Rollback. Calling either of these methods will end the current read-only
  transaction.

```go
tx, err := db.BeginTx(ctx, &sql.TxOptions{}) // Read-write transaction.

// Read-write transaction with a transaction tag.
conn, _ := db.Conn(ctx)
_, _ := conn.ExecContext(ctx, "SET TRANSACTION_TAG='my_transaction_tag'")
tx, err := conn.BeginTx(ctx, &sql.TxOptions{})


tx, err := db.BeginTx(ctx, &sql.TxOptions{
    ReadOnly: true, // Read-only transaction using strong reads.
})

conn, _ := db.Conn(ctx)
_, _ := conn.ExecContext(ctx, "SET READ_ONLY_STALENESS='EXACT_STALENESS 10s'")
tx, err := conn.BeginTx(ctx, &sql.TxOptions{
    ReadOnly: true, // Read-only transaction using a 10 second exact staleness.
})
```

## Transaction Runner (Retry Transactions)

Spanner can abort a read/write transaction if concurrent modifications are detected
that would violate the transaction consistency. When this happens, the driver will
return the `ErrAbortedDueToConcurrentModification` error. You can use the
`RunTransaction` and `RunTransactionWithOptions` functions to let the driver
automatically retry transactions that are aborted by Spanner.

```go
package sample

import (
  "context"
  "database/sql"
  "fmt"

  _ "github.com/googleapis/go-sql-spanner"
  spannerdriver "github.com/googleapis/go-sql-spanner"
)

spannerdriver.RunTransactionWithOptions(ctx, db, &sql.TxOptions{}, func(ctx context.Context, tx *sql.Tx) error {
    row := tx.QueryRowContext(ctx, "select Name from Singers where SingerId=@id", 123)
    var name string
    if err := row.Scan(&name); err != nil {
        return err
    }
    return nil
}, spanner.TransactionOptions{TransactionTag: "my_transaction_tag"})
```

See also the [transaction runner sample](./examples/run-transaction/main.go).

## DDL Statements

[DDL statements](https://cloud.google.com/spanner/docs/data-definition-language)
are not supported in transactions per Cloud Spanner restriction.
Instead, run them on a connection without an active transaction:

```go
db.ExecContext(ctx, "CREATE TABLE ...")
```

Multiple DDL statements can be sent in one batch to Cloud Spanner by defining a DDL batch:

```go
conn, _ := db.Conn(ctx)
_, _ := conn.ExecContext(ctx, "START BATCH DDL")
_, _ = conn.ExecContext(ctx, "CREATE TABLE Singers (SingerId INT64, Name STRING(MAX)) PRIMARY KEY (SingerId)")
_, _ = conn.ExecContext(ctx, "CREATE INDEX Idx_Singers_Name ON Singers (Name)")
// Executing `RUN BATCH` will run the previous DDL statements as one batch.
_, _ := conn.ExecContext(ctx, "RUN BATCH")
```

See also [the batch DDL example](/examples/ddl-batches).

## Examples

The [`examples`](/examples) directory contains standalone code samples that show how to use common
features of Cloud Spanner and/or the database/sql package. Each standalone code sample can be
executed without any prior setup, as long as Docker is installed on your local system.

## Raw Connection / Specific Cloud Spanner Features

Use the `Conn.Raw` method to get access to a Cloud Spanner specific connection instance. This
instance can be used to access Cloud Spanner specific features and settings, such as mutations,
read-only staleness settings and commit timestamps.

```go
conn, _ := db.Conn(ctx)
_ = conn.Raw(func(driverConn interface{}) error {
    spannerConn, ok := driverConn.(spannerdriver.SpannerConn)
    if !ok {
        return fmt.Errorf("unexpected driver connection %v, expected SpannerConn", driverConn)
    }
    // Use the `SpannerConn` interface to set specific Cloud Spanner settings or access
    // specific Cloud Spanner features.

    // Example: Set and get the current read-only staleness of the connection.
    _ = spannerConn.SetReadOnlyStaleness(spanner.ExactStaleness(10 * time.Second))
    _ = spannerConn.ReadOnlyStaleness()

    return nil
})
```

See also the [examples](/examples) directory for further code samples.

## Emulator

See [Google Cloud Spanner Emulator](https://cloud.google.com/spanner/docs/emulator) to learn how to start the emulator.
Once the emulator is started and the host environmental flag is set, the driver will automatically connect to the
emulator.

```
$ gcloud beta emulators spanner start
$ export SPANNER_EMULATOR_HOST=localhost:9010
```

### Automatically Create Instance and Database on the Emulator
You can also add the `autoConfigEmulator=true` option to the connection string. This will instruct the driver
to connect to the Spanner emulator, and to automatically create the Spanner instance and database on the
emulator. See [examples/emulator](examples/emulator) for a working example.

## Spanner PostgreSQL Interface

This driver works with both Spanner GoogleSQL and PostgreSQL dialects.
The driver automatically detects the dialect of the database that it is connected to.

Note that the types of query parameters that are supported depends on the dialect of the
database that the driver is connected to:
1. Positional parameters (`?`): Supported for both dialects.
2. Named parameters (`@id`, `@name`, ...): Supported for both dialects.
3. PostgreSQL-style positional parameters (`$1`, `$2`, ...): Only supported for PostgreSQL databases.

### Alternatives

You can also use Spanner PostgreSQL dialect databases with any off-the-shelf PostgreSQL driver that
implements the [database/sql](https://golang.org/pkg/database/sql/) interface in combination with
[PGAdapter](https://cloud.google.com/spanner/docs/pgadapter).

For example, the [pgx](https://github.com/jackc/pgx) driver can be used in combination with
PGAdapter: https://github.com/GoogleCloudPlatform/pgadapter/blob/postgresql-dialect/docs/pgx.md

## Troubleshooting

The driver will retry any Aborted error that is returned by Cloud Spanner
during a read/write transaction. If the driver detects that the data that
was used by the transaction was changed by another transaction between the
initial attempt and the retry attempt, the Aborted error will be propagated
to the client application as an `spannerdriver.ErrAbortedDueToConcurrentModification`
error.

## Go Versions Supported

The Spanner database/sql driver follows the [Go release policy](https://go.dev/doc/devel/release)
and supports the two latest major Go versions.

## Authorization

By default, each API will use [Google Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials)
for authorization credentials used in calling the API endpoints. This will allow your
application to run in many environments without requiring explicit configuration.

## Contributing

Contributions are welcome. Please, see the
[CONTRIBUTING](https://github.com/googleapis/go-sql-spanner/blob/main/CONTRIBUTING.md)
document for details.

Please note that this project is released with a Contributor Code of Conduct.
By participating in this project you agree to abide by its terms.
See [Contributor Code of Conduct](https://github.com/googleapis/go-sql-spanner/blob/main/CODE_OF_CONDUCT.md)
for more information.
