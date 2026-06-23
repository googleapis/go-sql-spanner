# Google Cloud Spanner database/sql Driver - Developer Rules for AI Assistants (Maintainers & Contributors)

This repository contains the Google Cloud Spanner driver for Go's `database/sql` package, along with internal packages (`parser`, `connectionstate`, `spannerlib`) and dynamic wrappers for Python, Ruby, .NET, and Java that wrap `spannerlib`.

If you are an AI assistant (or human contributor) modifying or extending this driver, you **must** adhere to the following architectural invariants, coding guidelines, and testing requirements.

---

## 1. Repository Structure & Multi-Language Scope

This is a multi-language monorepo:
- **Go Driver**: The core Go package (`github.com/googleapis/go-sql-spanner`) implements Go's standard `database/sql` driver interfaces.
- **`spannerlib`**: An internal module that compiles to a C-shared library (or similar binding formats) to expose the underlying driver's connection and execution features to non-Go environments.
- **`spannerlib/wrappers`**: Contains wrapper implementations for Python, Ruby, .NET, and Java. These wrappers wrap the C-shared APIs exposed by `spannerlib` (not the Go-specific `database/sql` driver interfaces).

**Rule**: Any modifications to the core Go driver API, connection properties, or behavior may affect the dynamic wrappers. Be mindful of compatibility and update the corresponding wrapper signatures or `spannerlib` wrapper tests if necessary.

---

## 2. Connection State & Properties

The driver implements custom virtual connection properties (e.g. `STATEMENT_TAG`, `OPTIMIZER_VERSION`, `READ_ONLY_STALENESS`) which can be configured via virtual SQL-like statements (`SET ...`, `SHOW ...`).

- All connection properties must be defined and registered globally in `connection_properties.go` using the `createConnectionProperty` helper.
- Connection properties are bound to `connectionstate.ConnectionState`.
- When adding a new property:
  - Define its default value, user mutability (`ContextUser` vs. `ContextStartup`), and type converter/validator.
  - Implement getter/setter methods on the `SpannerConn` interface and the private `conn` struct.
  - Access properties internally using `propertyXYZ.GetValueOrDefault(c.state)` or `propertyXYZ.SetValue(...)`.

---

## 3. Transaction Lifecycle & Internal Concurrency

Although Go's `database/sql` connection pool serializes connection access (making a single `conn` instance thread-safe by execution serialization), you must ensure state changes within transactions are consistent:

- **Aborted Transactions & Internal Retry**: Cloud Spanner may abort read-write transactions at any point (during read, DML, or commit) with an `Aborted` error code. The driver will then internally retry the transaction and verify that the results returned during the retry match the original attempt:
  - If the results match, the transaction continues seamlessly as if nothing happened.
  - If the results differ (e.g. concurrent modification changed the query results), the driver returns `ErrAbortedDueToConcurrentModification` to the application.
- **State Tracking**: The driver tracks the active transaction using `delegatingTransaction`. When Spanner aborts a transaction and the internal retry/verification fails (or if internal retry is disabled), the operation will return an `Aborted` error (or `ErrAbortedDueToConcurrentModification`). The transaction is then considered aborted, and any subsequent operations within that transaction will also fail. The application should roll back the transaction.
- Ensure any transaction lifecycle logic matches the implementations in `transaction.go` and `conn.go`.

---

## 4. Statement Parsing Guidelines

The internal `parser` package handles detecting statement types (DQL vs. DML vs. DDL) and extracting query parameters.

- **Do not** write a full SQL compiler/parser. The parser is designed to be lightweight and fast, using prefix checking and manual string parsing (e.g. in `parser/statement_parser.go`).
- If you add or modify parser logic:
  - Maintain compatibility for both Spanner GoogleSQL and Spanner PostgreSQL statement dialects.
  - Add comprehensive test coverage in `parser/statement_parser_test.go`.

---

## 5. Testing Requirements (Mandatory)

Any pull request modifying or extending the driver's features must include:

- **Mock Server Tests**: Located in files such as `driver_with_mockserver_test.go`, `conn_with_mockserver_test.go`, and `stmt_with_mockserver_test.go`. Use these (or add new test files) to mock Spanner gRPC API responses (e.g. BeginTransaction, Commit, ExecuteSql) and verify that the driver translates options, tags, and states correctly.
- **Emulator Tests**: Validate integration behavior against the Cloud Spanner Emulator (`integration_test.go` and examples). Make sure the test configurations can run locally with `auto_config_emulator=true`.
- **Wrapper Tests**: If you modified `spannerlib`, ensure you trigger or run unit/integration tests for the respective wrappers (`python-spanner-lib-wrapper-unit-tests.yml`, `ruby-wrapper-tests.yml`, etc.).
- **Subtests & Early Failure**: Prefer using Go subtests (`t.Run`) to isolate distinct test cases. When an intermediate setup step fails (e.g., helper function error, unexpected nil, client initialization failure), use `t.Fatalf` or `t.Fatal` to fail and abort that specific test/subtest immediately. Avoid using `t.Errorf` or `t.Error` if subsequent assertions rely on values or state from the failed step, as this results in redundant and confusing secondary error outputs.
