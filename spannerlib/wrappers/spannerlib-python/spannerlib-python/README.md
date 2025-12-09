# SPANNERLIB-PYTHON: A High-Performance Python Wrapper for the Go Spanner Client Shared lib

> **NOTICE:** This is an internal library that can make breaking changes without prior notice.

## Introduction
The `spannerlib-python` wrapper provides a high-performance, idiomatic Python interface for Google Cloud Spanner by wrapping the official Go Client Shared library.

The Go library is compiled into a C-shared library, and this project calls it directly from Python, aiming to combine Go's performance with Python's ease of use.


## Installation

To install the library, use pip:

```bash
pip install spannerlib-python
```

## Usage

Here is a simple example of how to use the library to execute a SQL query.

### 1. Import the library

```python
from google.cloud.spannerlib import Pool
from google.cloud.spanner_v1 import ExecuteSqlRequest
```

### 2. Create a connection pool

Initialize the pool with your database connection string.

```python
connection_string = "projects/<your-project>/instances/<your-instance>/databases/<your-database>"
pool = Pool.create_pool(connection_string)
```

### 3. Create a connection

```python
conn = pool.create_connection()
```

### 4. Execute a query

Use `ExecuteSqlRequest` to specify your SQL query.

```python
sql = "SELECT 'Hello, World!' as greeting"
request = ExecuteSqlRequest(sql=sql)

# Execute the query
rows = conn.execute(request)

# Iterate over the results
while True:
    row = rows.next()
    if row is None:
        break
    # row is a google.protobuf.struct_pb2.ListValue
    print(row.values[0].string_value)

# Close the rows object when done
rows.close()
```

### 5. Cleanup

Always close the connection and pool to release resources.

```python
conn.close()
pool.close()
```

### Using Context Managers

You can also use the `with` statement to automatically manage resources (close connections and pools).

```python
from google.cloud.spannerlib import Pool
from google.cloud.spanner_v1 import ExecuteSqlRequest

connection_string = "projects/<your-project>/instances/<your-instance>/databases/<your-database>"

# Pool and Connection are context managers
with Pool.create_pool(connection_string) as pool:
    with pool.create_connection() as conn:
        sql = "SELECT 'Hello, World!' as greeting"
        request = ExecuteSqlRequest(sql=sql)
        
        # Rows/Results are also context managers
        with conn.execute(request) as rows:
            for row in iter(rows.next, None):
                print(row.values[0].string_value)
# Resources are automatically closed here
```
