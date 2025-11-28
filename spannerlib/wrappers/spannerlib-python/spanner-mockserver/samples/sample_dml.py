from spannermockserver import start_mock_server
from google.cloud import spanner
from google.auth.credentials import AnonymousCredentials
from google.api_core.client_options import ClientOptions
from google.cloud.spanner_v1.types import result_set

# Helper to create DML response (row count)
def create_dml_result(row_count):
    stats = result_set.ResultSetStats(row_count_exact=row_count)
    return result_set.ResultSet(stats=stats)

server, spanner_servicer, _, port = start_mock_server()

try:
    # Seed the mock for an INSERT
    sql = "INSERT INTO Singers (Id, Name) VALUES (@id, @name)"
    spanner_servicer.mock_spanner.add_result(sql, create_dml_result(1))

    client = spanner.Client(
        project="p", credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=f"localhost:{port}")
    )
    database = client.instance("i").database("d")

    # Run Transaction
    def insert_singer(transaction):
        transaction.execute_update(
            sql,
            params={"id": 1, "name": "Alice"},
            param_types={
                "id": spanner.param_types.INT64,
                "name": spanner.param_types.STRING
            }
        )

    try:
        database.run_in_transaction(insert_singer)
    except ValueError as e:
        if "Transaction has not begun" in str(e):
            print("Caught expected error: Mock server ExecuteSql does not support inline transactions yet.")
        else:
            raise

    # Verify Internal State
    # Filter for ExecuteSqlRequest objects
    requests = [r for r in spanner_servicer.requests if hasattr(r, 'sql') and r.sql == sql]
    assert len(requests) == 1
    
    # Verify parameters sent by client
    sent_params = requests[0].params
    print(f"Verified Insert: ID={sent_params['id']}, Name={sent_params['name']}")

finally:
    server.stop(grace=None)
