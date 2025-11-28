from spannermockserver import start_mock_server
from google.cloud import spanner
from google.auth.credentials import AnonymousCredentials
from google.api_core.client_options import ClientOptions
from google.cloud.spanner_v1.types import result_set, type as spanner_type

# Helper to construct Spanner ResultSets easily
def create_scalar_result(value, col_name="c", code=spanner_type.TypeCode.INT64):
    meta = result_set.ResultSetMetadata(
        row_type=spanner_type.StructType(
            fields=[
                spanner_type.StructType.Field(
                    name=col_name,
                    type=spanner_type.Type(code=code)
                )
            ]
        )
    )
    rs = result_set.ResultSet(metadata=meta)
    # Value must be a string for Spanner gRPC
    from google.protobuf.struct_pb2 import ListValue, Value
    rs.rows.extend([ListValue(values=[Value(string_value=str(value))])])
    return rs

# 1. Start Server
server, spanner_servicer, admin_servicer, port = start_mock_server()

try:
    # 2. Seed the mock with expected behavior
    # Note: The SQL must match exactly (often whitespace/case sensitive depending on implementation)
    mock_result = create_scalar_result(1)
    spanner_servicer.mock_spanner.add_result("SELECT 1", mock_result)

    # 3. Configure Client
    client = spanner.Client(
        project="test-project",
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=f"localhost:{port}")
    )
    instance = client.instance("test-instance")
    database = instance.database("test-db")

    # 4. Execute Code
    with database.snapshot() as snapshot:
        results = list(snapshot.execute_sql("SELECT 1"))
        print(f"Query Result: {results[0][0]}")  # Should print 1

finally:
    server.stop(grace=None)
