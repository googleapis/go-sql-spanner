from spannermockserver import start_mock_server
from google.cloud import spanner
from google.auth.credentials import AnonymousCredentials
from google.api_core.client_options import ClientOptions

server, _, admin_servicer, port = start_mock_server()

try:
    client = spanner.Client(
        project="p", credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=f"localhost:{port}")
    )
    database = client.instance("i").database("d")

    # Execute DDL
    operation = database.update_ddl([
        "CREATE TABLE Singers (Id INT64, Name STRING(MAX)) PRIMARY KEY (Id)"
    ])
    operation.result() # Wait for completion

    # Verify Admin Request
    # The mock_database_admin.py stores requests in admin_servicer.requests
    assert len(admin_servicer.requests) > 0
    last_request = admin_servicer.requests[-1]
    
    # Check the DDL statement passed
    print(f"DDL Received: {last_request.statements[0]}")

finally:
    server.stop(grace=None)
