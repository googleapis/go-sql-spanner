from spannermockserver import start_mock_server
from google.cloud import spanner
from google.auth.credentials import AnonymousCredentials
from google.api_core.client_options import ClientOptions

# 1. Start the mock server
server, spanner_servicer, admin_servicer, port = start_mock_server()

try:
    # 2. Configure the official Spanner client to talk to the mock
    client = spanner.Client(
        project="test-project",
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=f"localhost:{port}")
    )
    
    # 3. Use the client normally
    instance = client.instance("test-instance")
    database = instance.database("test-db")
    
    # You may need to seed results into the mock manually depending on usage
    # spanner_servicer.mock_spanner.add_result("SELECT 1", ...)

finally:
    server.stop(grace=None)
  