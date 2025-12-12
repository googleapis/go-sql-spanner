#  Copyright 2025 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Helper functions for mock tests."""

PROJECT_ID = "p"
INSTANCE_ID = "i"
DATABASE_ID = "d"

MOCK_CONNECTION_STRING = (
    f"projects/{PROJECT_ID}"
    f"/instances/{INSTANCE_ID}"
    f"/databases/{DATABASE_ID}"
    ";usePlainText=true"
)


def get_mockserver_connection_string(port: int) -> str:
    conn_str = f"localhost:{str(port)}" + MOCK_CONNECTION_STRING
    print(f"Mock server connection string: {conn_str}")
    return conn_str
