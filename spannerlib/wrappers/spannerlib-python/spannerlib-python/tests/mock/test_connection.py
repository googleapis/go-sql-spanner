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

from google.cloud.spanner_v1 import ExecuteSqlRequest
from spannermockserver import add_result_select_1

from google.cloud.spannerlib import Pool

from ._helper import get_mockserver_connection_string
from .mock_server_test_base_go_driver import MockServerTestBaseGoDriver


class TestConnectionOnMockServer(MockServerTestBaseGoDriver):
    """End-to-end tests for the Connection class."""

    def setUp(self):
        super().setUp()
        self.pool = None
        self.connection = None
        self.pool = Pool.create_pool(
            get_mockserver_connection_string(self.port)
        )
        self.connection = self.pool.create_connection()

    def tearDown(self):
        if self.connection:
            self.connection.close()
        if self.pool:
            self.pool.close()
        super().tearDown()

    def test_connection_creation(self):
        """Tests that a connection can be created."""
        assert self.connection is not None
        assert self.connection.oid > 0

    def test_execute_query(self):
        """Tests executing a simple SQL query."""
        add_result_select_1()
        sql = "SELECT 1"
        request = ExecuteSqlRequest(sql=sql)
        rows = self.connection.execute(request)
        try:
            assert rows is not None
            assert rows.oid > 0
            row = rows.next()
            assert row is not None
            assert row.values[0].string_value == "1"
            assert rows.next() is None
        finally:
            rows.close()
