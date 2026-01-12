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
import os
import subprocess
import sys

from google.cloud.spanner_v1 import ExecuteSqlRequest, Type, TypeCode
from google.protobuf.struct_pb2 import Struct, Value
import pytest

from google.cloud.spannerlib import Pool

from ._helper import get_test_connection_string, setup_test_env


@pytest.fixture(scope="module", autouse=True)
def test_env():
    """Sets up the test environment for the module."""
    setup_test_env()


@pytest.fixture(scope="module")
def pool():
    """Creates a connection pool for the test module."""
    pool = Pool.create_pool(get_test_connection_string())
    yield pool
    if pool:
        pool.close()


@pytest.fixture()
def connection(pool):
    """Creates a connection from the pool for each test."""
    conn = pool.create_connection()
    yield conn
    if conn:
        conn.close()


@pytest.fixture(scope="module")
def setup_env():
    """Creates the enviroment for testing using a separate process."""
    if os.environ.get("SPANNERLIB_TEST_ON_PROD"):
        return

    # Run setup script in a separate process to avoid gRPC conflicts
    setup_script = os.path.join(os.path.dirname(__file__), "_setup_env.py")
    subprocess.check_call([sys.executable, setup_script, "teardown"])
    subprocess.check_call([sys.executable, setup_script])
    yield


class TestRowsE2E:
    """End-to-end tests for the Rows class."""

    def test_metadata(self, connection):
        """Tests retrieving metadata from a result set."""
        sql = "SELECT 1 AS num"
        request = ExecuteSqlRequest(sql=sql)
        rows = connection.execute(request)

        try:
            metadata = rows.metadata()
            assert metadata is not None
            assert len(metadata.row_type.fields) == 1
            field = metadata.row_type.fields[0]
            assert field.name == "num"
            assert field.type.code == TypeCode.INT64
        finally:
            rows.close()

    def test_stats_and_update_count(self, connection):
        """Tests retrieving result set stats and update count
        from a DML statement."""
        import random

        singer_id = random.randint(1000, 100000)
        sql = (
            "INSERT INTO Singers (SingerId, FirstName, LastName) "
            + f"VALUES ({singer_id}, 'Stats', 'Test')"
        )
        request = ExecuteSqlRequest(sql=sql)
        rows = connection.execute(request)

        try:
            stats = rows.result_set_stats()
            assert stats is not None
            assert stats.row_count_exact == 1

            assert rows.update_count() == 1
        finally:
            rows.close()

    def test_next(self, connection):
        """Tests fetching rows using next()."""
        sql = "SELECT 1 AS num UNION ALL SELECT 2 AS num ORDER BY num"
        request = ExecuteSqlRequest(sql=sql)
        rows = connection.execute(request)

        try:
            # Fetch first row
            row1 = rows.next()
            assert row1 is not None
            assert len(row1.values) == 1
            assert row1.values[0].string_value == "1"

            # Fetch second row
            row2 = rows.next()
            assert row2 is not None
            assert len(row2.values) == 1
            assert row2.values[0].string_value == "2"

            # Fetch end of rows
            assert rows.next() is None
        finally:
            rows.close()

    def test_next_with_params(self, connection):
        """Tests fetching rows using next() with parameters."""
        sql = "SELECT @value AS value"
        params = Struct(fields={"value": Value(string_value="test_param")})
        param_types = {"value": Type(code=TypeCode.STRING)}
        request = ExecuteSqlRequest(
            sql=sql, params=params, param_types=param_types
        )
        rows = connection.execute(request)

        try:
            row = rows.next()
            assert row is not None
            assert len(row.values) == 1
            assert row.values[0].string_value == "test_param"
            assert rows.next() is None
        finally:
            rows.close()

    def test_next_result_set(self, connection):
        """Tests next_result_set() returns None for standard query."""
        sql = "SELECT 1"
        request = ExecuteSqlRequest(sql=sql)
        rows = connection.execute(request)

        try:
            # Consume rows
            while rows.next():
                pass

            # Check next result set
            assert rows.next_result_set() is None
        finally:
            rows.close()

    def test_next_result_set_multiple_statements(self, connection):
        """Tests next_result_set() with multiple statements."""
        sql = "SELECT 1 AS num; SELECT 2 AS num"
        request = ExecuteSqlRequest(sql=sql)
        rows = connection.execute(request)

        try:
            # Consume first result set
            row = rows.next()
            assert row is not None
            assert row.values[0].string_value == "1"
            assert rows.next() is None

            # Check next result set
            metadata = rows.next_result_set()
            assert metadata is not None
            assert len(metadata.row_type.fields) == 1
            assert metadata.row_type.fields[0].name == "num"

            # Consume second result set
            row = rows.next()
            assert row is not None
            assert row.values[0].string_value == "2"
            assert rows.next() is None

            # Check end of result sets
            assert rows.next_result_set() is None
        finally:
            rows.close()
