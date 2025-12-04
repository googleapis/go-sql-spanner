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

from google.cloud.spanner_v1 import (
    BatchWriteRequest,
    ExecuteBatchDmlRequest,
    ExecuteSqlRequest,
    Mutation,
)
from google.protobuf.struct_pb2 import ListValue, Value
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


class TestConnectionE2E:
    """End-to-end tests for the Connection class."""

    def test_connection_creation(self, connection):
        """Tests that a connection can be created."""
        assert connection is not None
        assert connection.oid > 0

    def test_execute_query(self, connection):
        """Tests executing a simple SQL query."""
        sql = "SELECT 1"
        request = ExecuteSqlRequest(sql=sql)
        rows = connection.execute(request)
        assert rows is not None
        assert rows.oid > 0
        rows.close()

    def test_execute_batch_dml(self, connection, setup_env):
        """Tests executing a batch of DML statements."""
        # Insert data using DML
        insert_sql = (
            "INSERT INTO Singers (SingerId, FirstName, LastName) "
            "VALUES (1, 'John', 'Doe')"
        )
        # Update data
        update_sql = "UPDATE Singers SET FirstName = 'Jane' WHERE SingerId = 1"

        request = ExecuteBatchDmlRequest(
            statements=[
                ExecuteBatchDmlRequest.Statement(sql=insert_sql),
                ExecuteBatchDmlRequest.Statement(sql=update_sql),
            ]
        )

        response = connection.execute_batch(request)
        assert response is not None
        assert len(response.result_sets) == 2
        assert response.status.code == 0

    def test_write_mutations(self, connection, setup_env):
        """Tests writing mutations."""
        mutation = Mutation(
            insert=Mutation.Write(
                table="Singers",
                columns=["SingerId", "FirstName", "LastName"],
                values=[
                    ListValue(
                        values=[
                            Value(string_value="2"),
                            Value(string_value="Alice"),
                            Value(string_value="Smith"),
                        ]
                    )
                ],
            )
        )

        mutation_group = BatchWriteRequest.MutationGroup(mutations=[mutation])

        response = connection.write_mutations(mutation_group)
        assert response is not None
        assert response.commit_timestamp is not None

    def test_transaction_commit(self, connection, setup_env):
        """Tests a successful transaction commit."""
        connection.begin_transaction()

        # Insert data within transaction
        sql = (
            "INSERT INTO Singers (SingerId, FirstName, LastName) "
            "VALUES (10, 'Transaction', 'Commit')"
        )
        request = ExecuteSqlRequest(sql=sql)
        rows = connection.execute(request)
        rows.close()

        commit_resp = connection.commit()
        assert commit_resp is not None
        assert commit_resp.commit_timestamp is not None

        # Verify data was committed
        verify_sql = "SELECT FirstName FROM Singers WHERE SingerId = 10"
        verify_req = ExecuteSqlRequest(sql=verify_sql)
        verify_rows = connection.execute(verify_req)
        row = verify_rows.next()
        assert row is not None
        assert row.values[0].string_value == "Transaction"
        verify_rows.close()

    def test_transaction_rollback(self, connection, setup_env):
        """Tests a successful transaction rollback."""
        connection.begin_transaction()

        # Insert data within transaction
        sql = (
            "INSERT INTO Singers (SingerId, FirstName, LastName) "
            "VALUES (20, 'Transaction', 'Rollback')"
        )
        request = ExecuteSqlRequest(sql=sql)
        rows = connection.execute(request)
        rows.close()

        connection.rollback()

        # Verify data was NOT committed
        verify_sql = "SELECT FirstName FROM Singers WHERE SingerId = 20"
        verify_req = ExecuteSqlRequest(sql=verify_sql)
        verify_rows = connection.execute(verify_req)
        row = verify_rows.next()
        assert row is None
        verify_rows.close()

    def test_transaction_write_mutations(self, connection, setup_env):
        """Tests writing mutations within a transaction."""
        connection.begin_transaction()

        mutation = Mutation(
            insert=Mutation.Write(
                table="Singers",
                columns=["SingerId", "FirstName", "LastName"],
                values=[
                    ListValue(
                        values=[
                            Value(string_value="30"),
                            Value(string_value="Mutation"),
                            Value(string_value="Transaction"),
                        ]
                    )
                ],
            )
        )

        mutation_group = BatchWriteRequest.MutationGroup(mutations=[mutation])

        response = connection.write_mutations(mutation_group)
        assert response is None

        # If write_mutations commits, this commit() might fail or do nothing.
        # But if it buffers, commit() is needed.
        commit_resp = connection.commit()

        assert commit_resp is not None

        # Verify
        verify_sql = "SELECT FirstName FROM Singers WHERE SingerId = 30"
        verify_req = ExecuteSqlRequest(sql=verify_sql)
        verify_rows = connection.execute(verify_req)
        row = verify_rows.next()
        assert row is not None
        assert row.values[0].string_value == "Mutation"
        verify_rows.close()

    def test_transaction_write_mutations_rollback(self, connection, setup_env):
        """Tests that mutations in a transaction can be rolled back."""
        connection.begin_transaction()

        mutation = Mutation(
            insert=Mutation.Write(
                table="Singers",
                columns=["SingerId", "FirstName", "LastName"],
                values=[
                    ListValue(
                        values=[
                            Value(string_value="40"),
                            Value(string_value="Mutation"),
                            Value(string_value="Rollback"),
                        ]
                    )
                ],
            )
        )

        mutation_group = BatchWriteRequest.MutationGroup(mutations=[mutation])

        response = connection.write_mutations(mutation_group)
        assert response is None
        connection.rollback()

        # Verify data was NOT committed
        verify_sql = "SELECT FirstName FROM Singers WHERE SingerId = 40"
        verify_req = ExecuteSqlRequest(sql=verify_sql)
        verify_rows = connection.execute(verify_req)
        row = verify_rows.next()
        assert row is None
        verify_rows.close()
