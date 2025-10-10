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
from __future__ import absolute_import

import os
import sys
import unittest

from ._helper import get_test_connection_string, setup_test_env

# Adjust path to import from src
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
)

from google.cloud.spanner_v1 import ExecuteBatchDmlRequest  # noqa: E402
from google.cloud.spanner_v1 import ExecuteSqlRequest  # noqa: E402

from google.cloud.spannerlib import Pool, SpannerLibError  # noqa: E402
from google.cloud.spannerlib.rows import Rows  # noqa: E402


class TestConnectionE2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_test_env()
        print(f"Using Connection String: {get_test_connection_string()}")
        try:
            with Pool.create_pool(get_test_connection_string()) as pool:
                with pool.create_connection() as conn:
                    try:
                        conn.execute(
                            ExecuteSqlRequest(sql="DROP TABLE test_table")
                        )
                    except Exception:
                        pass  # Ignore error if table doesn't exist
                    conn.execute(
                        ExecuteSqlRequest(
                            sql="CREATE TABLE test_table (id INT64, name STRING(MAX)) PRIMARY KEY (id)"
                        )
                    )
        except Exception as e:
            print(f"Error in setUpClass: {e}")
            raise

    def setUp(self):
        self.pool = Pool.create_pool(get_test_connection_string())
        self.conn = self.pool.create_connection()

    def tearDown(self):
        if self.conn:
            self.conn.close()
        if self.pool:
            self.pool.close()

    def test_execute_query(self):
        """Test ExecuteSqlRequest with a SELECT statement."""
        request = ExecuteSqlRequest(sql="SELECT 1")
        rows = self.conn.execute(request)
        self.assertIsInstance(rows, Rows)
        row = rows.next()
        self.assertIsNotNone(row)
        self.assertEqual(row.values[0].string_value, "1")
        self.assertIsNone(rows.next())  # No more rows
        rows.close()

    def test_transaction_commit(self):
        """Test begin_transaction, execute, and commit."""
        self.conn.begin_transaction()

        insert_request = ExecuteSqlRequest(
            sql="INSERT INTO test_table (id, name) VALUES (1, 'Test User')"
        )
        self.conn.execute(insert_request)

        self.conn.commit()

        # Verify the insert
        select_request = ExecuteSqlRequest(
            sql="SELECT name FROM test_table WHERE id = 1"
        )
        rows = self.conn.execute(select_request)
        row = rows.next()
        self.assertEqual(row.values[0].string_value, "Test User")
        rows.close()

    def test_transaction_rollback(self):
        """Test begin_transaction, execute, and rollback."""
        self.conn.begin_transaction()

        insert_request = ExecuteSqlRequest(
            sql="INSERT INTO test_table (id, name) VALUES (2, 'Rollback User')"
        )
        self.conn.execute(insert_request)

        self.conn.rollback()

        # Verify the insert was rolled back
        select_request = ExecuteSqlRequest(
            sql="SELECT name FROM test_table WHERE id = 2"
        )
        rows = self.conn.execute(select_request)
        self.assertIsNone(rows.next())
        rows.close()

    def test_execute_batch_dml(self):
        """Test ExecuteBatchDmlRequest with INSERT statements."""
        statements = [
            ExecuteBatchDmlRequest.Statement(
                sql="INSERT INTO test_table (id, name) VALUES (10, 'Batch User 1')"
            ),
            ExecuteBatchDmlRequest.Statement(
                sql="INSERT INTO test_table (id, name) VALUES (11, 'Batch User 2')"
            ),
        ]
        request = ExecuteBatchDmlRequest(statements=statements)

        response = self.conn.execute_batch(request)
        self.assertIsNotNone(response)
        self.assertEqual(len(response.result_sets), 2)
        self.assertEqual(response.status.code, 0)  # OK

        for i, result_set in enumerate(response.result_sets):
            self.assertEqual(result_set.stats.row_count_exact, 1)

        # Verify the inserts
        select_request = ExecuteSqlRequest(
            sql="SELECT name FROM test_table WHERE id IN (10, 11) ORDER BY id"
        )
        rows = self.conn.execute(select_request)
        row = rows.next()
        self.assertEqual(row.values[0].string_value, "Batch User 1")
        row = rows.next()
        self.assertEqual(row.values[0].string_value, "Batch User 2")
        self.assertIsNone(rows.next())
        rows.close()

    def test_execute_batch_dml_with_error(self):
        """Test ExecuteBatchDmlRequest with a statement that causes an error."""
        statements = [
            ExecuteBatchDmlRequest.Statement(
                sql="INSERT INTO test_table (id, name) VALUES (20, 'Good Batch')"
            ),
            ExecuteBatchDmlRequest.Statement(
                sql="INSERT INTO non_existent_table (id, name) VALUES (21, 'Bad Batch')"
            ),
        ]
        request = ExecuteBatchDmlRequest(statements=statements)

        with self.assertRaises(SpannerLibError) as cm:
            self.conn.execute_batch(request)

        self.assertIn("non_existent_table", str(cm.exception))

        # The first statement should have been rolled back
        select_request = ExecuteSqlRequest(
            sql="SELECT name FROM test_table WHERE id = 20"
        )
        rows = self.conn.execute(select_request)
        self.assertIsNone(rows.next())
        rows.close()


if __name__ == "__main__":
    unittest.main()
