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

from google.cloud.spanner_v1 import ExecuteSqlRequest  # noqa: E402
from google.cloud.spanner_v1 import ResultSetMetadata  # noqa: E402

from google.cloud.spannerlib import Pool  # noqa: E402
from google.cloud.spannerlib.rows import Rows  # noqa: E402


class TestRowsE2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_test_env()
        print(f"Using Connection String: {get_test_connection_string()}")
        try:
            with Pool.create_pool(get_test_connection_string()) as pool:
                with pool.create_connection() as conn:
                    try:
                        conn.execute(
                            ExecuteSqlRequest(sql="DROP TABLE rows_test")
                        )
                    except Exception:
                        pass  # Ignore error if table doesn't exist
                    conn.execute(
                        ExecuteSqlRequest(
                            sql="CREATE TABLE rows_test (id INT64, name STRING(MAX)) PRIMARY KEY (id)"
                        )
                    )
                    conn.execute(
                        ExecuteSqlRequest(
                            sql="INSERT INTO rows_test (id, name) VALUES (1, 'One'), (2, 'Two')"
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

    def test_rows_iteration(self):
        """Test iterating through rows using next()."""
        with self.conn.execute(
            ExecuteSqlRequest(sql="SELECT id, name FROM rows_test ORDER BY id")
        ) as rows:
            self.assertIsInstance(rows, Rows)

            row1 = rows.next()
            self.assertIsNotNone(row1)
            self.assertEqual(row1.values[0].string_value, "1")
            self.assertEqual(row1.values[1].string_value, "One")

            row2 = rows.next()
            self.assertIsNotNone(row2)
            self.assertEqual(row2.values[0].string_value, "2")
            self.assertEqual(row2.values[1].string_value, "Two")

            self.assertIsNone(rows.next())  # No more rows

    def test_rows_metadata(self):
        """Test retrieving metadata."""
        with self.conn.execute(
            ExecuteSqlRequest(sql="SELECT id, name FROM rows_test")
        ) as rows:
            metadata = rows.metadata()
            self.assertIsInstance(metadata, ResultSetMetadata)
            self.assertEqual(len(metadata.row_type.fields), 2)
            self.assertEqual(metadata.row_type.fields[0].name, "id")
            self.assertEqual(metadata.row_type.fields[0].type.code, 2)  # INT64
            self.assertEqual(metadata.row_type.fields[1].name, "name")
            self.assertEqual(metadata.row_type.fields[1].type.code, 6)  # STRING

    def test_rows_context_manager(self):
        """Test that rows are closed when exiting context manager."""
        rows = self.conn.execute(ExecuteSqlRequest(sql="SELECT 1"))
        with rows:
            self.assertFalse(rows.closed)
        self.assertTrue(rows.closed)

    def test_rows_stats_select(self):
        """Test ResultSetStats for a SELECT statement."""
        with self.conn.execute(
            ExecuteSqlRequest(sql="SELECT id, name FROM rows_test")
        ) as rows:
            stats = rows.result_set_stats()
            # Stats are not typically populated for SELECT in the same way as DML
            self.assertIsNotNone(stats)

    def test_ddl_update_count(self):
        """Test update_count for DDL."""
        with self.conn.execute(
            ExecuteSqlRequest(
                sql="CREATE TABLE dummy (id INT64) PRIMARY KEY (id)"
            )
        ) as rows:
            self.assertEqual(rows.update_count(), 0)
        with self.conn.execute(
            ExecuteSqlRequest(sql="DROP TABLE dummy")
        ) as rows:
            self.assertEqual(rows.update_count(), 0)

    def test_dml_update_count(self):
        """Test update_count for DML."""
        with self.conn.execute(
            ExecuteSqlRequest(
                sql="INSERT INTO rows_test (id, name) VALUES (100, 'DML Test')"
            )
        ) as rows:
            self.assertEqual(rows.update_count(), 1)
        # Clean up
        self.conn.execute(
            ExecuteSqlRequest(sql="DELETE FROM rows_test WHERE id = 100")
        )


if __name__ == "__main__":
    unittest.main()
