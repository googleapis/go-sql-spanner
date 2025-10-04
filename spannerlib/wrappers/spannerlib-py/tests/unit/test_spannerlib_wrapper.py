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
import sys
import unittest
from unittest import mock

from google.cloud.spannerlib import Pool
from google.cloud.spannerlib.internal import GoReturn

# Adjust path to import from src
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
)

TEST_CONNECTION_STRING = (
    "projects/test-project/instances/test-instance/databases/test-database"
)


class TestSpannerLib(unittest.TestCase):
    @mock.patch("google.cloud.spannerlib.internal.spannerlib.Spannerlib._lib")
    def test_pool_creation_and_close(self, mock_lib):
        mock_lib.CreatePool.return_value = GoReturn(
            pinner_id=1, error_code=0, object_id=1, msg_len=0, msg=None
        )
        mock_lib.ClosePool.return_value = GoReturn(
            pinner_id=0, error_code=0, object_id=1, msg_len=0, msg=None
        )

        pool = Pool.create_pool(TEST_CONNECTION_STRING)
        self.assertEqual(pool.pool_id, 1)
        self.assertFalse(pool._closed)
        pool.close()
        self.assertTrue(pool._closed)
        mock_lib.CreatePool.assert_called_once()
        mock_lib.ClosePool.assert_called_once_with(1)

    @mock.patch("google.cloud.spannerlib.internal.spannerlib.Spannerlib._lib")
    def test_connection_creation_and_close(self, mock_lib):
        mock_lib.CreatePool.return_value = GoReturn(
            pinner_id=1, error_code=0, object_id=1, msg_len=0, msg=None
        )
        mock_lib.CreateConnection.return_value = GoReturn(
            pinner_id=2, error_code=0, object_id=101, msg_len=0, msg=None
        )
        mock_lib.CloseConnection.return_value = GoReturn(
            pinner_id=0, error_code=0, object_id=0, msg_len=0, msg=None
        )
        mock_lib.ClosePool.return_value = GoReturn(
            pinner_id=0, error_code=0, object_id=0, msg_len=0, msg=None
        )

        with Pool.create_pool(TEST_CONNECTION_STRING) as pool:
            conn = pool.create_connection()
            self.assertEqual(conn.conn_id, 101)
            self.assertFalse(conn.closed)
            conn.close()
            self.assertTrue(conn.closed)
            mock_lib.CreateConnection.assert_called_once_with(1)
            mock_lib.CloseConnection.assert_called_once_with(1, 101)

        mock_lib.ClosePool.assert_called_once_with(1)

    @mock.patch("google.cloud.spannerlib.internal.spannerlib.Spannerlib._lib")
    def test_connection_with_statement(self, mock_lib):
        mock_lib.CreatePool.return_value = GoReturn(
            pinner_id=1, error_code=0, object_id=1, msg_len=0, msg=None
        )
        mock_lib.CreateConnection.return_value = GoReturn(
            pinner_id=2, error_code=0, object_id=101, msg_len=0, msg=None
        )
        mock_lib.CloseConnection.return_value = GoReturn(
            pinner_id=0, error_code=0, object_id=0, msg_len=0, msg=None
        )
        mock_lib.ClosePool.return_value = GoReturn(
            pinner_id=0, error_code=0, object_id=0, msg_len=0, msg=None
        )

        with Pool.create_pool(TEST_CONNECTION_STRING) as pool:
            with pool.create_connection() as conn:
                self.assertEqual(conn.conn_id, 101)
            mock_lib.CloseConnection.assert_called_once_with(1, 101)
        mock_lib.ClosePool.assert_called_once_with(1)


if __name__ == "__main__":
    unittest.main()
