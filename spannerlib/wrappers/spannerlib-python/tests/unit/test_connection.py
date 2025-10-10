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

"""Unit tests for the Connection class."""
from __future__ import absolute_import

import ctypes
import unittest
from unittest.mock import MagicMock, patch

from google.cloud.spanner_v1 import ExecuteBatchDmlRequest, ExecuteSqlRequest

from google.cloud.spannerlib import Connection, Rows, SpannerLibError
from google.cloud.spannerlib.internal import GoReturn


class TestConnection(unittest.TestCase):
    """Unit tests for the Connection class."""

    def setUp(self):
        """Set up the test environment."""
        self.mock_pool = MagicMock()
        self.mock_pool.id = 1
        self.mock_pool.closed = False

        self.conn = Connection(id=123, pool=self.mock_pool)
        self.mock_lib = MagicMock()

    def tearDown(self):
        """Tear down the test environment."""
        if not self.conn.closed:
            try:
                # Minimal mock to avoid errors in close
                with patch(
                    "google.cloud.spannerlib.connection.get_lib"
                ) as mock_get_lib:
                    mock_lib = MagicMock()
                    mock_get_lib.return_value = mock_lib
                    mock_lib.CloseConnection.return_value = GoReturn(
                        pinner_id=0,
                        error_code=0,
                        object_id=0,
                        msg_len=0,
                        msg=None,
                    )
                    self.conn.close()
            except SpannerLibError:
                pass

    @patch("google.cloud.spannerlib.connection.get_lib")
    def test_close_success(self, mock_get_lib):
        """Test the close method in case of success."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.CloseConnection.return_value = GoReturn(
            pinner_id=0, error_code=0, object_id=0, msg_len=0, msg=None
        )

        self.conn.close()
        self.mock_lib.CloseConnection.assert_called_once_with(1, 123)
        self.assertTrue(self.conn.closed)

    @patch("google.cloud.spannerlib.connection.get_lib")
    def test_execute_success(self, mock_get_lib):
        """Test the execute method in case of success."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.Execute.return_value = GoReturn(
            pinner_id=789, error_code=0, object_id=101, msg_len=0, msg=None
        )

        sql = "SELECT 1"
        request = ExecuteSqlRequest(sql=sql)
        rows = self.conn.execute(request)
        rows.close = MagicMock()  # Prevent __del__ from calling mock lib

        self.assertIsInstance(rows, Rows)
        self.assertEqual(rows.id, 101)
        self.assertEqual(rows._pool, self.mock_pool)
        self.assertEqual(rows._conn, self.conn)
        self.mock_lib.Execute.assert_called_once()

    @patch("google.cloud.spannerlib.connection.get_lib")
    def test_execute_failure(self, mock_get_lib):
        """Test the execute method in case of failure."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.Execute.return_value = GoReturn(
            pinner_id=0,
            error_code=1,
            object_id=0,
            msg_len=13,
            msg=ctypes.cast(ctypes.c_char_p(b"Test error"), ctypes.c_void_p),
        )

        sql = "SELECT 1"
        request = ExecuteSqlRequest(sql=sql)
        with self.assertRaises(SpannerLibError):
            self.conn.execute(request)

        self.mock_lib.Execute.assert_called_once()

    def test_execute_closed_connection(self):
        """Test executing on a closed connection."""
        self.conn.closed = True
        with self.assertRaises(RuntimeError):
            sql = "SELECT 1"
            request = ExecuteSqlRequest(sql=sql)
            self.conn.execute(request)

    @patch("google.cloud.spannerlib.connection.get_lib")
    def test_begin_transaction_success(self, mock_get_lib):
        """Test the begin_transaction method in case of success."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.BeginTransaction.return_value = GoReturn(
            pinner_id=0, error_code=0, object_id=0, msg_len=0, msg=None
        )

        self.conn.begin_transaction()
        self.mock_lib.BeginTransaction.assert_called_once()

    @patch("google.cloud.spannerlib.connection.get_lib")
    def test_begin_transaction_failure(self, mock_get_lib):
        """Test the begin_transaction method in case of failure."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.BeginTransaction.return_value = GoReturn(
            pinner_id=0, error_code=1, object_id=0, msg_len=0, msg=None
        )

        with self.assertRaises(SpannerLibError):
            self.conn.begin_transaction()
        self.mock_lib.BeginTransaction.assert_called_once()

    @patch("google.cloud.spannerlib.connection.get_lib")
    def test_commit_success(self, mock_get_lib):
        """Test the commit method in case of success."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.Commit.return_value = GoReturn(
            pinner_id=0, error_code=0, object_id=0, msg_len=0, msg=None
        )

        self.conn.commit()
        self.mock_lib.Commit.assert_called_once_with(1, 123)

    @patch("google.cloud.spannerlib.connection.get_lib")
    def test_commit_failure(self, mock_get_lib):
        """Test the commit method in case of failure."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.Commit.return_value = GoReturn(
            pinner_id=0, error_code=1, object_id=0, msg_len=0, msg=None
        )

        with self.assertRaises(SpannerLibError):
            self.conn.commit()
        self.mock_lib.Commit.assert_called_once_with(1, 123)

    @patch("google.cloud.spannerlib.connection.get_lib")
    def test_rollback_success(self, mock_get_lib):
        """Test the rollback method in case of success."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.Rollback.return_value = GoReturn(
            pinner_id=0, error_code=0, object_id=0, msg_len=0, msg=None
        )

        self.conn.rollback()
        self.mock_lib.Rollback.assert_called_once()

    @patch("google.cloud.spannerlib.connection.get_lib")
    def test_rollback_failure(self, mock_get_lib):
        """Test the rollback method in case of failure."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.Rollback.return_value = GoReturn(
            pinner_id=0, error_code=1, object_id=0, msg_len=0, msg=None
        )

        with self.assertRaises(SpannerLibError):
            self.conn.rollback()
        self.mock_lib.Rollback.assert_called_once()

    @patch("google.cloud.spannerlib.connection.ExecuteBatchDmlResponse")
    @patch("google.cloud.spannerlib.connection.get_lib")
    def test_execute_batch_success(self, mock_get_lib, mock_response_cls):
        """Test the execute_batch method in case of success."""
        mock_get_lib.return_value = self.mock_lib
        mock_deserialize = MagicMock()
        mock_response_cls.deserialize = mock_deserialize
        mock_response_obj = MagicMock()
        mock_deserialize.return_value = mock_response_obj

        # Simulate a serialized response
        dummy_response_bytes = b"dummy"
        self.mock_lib.ExecuteBatch.return_value = GoReturn(
            pinner_id=0,
            error_code=0,
            object_id=0,
            msg_len=len(dummy_response_bytes),
            msg=ctypes.cast(
                ctypes.c_char_p(dummy_response_bytes), ctypes.c_void_p
            ),
        )

        request = ExecuteBatchDmlRequest()
        response = self.conn.execute_batch(request)

        self.mock_lib.ExecuteBatch.assert_called_once()
        mock_deserialize.assert_called_once_with(dummy_response_bytes)
        self.assertIs(response, mock_response_obj)

    @patch("google.cloud.spannerlib.connection.get_lib")
    def test_execute_batch_failure(self, mock_get_lib):
        """Test the execute_batch method in case of failure."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.ExecuteBatch.return_value = GoReturn(
            pinner_id=0, error_code=1, object_id=0, msg_len=0, msg=None
        )

        request = ExecuteBatchDmlRequest()
        with self.assertRaises(SpannerLibError):
            self.conn.execute_batch(request)
        self.mock_lib.ExecuteBatch.assert_called_once()

    def test_execute_batch_closed_connection(self):
        """Test executing batch on a closed connection."""
        self.conn.closed = True
        with self.assertRaises(RuntimeError):
            request = ExecuteBatchDmlRequest()
            self.conn.execute_batch(request)


if __name__ == "__main__":
    unittest.main()
