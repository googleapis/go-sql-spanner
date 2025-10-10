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

"""Unit tests for the Rows class."""
from __future__ import absolute_import

import ctypes
import unittest
from unittest.mock import MagicMock, patch

from google.cloud.spanner_v1 import ResultSetMetadata, ResultSetStats
from google.protobuf.struct_pb2 import ListValue, Value

from google.cloud.spannerlib import Rows, SpannerLibError
from google.cloud.spannerlib.internal import GoReturn


class TestRows(unittest.TestCase):
    """Unit tests for the Rows class."""

    def setUp(self):
        """Set up the test environment."""
        self.mock_pool = MagicMock()
        self.mock_pool.id = 1
        self.mock_pool.closed = False

        self.mock_conn = MagicMock()
        self.mock_conn.id = 123
        self.mock_conn.closed = False

        self.rows = Rows(id=101, pool=self.mock_pool, conn=self.mock_conn)
        self.mock_lib = MagicMock()

    def tearDown(self):
        """Tear down the test environment."""
        if not self.rows.closed:
            try:
                # Minimal mock to avoid errors in close
                with patch(
                    "google.cloud.spannerlib.rows.get_lib"
                ) as mock_get_lib:
                    mock_lib = MagicMock()
                    mock_get_lib.return_value = mock_lib
                    mock_lib.CloseRows.return_value = GoReturn(
                        pinner_id=0,
                        error_code=0,
                        object_id=0,
                        msg_len=0,
                        msg=None,
                    )
                    self.rows.close()
            except SpannerLibError:
                pass

    @patch("google.cloud.spannerlib.rows.get_lib")
    def test_close_success(self, mock_get_lib):
        """Test the close method."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.CloseRows.return_value = GoReturn(
            pinner_id=0, error_code=0, object_id=0, msg_len=0, msg=None
        )
        self.rows.close()
        self.mock_lib.CloseRows.assert_called_once_with(1, 123, 101)
        self.assertTrue(self.rows.closed)

    @patch("google.cloud.spannerlib.rows.get_lib")
    def test_metadata_success(self, mock_get_lib):
        """Test metadata success."""
        mock_get_lib.return_value = self.mock_lib
        metadata = ResultSetMetadata()
        metadata_bytes = ResultSetMetadata.serialize(metadata)
        self.mock_lib.Metadata.return_value = GoReturn(
            pinner_id=0,
            error_code=0,
            object_id=0,
            msg_len=len(metadata_bytes),
            msg=ctypes.cast(ctypes.c_char_p(metadata_bytes), ctypes.c_void_p),
        )

        result = self.rows.metadata()
        self.assertIsInstance(result, ResultSetMetadata)
        self.mock_lib.Metadata.assert_called_once_with(1, 123, 101)

    @patch("google.cloud.spannerlib.rows.get_lib")
    def test_metadata_failure(self, mock_get_lib):
        """Test metadata failure."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.Metadata.return_value = GoReturn(
            pinner_id=0, error_code=1, object_id=0, msg_len=0, msg=None
        )
        with self.assertRaises(SpannerLibError):
            self.rows.metadata()

    def test_metadata_closed(self):
        """Test metadata on closed Rows."""
        self.rows.closed = True
        with self.assertRaises(RuntimeError):
            self.rows.metadata()

    @patch("google.cloud.spannerlib.rows.get_lib")
    def test_next_success(self, mock_get_lib):
        """Test next success."""
        mock_get_lib.return_value = self.mock_lib
        list_value = ListValue(values=[Value(string_value="test")])
        list_value_bytes = list_value.SerializeToString()
        self.mock_lib.Next.return_value = GoReturn(
            pinner_id=0,
            error_code=0,
            object_id=0,
            msg_len=len(list_value_bytes),
            msg=ctypes.cast(ctypes.c_char_p(list_value_bytes), ctypes.c_void_p),
        )

        result = self.rows.next()
        self.assertIsInstance(result, ListValue)
        self.assertEqual(result.values[0].string_value, "test")
        self.mock_lib.Next.assert_called_once_with(1, 123, 101, 1, 1)

    @patch("google.cloud.spannerlib.rows.get_lib")
    def test_next_no_more_rows(self, mock_get_lib):
        """Test next when no more rows."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.Next.return_value = GoReturn(
            pinner_id=0, error_code=0, object_id=0, msg_len=0, msg=None
        )
        result = self.rows.next()
        self.assertIsNone(result)

    @patch("google.cloud.spannerlib.rows.get_lib")
    def test_next_failure(self, mock_get_lib):
        """Test next failure."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.Next.return_value = GoReturn(
            pinner_id=0, error_code=1, object_id=0, msg_len=0, msg=None
        )
        with self.assertRaises(SpannerLibError):
            self.rows.next()

    def test_next_closed(self):
        """Test next on closed Rows."""
        self.rows.closed = True
        with self.assertRaises(RuntimeError):
            self.rows.next()

    @patch("google.cloud.spannerlib.rows.get_lib")
    def test_result_set_stats_success(self, mock_get_lib):
        """Test result_set_stats success."""
        mock_get_lib.return_value = self.mock_lib
        stats = ResultSetStats(row_count_exact=5)
        stats_bytes = ResultSetStats.serialize(stats)
        self.mock_lib.ResultSetStats.return_value = GoReturn(
            pinner_id=0,
            error_code=0,
            object_id=0,
            msg_len=len(stats_bytes),
            msg=ctypes.cast(ctypes.c_char_p(stats_bytes), ctypes.c_void_p),
        )

        result = self.rows.result_set_stats()
        self.assertIsInstance(result, ResultSetStats)
        self.assertEqual(result.row_count_exact, 5)
        self.mock_lib.ResultSetStats.assert_called_once_with(1, 123, 101)

    @patch("google.cloud.spannerlib.rows.get_lib")
    def test_result_set_stats_failure(self, mock_get_lib):
        """Test result_set_stats failure."""
        mock_get_lib.return_value = self.mock_lib
        self.mock_lib.ResultSetStats.return_value = GoReturn(
            pinner_id=0, error_code=1, object_id=0, msg_len=0, msg=None
        )
        with self.assertRaises(SpannerLibError):
            self.rows.result_set_stats()

    @patch("google.cloud.spannerlib.rows.get_lib")
    def test_update_count(self, mock_get_lib):
        """Test update_count."""
        mock_get_lib.return_value = self.mock_lib
        stats = ResultSetStats(row_count_exact=10)
        stats_bytes = ResultSetStats.serialize(stats)
        self.mock_lib.ResultSetStats.return_value = GoReturn(
            pinner_id=0,
            error_code=0,
            object_id=0,
            msg_len=len(stats_bytes),
            msg=ctypes.cast(ctypes.c_char_p(stats_bytes), ctypes.c_void_p),
        )
        self.assertEqual(self.rows.update_count(), 10)


if __name__ == "__main__":
    unittest.main()
