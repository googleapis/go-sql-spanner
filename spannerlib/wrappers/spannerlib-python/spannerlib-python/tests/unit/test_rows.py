# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Unit tests for Rows."""
from unittest.mock import MagicMock, patch

import pytest

from google.cloud.spannerlib.internal.errors import SpannerLibError
from google.cloud.spannerlib.internal.spannerlib_protocol import (
    SpannerLibProtocol,
)
from google.cloud.spannerlib.rows import Rows


class TestRows:
    """Test suite for Rows class."""

    def test_init(self) -> None:
        """Verifies correct initialization of Rows."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        oid = 123
        rows = Rows(oid, mock_pool, mock_conn)

        assert rows.oid == oid
        assert rows.pool == mock_pool
        assert rows.conn == mock_conn
        assert rows.spannerlib == mock_spannerlib
        assert not rows.closed

    def test_close_calls_lib(self) -> None:
        """Verifies that close() calls the underlying library function."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        # Set OIDs
        mock_pool.oid = 10
        mock_conn.oid = 20
        rows_oid = 30

        rows = Rows(rows_oid, mock_pool, mock_conn)

        # Setup mock context manager for close_rows
        mock_msg = MagicMock()
        mock_spannerlib.close_rows.return_value.__enter__.return_value = (
            mock_msg
        )

        rows.close()

        # Verify call arguments
        mock_spannerlib.close_rows.assert_called_once_with(10, 20, 30)
        mock_msg.raise_if_error.assert_called_once()
        assert rows.closed

    def test_close_handles_error(self) -> None:
        """Verifies that exceptions during close are propagated
        but object is still disposed."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        rows = Rows(30, mock_pool, mock_conn)

        # Setup mock to raise exception
        mock_spannerlib.close_rows.side_effect = Exception("Close failed")

        with pytest.raises(SpannerLibError, match="Close failed"):
            rows.close()

        # Verify object is marked closed despite error
        assert rows.closed

    def test_context_manager(self) -> None:
        """Verifies that Rows works as a context manager."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        # Setup mock context manager for close_rows
        mock_msg = MagicMock()
        mock_spannerlib.close_rows.return_value.__enter__.return_value = (
            mock_msg
        )

        rows = Rows(123, mock_pool, mock_conn)

        with rows as r:
            assert r is rows
            assert not rows.closed

        assert rows.closed
        mock_spannerlib.close_rows.assert_called_once()

    def test_metadata_returns_metadata(self) -> None:
        """Verifies that metadata() retrieves and deserializes metadata."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        # Set OIDs
        mock_pool.oid = 10
        mock_conn.oid = 20
        rows_oid = 30

        rows = Rows(rows_oid, mock_pool, mock_conn)

        # Mock the context manager and message
        mock_msg = MagicMock()
        mock_msg.msg_len = 10
        mock_msg.msg = 12345  # Fake pointer
        mock_spannerlib.metadata.return_value.__enter__.return_value = mock_msg

        # Patch ctypes and ResultSetMetadata
        with patch("google.cloud.spannerlib.rows.ctypes") as mock_ctypes, patch(
            "google.cloud.spannerlib.rows.ResultSetMetadata"
        ) as mock_metadata_cls:

            mock_ctypes.string_at.return_value = b"serialized_proto"
            expected_metadata = MagicMock()
            mock_metadata_cls.deserialize.return_value = expected_metadata

            result = rows.metadata()

            assert result == expected_metadata
            mock_spannerlib.metadata.assert_called_once_with(10, 20, 30)
            mock_msg.raise_if_error.assert_called_once()
            mock_ctypes.string_at.assert_called_once_with(12345, 10)
            mock_metadata_cls.deserialize.assert_called_once_with(
                b"serialized_proto"
            )

    def test_metadata_raises_if_closed(self) -> None:
        """Verifies that metadata() raises SpannerLibError if rows are closed."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        # Setup mock context manager for close_rows to avoid errors
        # during close()
        mock_spannerlib.close_rows.return_value.__enter__.return_value = (
            MagicMock()
        )

        rows = Rows(1, mock_pool, mock_conn)
        rows.close()

        with pytest.raises(SpannerLibError, match="Rows object is closed"):
            rows.metadata()

    def test_metadata_handles_deserialization_error(self) -> None:
        """Verifies that metadata() handles deserialization errors."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        rows = Rows(1, mock_pool, mock_conn)

        mock_msg = MagicMock()
        mock_msg.msg_len = 10
        mock_msg.msg = 12345
        mock_spannerlib.metadata.return_value.__enter__.return_value = mock_msg

        with patch("google.cloud.spannerlib.rows.ctypes") as mock_ctypes, patch(
            "google.cloud.spannerlib.rows.ResultSetMetadata"
        ) as mock_metadata_cls:

            mock_ctypes.string_at.return_value = b"data"
            mock_metadata_cls.deserialize.side_effect = Exception("Parse error")

            with pytest.raises(
                SpannerLibError, match="Failed to get metadata: Parse error"
            ):
                rows.metadata()

    def test_result_set_stats_returns_stats(self) -> None:
        """Verifies that result_set_stats() retrieves and deserializes stats."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        # Set OIDs
        mock_pool.oid = 10
        mock_conn.oid = 20
        rows_oid = 30

        rows = Rows(rows_oid, mock_pool, mock_conn)

        # Mock the context manager and message
        mock_msg = MagicMock()
        mock_msg.msg_len = 10
        mock_msg.msg = 12345
        mock_spannerlib.result_set_stats.return_value.__enter__.return_value = (
            mock_msg
        )

        # Patch ctypes and ResultSetStats
        with patch("google.cloud.spannerlib.rows.ctypes") as mock_ctypes, patch(
            "google.cloud.spannerlib.rows.ResultSetStats"
        ) as mock_stats_cls:

            mock_ctypes.string_at.return_value = b"stats_proto"
            expected_stats = MagicMock()
            mock_stats_cls.deserialize.return_value = expected_stats

            result = rows.result_set_stats()

            assert result == expected_stats
            mock_spannerlib.result_set_stats.assert_called_once_with(10, 20, 30)
            mock_msg.raise_if_error.assert_called_once()
            mock_ctypes.string_at.assert_called_once_with(12345, 10)
            mock_stats_cls.deserialize.assert_called_once_with(b"stats_proto")

    def test_result_set_stats_raises_if_closed(self) -> None:
        """Verifies that result_set_stats() raises SpannerLibError
        if rows are closed."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        # Setup mock context manager for close_rows to avoid
        # errors during close()
        mock_spannerlib.close_rows.return_value.__enter__.return_value = (
            MagicMock()
        )

        rows = Rows(1, mock_pool, mock_conn)
        rows.close()

        with pytest.raises(SpannerLibError, match="Rows object is closed"):
            rows.result_set_stats()

    def test_result_set_stats_handles_deserialization_error(self) -> None:
        """Verifies that result_set_stats() handles deserialization errors."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        rows = Rows(1, mock_pool, mock_conn)

        mock_msg = MagicMock()
        mock_msg.msg_len = 10
        mock_msg.msg = 12345
        mock_spannerlib.result_set_stats.return_value.__enter__.return_value = (
            mock_msg
        )

        with patch("google.cloud.spannerlib.rows.ctypes") as mock_ctypes, patch(
            "google.cloud.spannerlib.rows.ResultSetStats"
        ) as mock_stats_cls:

            mock_ctypes.string_at.return_value = b"data"
            mock_stats_cls.deserialize.side_effect = Exception(
                "Stats parse error"
            )

            with pytest.raises(
                SpannerLibError,
                match="Failed to get ResultSetStats: Stats parse error",
            ):
                rows.result_set_stats()

    def test_update_count_exact(self) -> None:
        """Verifies update_count returns row_count_exact if present."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        rows = Rows(1, mock_pool, mock_conn)

        mock_stats = MagicMock()
        mock_stats.row_count_exact = 42
        mock_stats.row_count_lower_bound = 0
        mock_stats._pb.WhichOneof.return_value = "row_count_exact"

        with patch.object(rows, "result_set_stats", return_value=mock_stats):
            assert rows.update_count() == 42

    def test_update_count_lower_bound(self) -> None:
        """Verifies update_count returns row_count_lower_bound
        if exact is missing."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        rows = Rows(1, mock_pool, mock_conn)

        mock_stats = MagicMock()
        mock_stats.row_count_exact = 0
        mock_stats.row_count_lower_bound = 15
        mock_stats._pb.WhichOneof.return_value = "row_count_lower_bound"

        with patch.object(rows, "result_set_stats", return_value=mock_stats):
            assert rows.update_count() == 15

    def test_update_count_zero(self) -> None:
        """Verifies update_count returns 0 if no stats available."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        rows = Rows(1, mock_pool, mock_conn)

        mock_stats = MagicMock()
        mock_stats.row_count_exact = 0
        mock_stats.row_count_lower_bound = 0
        mock_stats._pb.WhichOneof.return_value = None

        with patch.object(rows, "result_set_stats", return_value=mock_stats):
            assert rows.update_count() == 0

    def test_next_returns_row(self) -> None:
        """Verifies that next() retrieves and deserializes a row."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        rows = Rows(1, mock_pool, mock_conn)

        mock_msg = MagicMock()
        mock_msg.msg_len = 10
        mock_msg.msg = 12345
        mock_spannerlib.next.return_value.__enter__.return_value = mock_msg

        with patch("google.cloud.spannerlib.rows.ctypes") as mock_ctypes, patch(
            "google.cloud.spannerlib.rows.ListValue"
        ) as mock_list_value_cls:

            mock_ctypes.string_at.return_value = b"row_proto"
            expected_row = MagicMock()
            mock_list_value_cls.return_value = expected_row

            result = rows.next()

            assert result == expected_row
            mock_spannerlib.next.assert_called_once_with(
                mock_pool.oid, mock_conn.oid, rows.oid, 1, 1
            )
            mock_msg.raise_if_error.assert_called_once()
            mock_ctypes.string_at.assert_called_once_with(12345, 10)
            expected_row.ParseFromString.assert_called_once_with(b"row_proto")

    def test_next_returns_none_when_no_more_rows(self) -> None:
        """Verifies that next() returns None when no data is returned."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        rows = Rows(1, mock_pool, mock_conn)

        mock_msg = MagicMock()
        mock_msg.msg_len = 0
        mock_msg.msg = None
        mock_spannerlib.next.return_value.__enter__.return_value = mock_msg

        result = rows.next()

        assert result is None
        mock_spannerlib.next.assert_called_once()

    def test_next_raises_if_closed(self) -> None:
        """Verifies that next() raises SpannerLibError if rows are closed."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        # Setup mock context manager for close_rows
        mock_spannerlib.close_rows.return_value.__enter__.return_value = (
            MagicMock()
        )

        rows = Rows(1, mock_pool, mock_conn)
        rows.close()

        with pytest.raises(SpannerLibError, match="Rows object is closed"):
            rows.next()

    def test_next_handles_parse_error(self) -> None:
        """Verifies that next() handles parsing errors."""
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_spannerlib = MagicMock(spec=SpannerLibProtocol)
        mock_pool.spannerlib = mock_spannerlib

        rows = Rows(1, mock_pool, mock_conn)

        mock_msg = MagicMock()
        mock_msg.msg_len = 10
        mock_msg.msg = 12345
        mock_spannerlib.next.return_value.__enter__.return_value = mock_msg

        with patch("google.cloud.spannerlib.rows.ctypes") as mock_ctypes, patch(
            "google.cloud.spannerlib.rows.ListValue"
        ) as mock_list_value_cls:

            mock_ctypes.string_at.return_value = b"data"
            mock_row = MagicMock()
            mock_list_value_cls.return_value = mock_row
            mock_row.ParseFromString.side_effect = Exception("Parse error")

            with pytest.raises(SpannerLibError, match="Failed to get next row"):
                rows.next()
