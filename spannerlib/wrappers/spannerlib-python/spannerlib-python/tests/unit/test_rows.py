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
from unittest.mock import MagicMock

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
