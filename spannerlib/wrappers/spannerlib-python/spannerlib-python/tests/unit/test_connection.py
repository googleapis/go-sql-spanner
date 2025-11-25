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
from unittest.mock import MagicMock, Mock, patch

import pytest

from google.cloud.spannerlib import Connection  # type: ignore


class TestConnection:
    """
    Test suite for the Connection class.
    """

    # -------------------------------------------------------------------------
    # Fixtures
    # -------------------------------------------------------------------------

    @pytest.fixture
    def mock_msg(self):
        """Mocks the message object returned by the library context manager."""
        msg = Mock()
        return msg

    @pytest.fixture
    def mock_spanner_lib(self, mock_msg):
        """Mocks the underlying SpannerLib instance."""
        lib = Mock()
        # Ensure close_connection returns a context manager
        ctx_manager = MagicMock()
        ctx_manager.__enter__.return_value = mock_msg
        lib.close_connection.return_value = ctx_manager
        return lib

    @pytest.fixture
    def mock_pool(self, mock_spanner_lib):
        """
        Mocks the Pool object.
        The Connection class expects access to pool.spannerlib and pool.oid.
        """
        pool = Mock()
        pool.spannerlib = mock_spanner_lib
        pool.oid = 999  # Mock Pool ID
        return pool

    @pytest.fixture
    def connection(self, mock_pool):
        """Creates a Connection instance for testing."""
        return Connection(oid=123, pool=mock_pool)

    @pytest.fixture
    def setup_close_context(self, mock_spanner_lib, mock_msg):
        """
        Helper fixture that configures the context manager for
        close_connection.

        This setup is complex (Context Manager -> Returns Msg -> Checks Error),
        so encapsulating it keeps the test methods clean.
        """

        def _setup():
            # Create a MagicMock to handle the 'with' statement
            ctx_manager = MagicMock()
            # When entering 'with', return the message mock
            ctx_manager.__enter__.return_value = mock_msg
            mock_spanner_lib.close_connection.return_value = ctx_manager
            return ctx_manager

        return _setup

    # -------------------------------------------------------------------------
    # Test Methods: Initialization & State
    # -------------------------------------------------------------------------

    def test_initialization(self, mock_pool, mock_spanner_lib):
        """Test that Connection initializes correctly from the Pool."""
        conn = Connection(oid=50, pool=mock_pool)

        # Verify inheritance (AbstractLibraryObject) setup
        assert conn.spannerlib == mock_spanner_lib
        assert conn.oid == 50

        # Verify specific connection attributes
        assert conn.pool.oid == 999

    def test_pool_property_is_read_only(self, connection):
        """Ensure pool cannot be overwritten accidentally."""
        assert connection.pool.oid == 999

        with pytest.raises(AttributeError):
            connection.pool = None

    # -------------------------------------------------------------------------
    # Test Methods: Lifecycle & Cleanup
    # -------------------------------------------------------------------------

    def test_close_connection_success(
        self, connection, mock_spanner_lib, mock_msg, setup_close_context
    ):
        """Test the successful closure of a connection via the public
        .close() method."""
        # 1. Setup the mocks
        setup_close_context()

        # 2. Execute
        # calling the public .close() (from parent) which triggers
        # _close_lib_object()
        connection.close()

        # 3. Assertions
        # Verify correct arguments passed to Go Lib: (PoolID, ConnectionID)
        mock_spanner_lib.close_connection.assert_called_once_with(999, 123)

        # Verify context manager lifecycle
        mock_msg.bind_library.assert_called_once_with(mock_spanner_lib)
        mock_msg.raise_if_error.assert_called_once()

        # Verify object is marked disposed
        assert connection._is_disposed is True

    def test_close_connection_propagates_error(
        self, connection, mock_msg, setup_close_context
    ):
        """Test that exceptions from the Go library are logged
        and re-raised."""
        # 1. Setup
        setup_close_context()

        # Simulate a failure in the underlying library (e.g. C++ error)
        error = RuntimeError("Go Library Error")
        mock_msg.raise_if_error.side_effect = error

        # 2. Execute & Assert
        # We patch the logger to ensure the error is logged before crashing
        with patch("google.cloud.spannerlib.connection.logger") as mock_logger:
            with pytest.raises(RuntimeError):
                connection.close()

            # Verify logging
            mock_logger.exception.assert_called_once_with(
                "Error closing connection ID: %d", 123
            )

    def test_close_connection_unexpected_error(
        self, connection, mock_spanner_lib
    ):
        """Test handling when the context manager setup itself fails
        (e.g. memory error)."""
        # 1. Setup
        # Simulate a crash before yielding the message
        mock_spanner_lib.close_connection.side_effect = ValueError(
            "Invalid Arguments"
        )

        # 2. Execute & Assert
        with patch("google.cloud.spannerlib.connection.logger") as mock_logger:
            with pytest.raises(ValueError):
                connection.close()

            mock_logger.exception.assert_called_once()
