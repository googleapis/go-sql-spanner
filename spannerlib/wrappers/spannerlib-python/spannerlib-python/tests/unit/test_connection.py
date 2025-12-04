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

from google.cloud.spannerlib import Connection, Rows  # type: ignore
from google.cloud.spannerlib.internal.errors import SpannerLibError


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
        # Ensure close_rows returns a context manager
        lib.close_rows.return_value = ctx_manager
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
        conn = Connection(oid=123, pool=mock_pool)
        yield conn
        conn.close()

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
            with pytest.raises(SpannerLibError):
                connection.close()

            # Verify logging
            mock_logger.exception.assert_called_once_with(
                "Unexpected error closing connection ID: %d", 123
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
            with pytest.raises(SpannerLibError):
                connection.close()

            mock_logger.exception.assert_called_once()

    # -------------------------------------------------------------------------
    # Test Methods: Execution
    # -------------------------------------------------------------------------

    def test_execute_success(self, connection, mock_spanner_lib, mock_msg):
        """Test successful SQL execution."""
        # 1. Setup
        mock_request = Mock()
        serialized_request = b"serialized_request"

        with patch(
            "google.cloud.spanner_v1.ExecuteSqlRequest.serialize",
            return_value=serialized_request,
        ) as mock_serialize:
            # Mock spannerlib.execute context manager
            ctx_manager = MagicMock()
            ctx_manager.__enter__.return_value = mock_msg
            mock_spanner_lib.execute.return_value = ctx_manager

            # Set expected Rows ID
            mock_msg.object_id = 456

            # 2. Execute
            rows = connection.execute(mock_request)

            # 3. Assertions
            mock_serialize.assert_called_once_with(mock_request)
            mock_spanner_lib.execute.assert_called_once_with(
                999, 123, serialized_request
            )
            mock_msg.raise_if_error.assert_called_once()

            assert isinstance(rows, Rows)
            assert rows.oid == 456
            assert rows.pool == connection.pool
            assert rows.conn == connection

            # Clean up
            rows.close()

    def test_execute_closed_connection(self, connection):
        """Test execute raises error if connection is closed."""
        connection._mark_disposed()
        mock_request = Mock()

        with pytest.raises(SpannerLibError, match="Connection is closed"):
            connection.execute(mock_request)

    def test_execute_propagates_error(self, connection, mock_spanner_lib):
        """Test that execute propagates errors from the library."""
        # 1. Setup
        mock_request = Mock()
        serialized_request = b"serialized_request"

        with patch(
            "google.cloud.spanner_v1.ExecuteSqlRequest.serialize",
            return_value=serialized_request,
        ):
            # Mock spannerlib.execute context manager with a NEW message object
            ctx_manager = MagicMock()
            exec_msg = Mock()
            ctx_manager.__enter__.return_value = exec_msg
            mock_spanner_lib.execute.return_value = ctx_manager

            # Simulate error
            exec_msg.raise_if_error.side_effect = SpannerLibError(
                "Execution failed"
            )

            # 2. Execute & Assert
            with pytest.raises(SpannerLibError, match="Execution failed"):
                connection.execute(mock_request)

    def test_execute_batch_success(
        self, connection, mock_spanner_lib, mock_msg
    ):
        """Test successful batch DML execution."""
        # 1. Setup
        mock_request = Mock()
        serialized_request = b"serialized_request"
        mock_response = Mock()
        serialized_response = b"serialized_response"

        with patch(
            "google.cloud.spanner_v1.ExecuteBatchDmlRequest.serialize",
            return_value=serialized_request,
        ) as mock_serialize, patch(
            "google.cloud.spannerlib.connection.to_bytes",
            return_value=serialized_response,
        ) as mock_to_bytes, patch(
            "google.cloud.spanner_v1.ExecuteBatchDmlResponse.deserialize",
            return_value=mock_response,
        ) as mock_deserialize:
            # Mock spannerlib.execute_batch context manager
            ctx_manager = MagicMock()
            ctx_manager.__enter__.return_value = mock_msg
            mock_spanner_lib.execute_batch.return_value = ctx_manager

            # Mock message attributes
            mock_msg.msg = Mock()
            mock_msg.msg_len = 123

            # 2. Execute
            response = connection.execute_batch(mock_request)

            # 3. Assertions
            mock_serialize.assert_called_once_with(mock_request)
            mock_spanner_lib.execute_batch.assert_called_once_with(
                999, 123, serialized_request
            )
            mock_msg.raise_if_error.assert_called_once()
            mock_to_bytes.assert_called_once_with(
                mock_msg.msg, mock_msg.msg_len
            )
            mock_deserialize.assert_called_once_with(serialized_response)
            assert response == mock_response

    def test_execute_batch_closed_connection(self, connection):
        """Test execute_batch raises error if connection is closed."""
        connection._mark_disposed()
        mock_request = Mock()

        with pytest.raises(SpannerLibError, match="Connection is closed"):
            connection.execute_batch(mock_request)

    def test_execute_batch_propagates_error(self, connection, mock_spanner_lib):
        """Test that execute_batch propagates errors from the library."""
        # 1. Setup
        mock_request = Mock()
        serialized_request = b"serialized_request"

        with patch(
            "google.cloud.spanner_v1.ExecuteBatchDmlRequest.serialize",
            return_value=serialized_request,
        ):
            # Mock spannerlib.execute_batch context manager
            ctx_manager = MagicMock()
            exec_msg = Mock()
            ctx_manager.__enter__.return_value = exec_msg
            mock_spanner_lib.execute_batch.return_value = ctx_manager

            # Simulate error
            exec_msg.raise_if_error.side_effect = SpannerLibError(
                "Batch Execution failed"
            )

            # 2. Execute & Assert
            with pytest.raises(SpannerLibError, match="Batch Execution failed"):
                connection.execute_batch(mock_request)

    def test_write_mutations_success(
        self, connection, mock_spanner_lib, mock_msg
    ):
        """Test successful mutation write."""
        # 1. Setup
        mock_request = Mock()
        serialized_request = b"serialized_request"
        mock_response = Mock()
        serialized_response = b"serialized_response"

        with patch(
            "google.cloud.spanner_v1.BatchWriteRequest.MutationGroup.serialize",
            return_value=serialized_request,
        ) as mock_serialize, patch(
            "google.cloud.spannerlib.connection.to_bytes",
            return_value=serialized_response,
        ) as mock_to_bytes, patch(
            "google.cloud.spanner_v1.CommitResponse.deserialize",
            return_value=mock_response,
        ) as mock_deserialize:
            # Mock spannerlib.write_mutations context manager
            ctx_manager = MagicMock()
            ctx_manager.__enter__.return_value = mock_msg
            mock_spanner_lib.write_mutations.return_value = ctx_manager

            # Mock message attributes
            mock_msg.msg = Mock()
            mock_msg.msg_len = 123

            # 2. Execute
            response = connection.write_mutations(mock_request)

            # 3. Assertions
            mock_serialize.assert_called_once_with(mock_request)
            mock_spanner_lib.write_mutations.assert_called_once_with(
                999, 123, serialized_request
            )
            mock_msg.raise_if_error.assert_called_once()
            mock_to_bytes.assert_called_once_with(
                mock_msg.msg, mock_msg.msg_len
            )
            mock_deserialize.assert_called_once_with(serialized_response)
            assert response == mock_response

    def test_write_mutations_closed_connection(self, connection):
        """Test write_mutations raises error if connection is closed."""
        connection._mark_disposed()
        mock_request = Mock()

        with pytest.raises(SpannerLibError, match="Connection is closed"):
            connection.write_mutations(mock_request)

    def test_write_mutations_propagates_error(
        self, connection, mock_spanner_lib
    ):
        """Test that write_mutations propagates errors from the library."""
        # 1. Setup
        mock_request = Mock()
        serialized_request = b"serialized_request"

        with patch(
            "google.cloud.spanner_v1.BatchWriteRequest.MutationGroup.serialize",
            return_value=serialized_request,
        ):
            # Mock spannerlib.write_mutations context manager
            ctx_manager = MagicMock()
            exec_msg = Mock()
            ctx_manager.__enter__.return_value = exec_msg
            mock_spanner_lib.write_mutations.return_value = ctx_manager

            # Simulate error
            exec_msg.raise_if_error.side_effect = SpannerLibError(
                "Mutation Write failed"
            )

            with pytest.raises(SpannerLibError, match="Mutation Write failed"):
                connection.write_mutations(mock_request)

    def test_begin_transaction_success(
        self, connection, mock_spanner_lib, mock_msg
    ):
        """Test successful transaction start."""
        # 1. Setup
        mock_options = Mock()
        serialized_options = b"serialized_options"

        with patch(
            "google.cloud.spanner_v1.TransactionOptions.serialize",
            return_value=serialized_options,
        ) as mock_serialize:
            # Mock spannerlib.begin_transaction context manager
            ctx_manager = MagicMock()
            ctx_manager.__enter__.return_value = mock_msg
            mock_spanner_lib.begin_transaction.return_value = ctx_manager

            # 2. Execute
            connection.begin_transaction(mock_options)

            # 3. Assertions
            mock_serialize.assert_called_once_with(mock_options)
            mock_spanner_lib.begin_transaction.assert_called_once_with(
                999, 123, serialized_options
            )
            mock_msg.raise_if_error.assert_called_once()

    def test_begin_transaction_default_options(
        self, connection, mock_spanner_lib, mock_msg
    ):
        """Test begin_transaction with default options."""
        # 1. Setup
        serialized_options = b"default_options"

        with patch(
            "google.cloud.spannerlib.connection.TransactionOptions"
        ) as mock_options_cls:
            mock_options_cls.serialize.return_value = serialized_options

            # Mock spannerlib.begin_transaction context manager
            ctx_manager = MagicMock()
            ctx_manager.__enter__.return_value = mock_msg
            mock_spanner_lib.begin_transaction.return_value = ctx_manager

            # 2. Execute
            connection.begin_transaction()

            # 3. Assertions
            mock_options_cls.assert_called_once()
            mock_options_cls.serialize.assert_called_once()
            mock_spanner_lib.begin_transaction.assert_called_once_with(
                999, 123, serialized_options
            )
            mock_msg.raise_if_error.assert_called_once()

    def test_begin_transaction_closed_connection(self, connection):
        """Test begin_transaction raises error if connection is closed."""
        connection._mark_disposed()

        with pytest.raises(SpannerLibError, match="Connection is closed"):
            connection.begin_transaction()

    def test_begin_transaction_propagates_error(
        self, connection, mock_spanner_lib
    ):
        """Test that begin_transaction propagates errors from the library."""
        # 1. Setup
        serialized_options = b"serialized_options"

        with patch(
            "google.cloud.spanner_v1.TransactionOptions.serialize",
            return_value=serialized_options,
        ):
            # Mock spannerlib.begin_transaction context manager
            ctx_manager = MagicMock()
            exec_msg = Mock()
            ctx_manager.__enter__.return_value = exec_msg
            mock_spanner_lib.begin_transaction.return_value = ctx_manager

            # Simulate error
            exec_msg.raise_if_error.side_effect = SpannerLibError(
                "Transaction Start failed"
            )

            # 2. Execute & Assert
            with pytest.raises(SpannerLibError, match="Transaction Start failed"):
                connection.begin_transaction(Mock())

    def test_commit_success(self, connection, mock_spanner_lib, mock_msg):
        """Test successful commit."""
        # 1. Setup
        mock_response = Mock()
        serialized_response = b"serialized_response"

        with patch(
            "google.cloud.spannerlib.connection.to_bytes",
            return_value=serialized_response,
        ) as mock_to_bytes, patch(
            "google.cloud.spanner_v1.CommitResponse.deserialize",
            return_value=mock_response,
        ) as mock_deserialize:
            # Mock spannerlib.commit context manager
            ctx_manager = MagicMock()
            ctx_manager.__enter__.return_value = mock_msg
            mock_spanner_lib.commit.return_value = ctx_manager

            # Mock message attributes
            mock_msg.msg = Mock()
            mock_msg.msg_len = 123

            # 2. Execute
            response = connection.commit()

            # 3. Assertions
            mock_spanner_lib.commit.assert_called_once_with(999, 123)
            mock_msg.raise_if_error.assert_called_once()
            mock_to_bytes.assert_called_once_with(
                mock_msg.msg, mock_msg.msg_len
            )
            mock_deserialize.assert_called_once_with(serialized_response)
            assert response == mock_response

    def test_commit_closed_connection(self, connection):
        """Test commit raises error if connection is closed."""
        connection._mark_disposed()

        with pytest.raises(SpannerLibError, match="Connection is closed"):
            connection.commit()

    def test_commit_propagates_error(self, connection, mock_spanner_lib):
        """Test that commit propagates errors from the library."""
        # 1. Setup
        # Mock spannerlib.commit context manager
        ctx_manager = MagicMock()
        exec_msg = Mock()
        ctx_manager.__enter__.return_value = exec_msg
        mock_spanner_lib.commit.return_value = ctx_manager

        # Simulate error
        exec_msg.raise_if_error.side_effect = SpannerLibError("Commit failed")

        # 2. Execute & Assert
        with pytest.raises(SpannerLibError, match="Commit failed"):
            connection.commit()

    def test_rollback_success(self, connection, mock_spanner_lib, mock_msg):
        """Test successful rollback."""
        # 1. Setup
        # Mock spannerlib.rollback context manager
        ctx_manager = MagicMock()
        ctx_manager.__enter__.return_value = mock_msg
        mock_spanner_lib.rollback.return_value = ctx_manager

        # 2. Execute
        connection.rollback()

        # 3. Assertions
        mock_spanner_lib.rollback.assert_called_once_with(999, 123)
        mock_msg.raise_if_error.assert_called_once()

    def test_rollback_closed_connection(self, connection):
        """Test rollback raises error if connection is closed."""
        connection._mark_disposed()

        with pytest.raises(SpannerLibError, match="Connection is closed"):
            connection.rollback()

    def test_rollback_propagates_error(self, connection, mock_spanner_lib):
        """Test that rollback propagates errors from the library."""
        # 1. Setup
        # Mock spannerlib.rollback context manager
        ctx_manager = MagicMock()
        exec_msg = Mock()
        ctx_manager.__enter__.return_value = exec_msg
        mock_spanner_lib.rollback.return_value = ctx_manager

        # Simulate error
        exec_msg.raise_if_error.side_effect = SpannerLibError("Rollback failed")

        # 2. Execute & Assert
        with pytest.raises(SpannerLibError, match="Rollback failed"):
            connection.rollback()
