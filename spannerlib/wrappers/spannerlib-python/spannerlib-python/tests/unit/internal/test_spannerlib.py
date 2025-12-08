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
"""Unit tests for the SpannerLib class."""
import ctypes
from pathlib import Path
from unittest import mock

import pytest

from google.cloud.spannerlib.internal import GoString  # type: ignore
from google.cloud.spannerlib.internal import Message  # type: ignore
from google.cloud.spannerlib.internal import SpannerLib  # type: ignore
from google.cloud.spannerlib.internal import SpannerLibError  # type: ignore


@pytest.fixture(autouse=True)
def reset_singleton():
    """Resets the SpannerLib singleton before and after each test."""
    # pylint: disable=protected-access
    SpannerLib._instance = None
    SpannerLib._lib_handle = None
    yield
    SpannerLib._instance = None
    SpannerLib._lib_handle = None
    # pylint: enable=protected-access


# FIX: Use 'name=' to separate the fixture name from the function name.
# This prevents 'redefined-outer-name' warnings because the function
# 'fixture_mock_cdll_cls' is different from the argument 'mock_cdll_cls'.
@pytest.fixture(name="mock_cdll_cls")
def fixture_mock_cdll_cls():
    """Mocks the ctypes.CDLL class constructor."""
    with mock.patch("ctypes.CDLL") as mock_cls:
        mock_instance = mock.MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_cls


@pytest.fixture(name="mock_lib_instance")
def fixture_mock_lib_instance(mock_cdll_cls):
    """Returns the mocked library instance returned by CDLL()."""
    return mock_cdll_cls.return_value


@pytest.fixture(name="mock_lib_path")
def fixture_mock_lib_path():
    """Mocks get_shared_library to yield a dummy path."""
    with mock.patch(
        "google.cloud.spannerlib.internal.spannerlib.get_shared_library"
    ) as mock_ctx:
        mock_path = mock.MagicMock(spec=Path)
        mock_path.exists.return_value = True
        mock_path.name = "mock_lib.so"
        mock_path.__str__.return_value = "/abs/path/to/mock_lib.so"

        # Configure the context manager to yield mock_path
        mock_ctx.return_value.__enter__.return_value = mock_path
        yield mock_path


class TestSpannerlib:
    """Tests for the SpannerLib class."""

    def test_singleton_creation(self, mock_lib_path, mock_cdll_cls):
        """Test that SpannerLib is a singleton."""
        lib1 = SpannerLib()
        lib2 = SpannerLib()
        assert lib1 is lib2
        mock_cdll_cls.assert_called_once()

    def test_initialize_success(self, mock_lib_path, mock_cdll_cls):
        """Test successful initialization."""
        lib = SpannerLib()

        # Verify handle is the mock instance
        # pylint: disable=protected-access
        assert lib._lib_handle is mock_cdll_cls.return_value

        # Verify CDLL call
        mock_cdll_cls.assert_called_once_with(str(mock_lib_path))

    def test_initialize_load_failure(self, mock_lib_path):
        """Test initialization failure when CDLL raises OSError."""
        with mock.patch("ctypes.CDLL", side_effect=OSError("Load failed")):
            with pytest.raises(
                SpannerLibError, match="Could not load native dependency"
            ):
                SpannerLib()

    def test_initialize_lib_not_found(self, mock_lib_path):
        """Test initialization failure when the library file doesn't exist."""
        mock_lib_path.exists.return_value = False
        with mock.patch("platform.system", return_value="Linux"):
            with pytest.raises(
                SpannerLibError, match="Library path does not exist:"
            ):
                SpannerLib()

    def test_get_lib_filename_linux(self):
        """Test _get_lib_filename on Linux AMD64."""
        with mock.patch("platform.system", return_value="Linux"):
            with mock.patch("platform.machine", return_value="x86_64"):
                # pylint: disable=protected-access
                filename = SpannerLib._get_lib_filename()
                assert filename == "linux-x64/spannerlib.so"

    def test_get_lib_filename_linux_arm64(self):
        """Test _get_lib_filename on Linux ARM64."""
        with mock.patch("platform.system", return_value="Linux"):
            with mock.patch("platform.machine", return_value="aarch64"):
                # pylint: disable=protected-access
                filename = SpannerLib._get_lib_filename()
                assert filename == "linux-arm64/spannerlib.so"

    def test_get_lib_filename_darwin(self):
        """Test _get_lib_filename on Darwin ARM64."""
        with mock.patch("platform.system", return_value="Darwin"):
            with mock.patch("platform.machine", return_value="arm64"):
                # pylint: disable=protected-access
                filename = SpannerLib._get_lib_filename()
                assert filename == "osx-arm64/spannerlib.dylib"

    def test_get_lib_filename_windows(self):
        """Test _get_lib_filename on Windows AMD64."""
        with mock.patch("platform.system", return_value="Windows"):
            with mock.patch("platform.machine", return_value="AMD64"):
                # pylint: disable=protected-access
                filename = SpannerLib._get_lib_filename()
                assert filename == "win-x64/spannerlib.dll"

    def test_get_lib_filename_unsupported_arch(self):
        """Test _get_lib_filename on an unsupported architecture."""
        with mock.patch("platform.system", return_value="Linux"):
            with mock.patch("platform.machine", return_value="riscv64"):
                with pytest.raises(
                    SpannerLibError, match="Unsupported architecture"
                ):
                    # pylint: disable=protected-access
                    SpannerLib._get_lib_filename()

    def test_get_lib_filename_unsupported_os(self):
        """Test _get_lib_filename on an unsupported OS."""
        with mock.patch("platform.system", return_value="AmigaOS"):
            with pytest.raises(
                SpannerLibError, match="Unsupported operating system"
            ):
                # pylint: disable=protected-access
                SpannerLib._get_lib_filename()

    def test_configure_signatures_missing_symbol(
        self, mock_lib_path, mock_lib_instance
    ):
        """Test behavior if a required symbol is missing."""
        del mock_lib_instance.Release

        # Should not raise, but degrade gracefully
        # (or crash later depending on logic)
        SpannerLib()

        # Verify Release was skipped during configuration
        with pytest.raises(AttributeError):
            _ = mock_lib_instance.Release

    def test_lib_property_not_initialized(self):
        """Test accessing lib property before initialization."""
        instance = object.__new__(SpannerLib)
        with pytest.raises(SpannerLibError, match="not been initialized"):
            _ = instance.lib

    def test_lib_property_initialized(self, mock_lib_path, mock_lib_instance):
        """Test accessing lib property after initialization."""
        lib = SpannerLib()
        assert lib.lib is mock_lib_instance

    def test_create_pool(self, mock_lib_path, mock_lib_instance):
        """Test the create_pool method."""
        expected_message = Message()
        mock_lib_instance.CreatePool.return_value = expected_message

        lib = SpannerLib()
        config = "test_config"
        result = lib.create_pool(config)

        assert result is expected_message
        mock_lib_instance.CreatePool.assert_called_once()

        # Validate Argument Type
        args, _ = mock_lib_instance.CreatePool.call_args
        assert isinstance(args[0], GoString)

        # Validate Signature Setup
        assert mock_lib_instance.CreatePool.argtypes == [GoString]
        assert mock_lib_instance.CreatePool.restype == Message

        # Verify Library Binding
        assert result._lib is lib

    def test_close_pool(self, mock_lib_path, mock_lib_instance):
        """Test the close_pool method."""
        expected_message = Message()
        mock_lib_instance.ClosePool.return_value = expected_message

        lib = SpannerLib()
        lib.close_pool(456)

        # 1. Verify called exactly once
        mock_lib_instance.ClosePool.assert_called_once()

        # 2. Get the arguments of that call
        args, _ = mock_lib_instance.ClosePool.call_args
        passed_arg = args[0]

        # 3. Assert it is the correct type and holds the correct value
        assert isinstance(passed_arg, ctypes.c_longlong)
        assert passed_arg.value == 456

        # 4. Verify result
        assert mock_lib_instance.ClosePool.argtypes == [ctypes.c_longlong]
        assert mock_lib_instance.ClosePool.restype == Message

        # Verify Library Binding
        assert lib.close_pool(456)._lib is lib

    def test_create_connection(self, mock_lib_path, mock_lib_instance):
        """Test the create_connection method."""
        expected_message = Message()
        mock_lib_instance.CreateConnection.return_value = expected_message

        lib = SpannerLib()
        lib.create_connection(123)

        mock_lib_instance.CreateConnection.assert_called_once()
        args, _ = mock_lib_instance.CreateConnection.call_args
        assert args[0].value == 123

        # Verify Library Binding
        assert lib.create_connection(123)._lib is lib

    def test_close_connection(self, mock_lib_path, mock_lib_instance):
        """Test the close_connection method."""
        expected_message = Message()
        mock_lib_instance.CloseConnection.return_value = expected_message

        lib = SpannerLib()
        lib.close_connection(123, 456)

        mock_lib_instance.CloseConnection.assert_called_once()
        args, _ = mock_lib_instance.CloseConnection.call_args
        assert args[0].value == 123
        assert args[1].value == 456

        # Verify Library Binding
        assert lib.close_connection(123, 456)._lib is lib

    def test_execute(self, mock_lib_path, mock_lib_instance):
        """Test the execute method."""
        expected_message = Message()
        mock_lib_instance.Execute.return_value = expected_message

        lib = SpannerLib()
        lib.execute(123, 456, b"SQL_statement")

        mock_lib_instance.Execute.assert_called_once()
        args, _ = mock_lib_instance.Execute.call_args
        assert args[0].value == 123
        assert args[1].value == 456
        # However, we can check the type if we mocked GoSlice properly
        # or check argtypes.
        assert mock_lib_instance.Execute.argtypes[2] is type(args[2])

        # Verify Library Binding
        assert lib.execute(123, 456, b"SQL_statement")._lib is lib

    def test_execute_batch(self, mock_lib_path, mock_lib_instance):
        """Test the execute_batch method."""
        expected_message = Message()
        mock_lib_instance.ExecuteBatch.return_value = expected_message

        lib = SpannerLib()
        lib.execute_batch(123, 456, b"serialized_statements")

        mock_lib_instance.ExecuteBatch.assert_called_once()
        args, _ = mock_lib_instance.ExecuteBatch.call_args
        assert args[0].value == 123
        assert args[1].value == 456

        # Verify Library Binding
        assert lib.execute_batch(123, 456, b"serialized_statements")._lib is lib

    def test_next(self, mock_lib_path, mock_lib_instance):
        """Test the next method."""
        expected_message = Message()
        mock_lib_instance.Next.return_value = expected_message

        lib = SpannerLib()
        lib.next(1, 2, 3, 10, 1)

        mock_lib_instance.Next.assert_called_once()
        args, _ = mock_lib_instance.Next.call_args
        assert args[0].value == 1
        assert args[1].value == 2
        assert args[2].value == 3
        assert args[3].value == 10
        assert args[4].value == 1

        # Verify Library Binding
        assert lib.next(1, 2, 3, 10, 1)._lib is lib

    def test_close_rows(self, mock_lib_path, mock_lib_instance):
        """Test the close_rows method."""
        expected_message = Message()
        mock_lib_instance.CloseRows.return_value = expected_message

        lib = SpannerLib()
        lib.close_rows(1, 2, 3)

        mock_lib_instance.CloseRows.assert_called_once()
        args, _ = mock_lib_instance.CloseRows.call_args
        assert args[0].value == 1
        assert args[1].value == 2
        assert args[2].value == 3

        # Verify Library Binding
        assert lib.close_rows(1, 2, 3)._lib is lib

    def test_metadata(self, mock_lib_path, mock_lib_instance):
        """Test the metadata method."""
        expected_message = Message()
        mock_lib_instance.Metadata.return_value = expected_message

        lib = SpannerLib()
        lib.metadata(1, 2, 3)

        mock_lib_instance.Metadata.assert_called_once()
        args, _ = mock_lib_instance.Metadata.call_args
        assert args[0].value == 1
        assert args[1].value == 2
        assert args[2].value == 3

        # Verify Library Binding
        assert lib.metadata(1, 2, 3)._lib is lib

    def test_result_set_stats(self, mock_lib_path, mock_lib_instance):
        """Test the result_set_stats method."""
        expected_message = Message()
        mock_lib_instance.ResultSetStats.return_value = expected_message

        lib = SpannerLib()
        lib.result_set_stats(1, 2, 3)

        mock_lib_instance.ResultSetStats.assert_called_once()
        args, _ = mock_lib_instance.ResultSetStats.call_args
        assert args[0].value == 1
        assert args[1].value == 2
        assert args[2].value == 3

        # Verify Library Binding
        assert lib.result_set_stats(1, 2, 3)._lib is lib

    def test_begin_transaction(self, mock_lib_path, mock_lib_instance):
        """Test the begin_transaction method."""
        expected_message = Message()
        mock_lib_instance.BeginTransaction.return_value = expected_message

        lib = SpannerLib()
        lib.begin_transaction(1, 2, b"tx_opts")

        mock_lib_instance.BeginTransaction.assert_called_once()
        args, _ = mock_lib_instance.BeginTransaction.call_args
        assert args[0].value == 1
        assert args[1].value == 2

        # Verify Library Binding
        assert lib.begin_transaction(1, 2, b"tx_opts")._lib is lib

    def test_commit(self, mock_lib_path, mock_lib_instance):
        """Test the commit method."""
        expected_message = Message()
        mock_lib_instance.Commit.return_value = expected_message

        lib = SpannerLib()
        lib.commit(1, 2)

        mock_lib_instance.Commit.assert_called_once()
        args, _ = mock_lib_instance.Commit.call_args
        assert args[0].value == 1
        assert args[1].value == 2

        # Verify Library Binding
        assert lib.commit(1, 2)._lib is lib

    def test_rollback(self, mock_lib_path, mock_lib_instance):
        """Test the rollback method."""
        expected_message = Message()
        mock_lib_instance.Rollback.return_value = expected_message

        lib = SpannerLib()
        lib.rollback(1, 2)

        mock_lib_instance.Rollback.assert_called_once()
        args, _ = mock_lib_instance.Rollback.call_args
        assert args[0].value == 1
        assert args[1].value == 2

        # Verify Library Binding
        assert lib.rollback(1, 2)._lib is lib

    def test_write_mutations(self, mock_lib_path, mock_lib_instance):
        """Test the write_mutations method."""
        expected_message = Message()
        mock_lib_instance.WriteMutations.return_value = expected_message

        lib = SpannerLib()
        lib.write_mutations(1, 2, b"mutations")

        mock_lib_instance.WriteMutations.assert_called_once()
        args, _ = mock_lib_instance.WriteMutations.call_args
        assert args[0].value == 1
        assert args[1].value == 2

        # Verify Library Binding
        assert lib.write_mutations(1, 2, b"mutations")._lib is lib
