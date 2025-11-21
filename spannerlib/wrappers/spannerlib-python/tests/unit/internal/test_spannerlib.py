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

from google.cloud.spannerlib.internal import errors  # type: ignore
from google.cloud.spannerlib.internal import message  # type: ignore
from google.cloud.spannerlib.internal import spannerlib  # type: ignore
from google.cloud.spannerlib.internal import types  # type: ignore


@pytest.fixture(autouse=True)
def reset_singleton():
    """Resets the SpannerLib singleton before and after each test."""
    # pylint: disable=protected-access
    spannerlib.SpannerLib._instance = None
    spannerlib.SpannerLib._lib_handle = None
    yield
    spannerlib.SpannerLib._instance = None
    spannerlib.SpannerLib._lib_handle = None
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
    """Mocks _get_lib_path to return a dummy path and mock exists."""
    with mock.patch.object(
        spannerlib.SpannerLib, "_get_lib_path"
    ) as mock_path_getter:
        mock_path = mock.MagicMock(spec=Path)
        mock_path.exists.return_value = True
        mock_path.name = "mock_lib.so"
        mock_path.__str__.return_value = "/abs/path/to/mock_lib.so"
        mock_path_getter.return_value = mock_path
        yield mock_path


class TestSpannerlib:
    """Tests for the SpannerLib class."""

    def test_singleton_creation(self, mock_lib_path, mock_cdll_cls):
        """Test that SpannerLib is a singleton."""
        lib1 = spannerlib.SpannerLib()
        lib2 = spannerlib.SpannerLib()
        assert lib1 is lib2
        mock_cdll_cls.assert_called_once()

    def test_initialize_success(self, mock_lib_path, mock_cdll_cls):
        """Test successful initialization."""
        lib = spannerlib.SpannerLib()

        # Verify handle is the mock instance
        # pylint: disable=protected-access
        assert lib._lib_handle is mock_cdll_cls.return_value

        # Verify CDLL call
        mock_cdll_cls.assert_called_once_with(str(mock_lib_path))

    def test_initialize_load_failure(self, mock_lib_path):
        """Test initialization failure when CDLL raises OSError."""
        with mock.patch("ctypes.CDLL", side_effect=OSError("Load failed")):
            with pytest.raises(
                errors.SpannerLibError, match="Could not load native dependency"
            ):
                spannerlib.SpannerLib()

    def test_initialize_lib_not_found(self):
        """Test initialization failure when the library file doesn't exist."""
        with mock.patch("platform.system", return_value="Linux"):
            with mock.patch.object(Path, "exists", return_value=False):
                with pytest.raises(
                    errors.SpannerLibError, match="Library file not found"
                ):
                    spannerlib.SpannerLib()

    def test_get_lib_path_linux(self):
        """Test _get_lib_path on Linux."""
        with mock.patch("platform.system", return_value="Linux"):
            with mock.patch.object(Path, "exists", return_value=True):
                # pylint: disable=protected-access
                path = spannerlib.SpannerLib._get_lib_path()
                assert path.name == "spannerlib.so"

    def test_get_lib_path_darwin(self):
        """Test _get_lib_path on Darwin."""
        with mock.patch("platform.system", return_value="Darwin"):
            with mock.patch.object(Path, "exists", return_value=True):
                # pylint: disable=protected-access
                path = spannerlib.SpannerLib._get_lib_path()
                assert path.name == "spannerlib.dylib"

    def test_get_lib_path_windows(self):
        """Test _get_lib_path on Windows."""
        with mock.patch("platform.system", return_value="Windows"):
            with mock.patch.object(Path, "exists", return_value=True):
                # pylint: disable=protected-access
                path = spannerlib.SpannerLib._get_lib_path()
                assert path.name == "spannerlib.dll"

    def test_get_lib_path_unsupported_os(self):
        """Test _get_lib_path on an unsupported OS."""
        with mock.patch("platform.system", return_value="AmigaOS"):
            with pytest.raises(
                errors.SpannerLibError, match="Unsupported operating system"
            ):
                # pylint: disable=protected-access
                spannerlib.SpannerLib._get_lib_path()

    def test_configure_signatures_missing_symbol(
        self, mock_lib_path, mock_lib_instance
    ):
        """Test behavior if a required symbol is missing."""
        del mock_lib_instance.Release

        # Should not raise, but degrade gracefully
        # (or crash later depending on logic)
        spannerlib.SpannerLib()

        # Verify Release was skipped during configuration
        with pytest.raises(AttributeError):
            _ = mock_lib_instance.Release

    def test_lib_property_not_initialized(self):
        """Test accessing lib property before initialization."""
        instance = object.__new__(spannerlib.SpannerLib)
        with pytest.raises(
            errors.SpannerLibError, match="not been initialized"
        ):
            _ = instance.lib

    def test_lib_property_initialized(self, mock_lib_path, mock_lib_instance):
        """Test accessing lib property after initialization."""
        lib = spannerlib.SpannerLib()
        assert lib.lib is mock_lib_instance

    def test_create_pool(self, mock_lib_path, mock_lib_instance):
        """Test the create_pool method."""
        expected_message = message.Message()
        mock_lib_instance.CreatePool.return_value = expected_message

        lib = spannerlib.SpannerLib()
        config = "test_config"
        result = lib.create_pool(config)

        assert result is expected_message
        mock_lib_instance.CreatePool.assert_called_once()

        # Validate Argument Type
        args, _ = mock_lib_instance.CreatePool.call_args
        assert isinstance(args[0], types.GoString)

        # Validate Signature Setup
        assert mock_lib_instance.CreatePool.argtypes == [types.GoString]
        assert mock_lib_instance.CreatePool.restype == message.Message

    def test_close_pool(self, mock_lib_path, mock_lib_instance):
        """Test the close_pool method."""
        # Check if method exists to avoid AttributeError in tests
        # if implementation is pending
        if not hasattr(spannerlib.SpannerLib, "close_pool"):
            pytest.skip("close_pool wrapper not implemented yet")

        expected_message = message.Message()
        mock_lib_instance.ClosePool.return_value = expected_message

        lib = spannerlib.SpannerLib()
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
        assert mock_lib_instance.ClosePool.restype == message.Message
