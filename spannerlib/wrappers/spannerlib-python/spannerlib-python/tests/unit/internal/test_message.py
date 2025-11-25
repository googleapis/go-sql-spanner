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
"""Unit tests for the Message structure and its behavior."""
import ctypes
import gc
import logging
from unittest.mock import Mock
import warnings

import pytest

from google.cloud.spannerlib.internal import Message  # type: ignore
from google.cloud.spannerlib.internal import SpannerLibError  # type: ignore

# --- Fixtures ---


@pytest.fixture
def mock_lib():
    """Creates a mock object mimicking the C-Library."""
    lib = Mock(spec=ctypes.CDLL)
    # Mock the release function
    lib.release = Mock()
    return lib


@pytest.fixture
def raw_message_data():
    """Creates a raw C-string buffer for testing string decoding."""
    text = "Database connection timeout"
    # We must keep this buffer alive for the duration of the test
    # so the pointer remains valid.
    c_buffer = ctypes.create_string_buffer(text.encode("utf-8"))
    return c_buffer, len(text)


# --- Tests ---


class TestMessageLifecycle:
    """Tests for the Message Lifecycle."""

    def test_initialization(self):
        """Test that the structure initializes with default zero values."""
        msg = Message()
        assert msg.pinner_id == 0
        assert msg.error_code == 0
        assert msg.had_error is False

    def test_context_manager_auto_release(self, mock_lib):
        """Verify the 'with' statement triggers the release function."""
        pinner_id = 12345

        with Message() as msg:
            msg.pinner_id = pinner_id
            msg.bind_library(mock_lib)
            # pylint: disable=protected-access
            assert msg._is_released is False

        # After exit, release should have been called
        assert msg._is_released is True  # pylint: disable=protected-access
        mock_lib.release.assert_called_once_with(pinner_id)

    def test_manual_release_idempotency(self, mock_lib):
        """Verify calling release multiple times is safe (idempotent)."""
        msg = Message()
        msg.pinner_id = 999
        msg.bind_library(mock_lib)

        # First Call
        msg.release()
        assert msg._is_released is True  # pylint: disable=protected-access
        assert mock_lib.release.call_count == 1

        # Second Call
        msg.release()
        assert mock_lib.release.call_count == 1  # Should not increase

    def test_release_without_binding_logs_critical(self, caplog):
        """Verify that releasing without a library logs a CRITICAL error."""
        msg = Message()
        msg.pinner_id = 555

        # Note: We intentionally do NOT call bind_library()

        with caplog.at_level(logging.CRITICAL):
            msg.release()

        assert "cannot be released" in caplog.text
        assert "Library dependency was not injected" in caplog.text
        # Ensure we marked it as released to prevent infinite retry loops
        assert msg._is_released is True  # pylint: disable=protected-access


class TestMessageErrorHandling:
    """Tests for the Message Error Handling."""

    def test_had_error_property(self):
        """Tests the had_error property logic."""
        msg = Message()
        msg.error_code = 0
        assert not msg.had_error

        msg.error_code = 1
        assert msg.had_error

    def test_raise_if_error_success(self):
        """Should do nothing if error_code is 0."""
        msg = Message()
        msg.error_code = 0
        # Should not raise
        msg.raise_if_error()

    def test_raise_if_error_failure(self, raw_message_data):
        """Should raise SpannerLibError if error_code > 0."""
        c_buffer, length = raw_message_data

        msg = Message()
        msg.error_code = 500
        msg.msg = ctypes.cast(c_buffer, ctypes.c_void_p)
        msg.msg_len = length

        with pytest.raises(SpannerLibError) as exc_info:
            msg.raise_if_error()

        assert "Database connection timeout" in str(exc_info.value)
        assert "[Err 500]" in str(exc_info.value)


class TestMessageStringDecoding:
    """Tests for the Message String Decoding."""

    def test_decode_valid_string(self, raw_message_data):
        """Test decoding of a valid UTF-8 C-string."""
        c_buffer, length = raw_message_data

        msg = Message()
        msg.msg = ctypes.cast(c_buffer, ctypes.c_void_p)
        msg.msg_len = length

        assert msg.message == "Database connection timeout"

    def test_decode_empty_or_null(self):
        """Test handling of empty or null message pointers."""
        msg = Message()
        msg.msg = None
        msg.msg_len = 0
        assert msg.message == ""

    def test_decode_invalid_utf8(self):
        """Test handling of non-UTF8 bytes (garbage memory)."""
        # Create a buffer with invalid UTF-8 bytes (0xFF is invalid)
        bad_bytes = b"\xff\xff\xff"
        c_buffer = ctypes.create_string_buffer(bad_bytes)

        msg = Message()
        msg.msg = ctypes.cast(c_buffer, ctypes.c_void_p)
        msg.msg_len = len(bad_bytes)

        assert msg.message == "<Decoding Error>"


class TestMessageSafetyNet:
    """Tests for the Message Safety Net (__del__ warning)."""

    def test_del_warning_on_leak(self):
        """
        Verify that __del__ emits a ResourceWarning if the object is
        garbage collected without being released.
        """
        # We need to suppress the actual stderr output from __del__
        # but catch the warning it emits.

        with pytest.warns(ResourceWarning, match="Unclosed SpannerLib Message"):
            # 1. Create a leaky object (scope limited)
            def create_leak():
                msg = Message()
                msg.pinner_id = 777
                # Do NOT call release() or use context manager
                return

            create_leak()

            # 2. Force Garbage Collection
            # This forces __del__ to run immediately
            gc.collect()

    def test_del_no_warning_if_clean(self):
        """Verify no warning is issued if the object was properly released."""
        with warnings.catch_warnings(record=True) as recorded_warnings:
            warnings.simplefilter("always")  # Capture everything

            def create_clean():
                with Message() as msg:
                    msg.pinner_id = 888
                    # release happens here automatically

            create_clean()
            gc.collect()

            # Filter for our specific ResourceWarning
            relevant = [
                w
                for w in recorded_warnings
                if "Unclosed SpannerLib Message" in str(w.message)
            ]
            assert len(relevant) == 0
