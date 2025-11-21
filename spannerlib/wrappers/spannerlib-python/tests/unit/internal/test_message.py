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

import ctypes

import pytest

from google.cloud.spannerlib.internal import errors  # type: ignore
from google.cloud.spannerlib.internal import message  # type: ignore


class TestMessage:
    """Test suite for the Message class."""

    def test_message_fields(self) -> None:
        """Test that the Message structure has the correct fields."""
        # Access to ctypes Structure _fields_ is required here
        fields = message.Message._fields_  # pylint: disable=protected-access
        expected_fields = [
            ("pinner_id", ctypes.c_int64),
            ("error_code", ctypes.c_int32),
            ("object_id", ctypes.c_int64),
            ("msg_len", ctypes.c_int32),
            ("msg", ctypes.c_void_p),
        ]
        assert fields == expected_fields

    def test_message_had_error(self) -> None:
        """Test the had_error property."""
        msg = message.Message()
        msg.error_code = 0
        assert not msg.had_error

        msg.error_code = 1
        assert msg.had_error

        # Should not happen, but test a non-zero
        msg.error_code = -1
        assert not msg.had_error

    def test_message_message_property_null_ptr(self) -> None:
        """Test the message property when msg is NULL."""
        msg = message.Message()
        msg.msg = None
        msg.msg_len = 10
        assert msg.message == ""

    def test_message_message_property_zero_len(self) -> None:
        """Test the message property when msg_len is 0."""
        msg = message.Message()
        c_str = ctypes.c_char_p(b"Hello")
        msg.msg = ctypes.cast(c_str, ctypes.c_void_p)
        msg.msg_len = 0
        assert msg.message == ""

    def test_message_message_property_valid(self) -> None:
        """Test the message property with a valid C string."""
        msg = message.Message()
        text = "Hello, SpannerLib!"
        text_bytes = text.encode("utf-8")
        c_str = ctypes.c_char_p(text_bytes)
        msg.msg = ctypes.cast(c_str, ctypes.c_void_p)
        msg.msg_len = len(text_bytes)
        assert msg.message == text

    def test_message_message_property_decoding_error(self) -> None:
        """Test the message property with bytes that are not valid UTF-8."""
        msg = message.Message()
        invalid_bytes = b"\xff\xfe\xfd"
        c_str = ctypes.c_char_p(invalid_bytes)
        msg.msg = ctypes.cast(c_str, ctypes.c_void_p)
        msg.msg_len = len(invalid_bytes)
        assert msg.message == "<Decoding Error>"

    def test_raise_if_error_no_error(self) -> None:
        """Test raise_if_error when there is no error."""
        msg = message.Message()
        msg.error_code = 0
        try:
            msg.raise_if_error()
        except errors.SpannerLibError:
            pytest.fail("SpannerLibError raised unexpectedly")

    def test_raise_if_error_with_error(self) -> None:
        """Test raise_if_error when there is an error."""
        msg = message.Message()
        msg.error_code = 5
        error_text = "Something broke"
        error_bytes = error_text.encode("utf-8")
        c_str = ctypes.c_char_p(error_bytes)
        msg.msg = ctypes.cast(c_str, ctypes.c_void_p)
        msg.msg_len = len(error_bytes)

        with pytest.raises(errors.SpannerLibError) as excinfo:
            msg.raise_if_error()

        assert excinfo.value.error_code == 5
        assert excinfo.value.raw_message == error_text
        assert str(excinfo.value) == f"[Err 5] {error_text}"

    def test_raise_if_error_with_error_no_message(self) -> None:
        """Test raise_if_error when there is an error but no message."""
        msg = message.Message()
        msg.error_code = 2
        msg.msg = None

        with pytest.raises(errors.SpannerLibError) as excinfo:
            msg.raise_if_error()

        assert excinfo.value.error_code == 2
        assert excinfo.value.raw_message == ""
        # The error message in the exception will be the formatted one
        assert str(excinfo.value) == "[Err 2] "
