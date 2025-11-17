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

"""Unit tests for the MessageHandler class."""

import ctypes
import unittest
from unittest import mock

from google.cloud.spannerlib.internal.message import Message
from google.cloud.spannerlib.internal.message_handler import MessageHandler
from google.cloud.spannerlib.internal.spannerlib import SpannerLib


class TestMessageHandler(unittest.TestCase):
    """Unit tests for the MessageHandler class."""

    def setUp(self):
        self.mock_lib = mock.create_autospec(SpannerLib)
        self.mock_lib.Release = mock.MagicMock()

    def test_initialization(self):
        """Test that MessageHandler can be initialized."""
        msg = Message(object_id=0, error_code=0, msg=None)
        handler = MessageHandler(msg, self.mock_lib)
        self.assertIsInstance(handler, MessageHandler)
        self.assertEqual(handler.message, msg)

    def test_code(self):
        """Test the code() method."""
        msg = Message(object_id=0, error_code=123, msg=None)
        handler = MessageHandler(msg, self.mock_lib)
        self.assertEqual(handler.code(), 123)

    def test_has_error_true(self):
        """Test has_error() when there is an error."""
        msg = Message(object_id=0, error_code=1, msg=None)
        handler = MessageHandler(msg, self.mock_lib)
        self.assertTrue(handler.has_error())

    def test_has_error_false(self):
        """Test has_error() when there is no error."""
        msg = Message(object_id=0, error_code=0, msg=None)
        handler = MessageHandler(msg, self.mock_lib)
        self.assertFalse(handler.has_error())

    def test_error_message_valid(self):
        """Test error_message() with a valid Go error message."""
        error_msg = b"Something went wrong"
        msg_ptr = ctypes.cast(ctypes.c_char_p(error_msg), ctypes.c_void_p)
        msg = Message(object_id=0, error_code=1, msg=msg_ptr, pinner_id=123)
        handler = MessageHandler(msg, self.mock_lib)
        self.assertEqual(handler.error_message(), "Something went wrong")

    def test_error_message_invalid_utf8(self):
        """Test error_message() with a message that is not valid UTF-8."""
        error_msg = b"Invalid UTF-8 \xff"
        msg_ptr = ctypes.cast(ctypes.c_char_p(error_msg), ctypes.c_void_p)
        msg = Message(object_id=0, error_code=1, msg=msg_ptr, pinner_id=123)
        handler = MessageHandler(msg, self.mock_lib)
        self.assertIn("Invalid UTF-8", handler.error_message())
        self.assertIn("\ufffd", handler.error_message())

    def test_error_message_no_go_message(self):
        """Test error_message() when the Go message pointer is null."""
        msg = Message(object_id=0, error_code=1, msg=None, pinner_id=123)
        handler = MessageHandler(msg, self.mock_lib)
        self.assertEqual(handler.error_message(), "Unknown error")

    def test_release(self):
        """Test the release() method."""
        msg = Message(object_id=0, error_code=0, msg=None, pinner_id=456)
        handler = MessageHandler(msg, self.mock_lib)
        handler.release()
        self.mock_lib.Release.assert_called_once_with(456)

    def test_dispose_no_pinner_id(self):
        """Test release() when pinner_id is not set."""
        msg = Message(object_id=0, error_code=0, msg=None)
        handler = MessageHandler(msg, self.mock_lib)
        handler.release()
        self.mock_lib.Release.assert_called_once_with(0)


if __name__ == "__main__":
    unittest.main()
