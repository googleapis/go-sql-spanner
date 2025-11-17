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
"""Unit tests for the Pool class."""

import ctypes
import unittest
from unittest import mock

from google.cloud.spannerlib.internal.errors import SpannerLibError
from google.cloud.spannerlib.internal.message import Message
from google.cloud.spannerlib.pool import Pool


class TestPool(unittest.TestCase):
    """Unit tests for the Pool class."""

    @mock.patch("google.cloud.spannerlib.pool.SpannerLib")
    def test_create_pool(self, MockSpannerLib):
        """Test that create_pool calls the underlying library function."""
        mock_lib_instance = MockSpannerLib.return_value
        mock_lib_instance.CreatePool.return_value = Message(
            object_id=123, err=""
        )

        connection_string = (
            "projects/test-project/instances/test-instance/databases/test-db"
        )
        pool = Pool.create_pool(connection_string)

        self.assertIsInstance(pool, Pool)
        self.assertEqual(pool.id, 123)
        MockSpannerLib.assert_called_once()  # Check if SpannerLib was instantiated
        mock_lib_instance.CreatePool.assert_called_once()
        # Check that to_go_string was called on the connection string
        args, _ = mock_lib_instance.CreatePool.call_args
        self.assertEqual(args[0].p, connection_string.encode("utf-8"))

    @mock.patch("google.cloud.spannerlib.pool.SpannerLib")
    def test_create_pool_failure(self, MockSpannerLib):
        """Test that create_pool raises SpannerLibError on failure."""
        mock_lib_instance = MockSpannerLib.return_value
        error_msg = b"Pool creation failed"
        msg_ptr = ctypes.cast(ctypes.c_char_p(error_msg), ctypes.c_void_p)
        mock_lib_instance.CreatePool.return_value = Message(
            object_id=0, error_code=1, msg=msg_ptr
        )

        connection_string = "invalid_connection_string"
        with self.assertRaises(SpannerLibError) as context:
            Pool.create_pool(connection_string)

        self.assertIn("Failed to create pool", str(context.exception))
        MockSpannerLib.assert_called_once()
        mock_lib_instance.CreatePool.assert_called_once()

    @mock.patch("google.cloud.spannerlib.internal.spannerlib.SpannerLib")
    def test_close_pool(self, MockSpannerLib):
        """Test that close calls the underlying library function."""
        mock_lib_instance = MockSpannerLib.return_value
        mock_lib_instance.ClosePool = mock.Mock(
            return_value=Message(object_id=0, err="")
        )
        mock_lib_instance.Release = mock.Mock(
            return_value=Message(object_id=0, err="")
        )

        pool = Pool(456, mock_lib_instance)
        self.assertFalse(pool.closed)
        pool.close()

        self.assertTrue(pool.closed)
        mock_lib_instance.ClosePool.assert_called_once_with(456)
        mock_lib_instance.Release.assert_called_once_with(456)

    @mock.patch("google.cloud.spannerlib.internal.spannerlib.SpannerLib")
    def test_close_pool_idempotent(self, MockSpannerLib):
        """Test that closing an already closed pool does nothing."""
        mock_lib_instance = MockSpannerLib.return_value
        mock_lib_instance.ClosePool = mock.Mock(
            return_value=Message(object_id=0, err="")
        )
        mock_lib_instance.Release = mock.Mock(
            return_value=Message(object_id=0, err="")
        )

        pool = Pool(789, mock_lib_instance)
        pool.close()
        self.assertTrue(pool.closed)

        # Reset mocks to check they are not called again
        mock_lib_instance.ClosePool.reset_mock()
        mock_lib_instance.Release.reset_mock()

        pool.close()
        self.assertTrue(pool.closed)
        mock_lib_instance.ClosePool.assert_not_called()
        mock_lib_instance.Release.assert_not_called()

    @mock.patch("google.cloud.spannerlib.internal.spannerlib.SpannerLib")
    def test_close_pool_failure(self, MockSpannerLib):
        """Test that close raises SpannerLibError on failure."""
        mock_lib_instance = MockSpannerLib.return_value
        error_msg = b"Close failed"
        msg_ptr = ctypes.cast(ctypes.c_char_p(error_msg), ctypes.c_void_p)
        mock_lib_instance.ClosePool.return_value = Message(
            object_id=0, error_code=2, msg=msg_ptr
        )

        pool = Pool(101, mock_lib_instance)
        self.assertFalse(pool.closed)

        with self.assertRaises(SpannerLibError) as context:
            pool.close()
            # Assert that Release was not called with the pool ID
            for call in mock_lib_instance.Release.mock_calls:
                args, _ = call
                self.assertNotEqual(args[0], 101)
            
            
if __name__ == "__main__":
    unittest.main()
