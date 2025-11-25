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
"""Unit tests for Pool behavior."""
import ctypes  # Add this import
from unittest.mock import patch

import pytest

from google.cloud.spannerlib import Pool  # type: ignore
from google.cloud.spannerlib.internal import SpannerLibError  # type: ignore
from google.cloud.spannerlib.internal.message import Message  # type: ignore

# -----------------------------------------------------------------------------
# Fixtures & Helpers
# -----------------------------------------------------------------------------


@pytest.fixture
def mock_spanner_lib_class():
    """
    Patches the SpannerLib class in the 'pool' module namespace.
    Returns the Mock CLASS, not an instance.
    """
    with patch("google.cloud.spannerlib.pool.SpannerLib") as MockClass:
        yield MockClass


@pytest.fixture
def mock_lib_instance(mock_spanner_lib_class):
    """
    Returns the specific instance of SpannerLib that the Pool will use.
    """
    instance = mock_spanner_lib_class.return_value
    return instance


@pytest.fixture
def mock_msg_data():
    """
    Provides raw data for a Message object.
    """
    return {
        "pinner_id": 0,
        "error_code": 0,
        "object_id": 555,  # Default test ID
        "msg_len": 0,
        "msg": None,
    }


@pytest.fixture
def setup_spannerlib_method(mock_lib_instance, mock_msg_data):
    """
    Helper to setup the mock response for SpannerLib methods like create_pool and close_pool.
    """

    def _configure_method(method_name, return_msg_data=None):
        if return_msg_data is None:
            return_msg_data = mock_msg_data

        # Convert msg to C char pointer if present
        if return_msg_data["msg"]:
            c_msg = ctypes.c_char_p(return_msg_data["msg"])
            return_msg_data["msg"] = ctypes.cast(c_msg, ctypes.c_void_p)

        msg = Message(**return_msg_data)
        getattr(mock_lib_instance, method_name).return_value = msg
        return msg

    return _configure_method


class TestPool:
    """Tests for the Pool class method."""

    def test_create_pool_success(
        self,
        mock_spanner_lib_class,
        mock_lib_instance,
        setup_spannerlib_method,
        mock_msg_data,
    ):  # pylint: disable=redefined-outer-name
        """Test successful creation of a Pool."""
        # 1. Setup
        setup_spannerlib_method("create_pool")
        conn_string = "projects/test/instances/test/databases/test"

        # 2. Execute
        pool = Pool.create_pool(conn_string)

        # 3. Assertions
        mock_spanner_lib_class.assert_called_once()
        mock_lib_instance.create_pool.assert_called_once_with(conn_string)

        assert isinstance(pool, Pool)
        assert pool.oid == mock_msg_data["object_id"]
        assert pool.spannerlib == mock_lib_instance

    def test_create_pool_lib_error(
        self,
        mock_spanner_lib_class,
        mock_lib_instance,
        setup_spannerlib_method,
        mock_msg_data,
    ):  # pylint: disable=redefined-outer-name
        """Test that SpannerLibError from the underlying lib is propagated."""
        # 1. Setup
        mock_msg_data["error_code"] = 1
        mock_msg_data["msg"] = b"Connection failed"
        mock_msg_data["msg_len"] = len(b"Connection failed")
        setup_spannerlib_method("create_pool")

        # 2. Execute & Assert
        with pytest.raises(SpannerLibError) as exc_info:
            Pool.create_pool("bad_connection")

        assert "Connection failed" in str(exc_info.value)

    def test_create_pool_unexpected_error(
        self, mock_spanner_lib_class, mock_lib_instance, setup_spannerlib_method
    ):  # pylint: disable=redefined-outer-name
        """Test that unexpected generic exceptions are wrapped
        in SpannerLibError."""
        # 1. Setup
        mock_lib_instance.create_pool.side_effect = ValueError(
            "Something weird happened"
        )

        # 2. Execute & Assert
        with pytest.raises(SpannerLibError) as exc_info:
            Pool.create_pool("conn_str")

        # Ensure the original error is chained or mentioned
        msg = str(exc_info.value)
        assert "Unexpected error: Something weird happened" in msg

    def test_close_pool_success(
        self, mock_lib_instance, setup_spannerlib_method, mock_msg_data
    ):  # pylint: disable=redefined-outer-name
        """Test successful closing of the Pool."""
        # 1. Setup
        pool = Pool(spannerlib=mock_lib_instance, oid=100)
        setup_spannerlib_method("close_pool")

        # 2. Execute
        pool.close()

        # 3. Assertions
        mock_lib_instance.close_pool.assert_called_once_with(100)
        assert pool._is_disposed is True  # pylint: disable=protected-access

    def test_close_pool_propagates_lib_error(
        self, mock_lib_instance, setup_spannerlib_method, mock_msg_data
    ):  # pylint: disable=redefined-outer-name
        """Test handling of SpannerLibError during close."""
        pool = Pool(spannerlib=mock_lib_instance, oid=100)
        mock_msg_data["error_code"] = 1
        mock_msg_data["msg"] = b"Failed to release"
        mock_msg_data["msg_len"] = len(b"Failed to release")
        setup_spannerlib_method("close_pool")

        with pytest.raises(SpannerLibError) as exc_info:
            pool.close()

        assert "Failed to release" in str(exc_info.value)

    def test_close_pool_wraps_unexpected_error(
        self, mock_lib_instance, setup_spannerlib_method
    ):  # pylint: disable=redefined-outer-name
        """Test wrapping of generic errors during close."""
        pool = Pool(spannerlib=mock_lib_instance, oid=100)
        mock_lib_instance.close_pool.side_effect = TypeError(
            "Internal type error"
        )

        with pytest.raises(SpannerLibError) as exc_info:
            pool.close()

        assert "Unexpected error during close: Internal type error" in str(
            exc_info.value
        )
