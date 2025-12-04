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
"""Unit tests for AbstractLibraryObject behavior."""
import gc
from typing import Generator
from unittest.mock import Mock

import pytest

from google.cloud.spannerlib.abstract_library_object import (  # type: ignore
    AbstractLibraryObject,
    ObjectClosedError,
)

# --- 1. Concrete Implementation for Testing ---


class ConcreteTestObject(AbstractLibraryObject):
    """
    A concrete implementation of the AbstractLibObject for testing purposes.
    It spies on the _close_lib_object method to verify it was called.
    """

    def __init__(self, spannerlib, oid):
        super().__init__(spannerlib, oid)
        # Mocking the internal cleanup hook to track calls
        self.cleanup_called_count = 0

    def _close_lib_object(self) -> None:
        """Implementation of the abstract method."""
        self.cleanup_called_count += 1


# --- 2. Fixtures ---


@pytest.fixture
def mock_spanner_lib() -> Mock:
    """Provides a mock for the SpannerLibProtocol."""
    return Mock(spec=["some_lib_method"])


@pytest.fixture
def test_obj(mock_spanner_lib) -> Generator[ConcreteTestObject, None, None]:
    """Creates a fresh ConcreteTestObject for each test."""
    obj = ConcreteTestObject(mock_spanner_lib, oid=123)
    yield obj
    # Teardown: ensure we don't leave dangling resources in tests
    try:
        obj.close()
    except ObjectClosedError:
        pass


# --- 3. Test Suite ---


class TestAbstractLibraryObject:
    """Unit tests for AbstractLibraryObject behavior."""

    def test_abc_instantiation_fails(self):
        """Ensure the Abstract Base Class cannot be instantiated directly."""
        # We try to instantiate AbstractLibraryObject directly,
        # # not the concrete one
        with pytest.raises(TypeError) as exc:
            # pylint: disable=abstract-class-instantiated
            AbstractLibraryObject(Mock(), 1)  # type: ignore

        assert "Can't instantiate abstract class" in str(exc.value)

    def test_initialization(self, test_obj, mock_spanner_lib):
        """Test proper attribute assignment upon initialization."""
        assert test_obj.oid == 123
        assert test_obj.spannerlib == mock_spanner_lib
        # pylint: disable=protected-access
        assert test_obj._is_disposed is False
        assert test_obj.cleanup_called_count == 0

    def test_context_manager_lifecycle(self, mock_spanner_lib):
        """Verify __enter__ and __exit__ work as expected."""

        with ConcreteTestObject(mock_spanner_lib, 456) as obj:
            assert obj.oid == 456
            # pylint: disable=protected-access
            assert obj._is_disposed is False
            # Ensure we can use the object inside the block
            obj._check_disposed()

        # After block, object should be disposed
        assert obj._is_disposed is True  # pylint: disable=protected-access
        assert obj.cleanup_called_count == 1

        # Verify accessing it now raises error
        with pytest.raises(ObjectClosedError):
            obj._check_disposed()  # pylint: disable=protected-access

    def test_manual_close(self, test_obj):
        """Verify manual .close() works identical to context manager."""
        test_obj.close()

        # pylint: disable=protected-access
        assert test_obj._is_disposed is True
        assert test_obj.cleanup_called_count == 1

        with pytest.raises(ObjectClosedError):
            test_obj._check_disposed()

    def test_double_dispose_is_safe(self, test_obj):
        """
        Verify that calling close() multiple times is idempotent
        and does not trigger the underlying cleanup twice.
        """
        # 1. First Close
        test_obj.close()
        assert test_obj.cleanup_called_count == 1

        # 2. Second Close
        test_obj.close()
        # Count should STILL be 1. If it's 2, we have a double-free bug.
        assert test_obj.cleanup_called_count == 1

    def test_check_disposed_raises_error(self, test_obj):
        """Test the guard clause logic."""
        # Should not raise when alive
        test_obj._check_disposed()  # pylint: disable=protected-access

        test_obj.close()

        # Should raise when dead
        with pytest.raises(ObjectClosedError) as exc:
            test_obj._check_disposed()

        assert "ConcreteTestObject has already been disposed" in str(exc.value)

    def test_dispose_handles_zero_oid(self, mock_spanner_lib):
        """
        Verify that if OID is 0 (invalid/uninitialized), we skip the cleanup
        logic but still mark as disposed.
        """
        obj = ConcreteTestObject(mock_spanner_lib, oid=0)
        obj.close()

        assert obj._is_disposed is True  # pylint: disable=protected-access
        # Should NOT call cleanup for OID 0 to prevent C-library errors
        assert obj.cleanup_called_count == 0

    def test_del_warning_on_leak(self, mock_spanner_lib):
        """
        Verify __del__ emits ResourceWarning if object is garbage collected
        without being closed.
        """

        # We create a function to limit the scope of the variable
        def create_leaky_object():
            obj = ConcreteTestObject(mock_spanner_lib, oid=999)
            return obj
            # obj goes out of scope here, but is not closed

        # We use pytest.warns to assert the warning is caught
        with pytest.warns(
            ResourceWarning, match="Unclosed ConcreteTestObject"
        ) as record:
            create_leaky_object()

            # Force Garbage Collection to trigger __del__
            gc.collect()

        # Check stack level is correct (optional but good practice)
        assert len(record) > 0
