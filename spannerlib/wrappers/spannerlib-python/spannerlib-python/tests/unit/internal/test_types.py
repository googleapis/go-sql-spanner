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
"""Unit tests for GoString type conversions."""
import ctypes

from google.cloud.spannerlib.internal.types import GoSlice  # type: ignore
from google.cloud.spannerlib.internal.types import GoString  # type: ignore
from google.cloud.spannerlib.internal.types import to_bytes  # type: ignore


class TestGoString:
    """Test suite for GoString structure and logic."""

    def test_round_trip_conversion(self) -> None:
        """Verifies that a string converts to GoString and back identically."""
        original = "Hello, World!"
        go_str = GoString.from_str(original)

        # Verify internal structure
        assert go_str.n == len(original.encode("utf-8"))
        assert str(go_str) == original

    def test_utf8_byte_counting(self) -> None:
        """Verifies that length is calculated in bytes, not characters.

        Go's 'len(string)' returns byte count.
        Python's 'len(str)' returns char count.
        We must match Go's behavior.
        """
        # The fire emoji '🔥' is 1 character but 4 bytes in UTF-8.
        text = "Hot 🔥"
        go_str = GoString.from_str(text)

        # "Hot " (4 bytes) + "🔥" (4 bytes) = 8 bytes
        expected_byte_len = 8

        assert go_str.n == expected_byte_len
        assert go_str.n != len(text)  # length in chars is only 5
        assert str(go_str) == text

    def test_memory_safety_anchor(self) -> None:
        """White-box test to ensure the keep-alive reference is attached."""
        text = "Ephemeral String"
        go_str = GoString.from_str(text)

        # Check if the private attribute exists
        assert hasattr(go_str, "_keep_alive_ref")

        # Ensure it holds the correct encoded bytes
        assert getattr(go_str, "_keep_alive_ref") == text.encode("utf-8")

    def test_handle_none_and_empty(self) -> None:
        """Ensures None and empty strings are handled gracefully."""
        # Empty string
        empty = GoString.from_str("")
        assert empty.n == 0
        assert str(empty) == ""

        # None input
        none_str = GoString.from_str(None)
        assert none_str.n == 0
        assert none_str.p is None
        assert str(none_str) == ""


class TestGoSlice:
    """Test suite for GoSlice structure and logic."""

    def test_from_str_basic(self) -> None:
        """Verifies that a string converts to GoSlice correctly."""
        s = "Hello, World!"
        go_slice = GoSlice.from_str(s)

        encoded = s.encode("utf-8")
        assert go_slice.len == len(encoded)
        assert go_slice.cap == len(encoded)
        assert go_slice.data is not None

    def test_utf8_handling(self) -> None:
        """Verifies UTF-8 handling in GoSlice."""
        s = "Hot 🔥"
        go_slice = GoSlice.from_str(s)

        encoded = s.encode("utf-8")
        assert go_slice.len == len(encoded)
        assert go_slice.cap == len(encoded)

    def test_memory_safety_anchor(self) -> None:
        """White-box test to ensure the keep-alive reference is attached."""
        s = "Ephemeral String"
        go_slice = GoSlice.from_str(s)

        # Check if the private attribute exists
        assert hasattr(go_slice, "_keep_alive_ref")

        # Ensure it holds the correct encoded bytes
        # create_string_buffer returns an object that has a .value attribute
        # with the bytes
        assert getattr(go_slice, "_keep_alive_ref").value == s.encode("utf-8")

    def test_from_bytes_basic(self) -> None:
        """Verifies that bytes convert to GoSlice correctly."""
        b = b"Hello, Bytes!"
        go_slice = GoSlice.from_bytes(b)

        assert go_slice.len == len(b)
        assert go_slice.cap == len(b)
        assert go_slice.data is not None

    def test_from_bytes_memory_safety(self) -> None:
        """White-box test to ensure the keep-alive reference is attached
        for bytes."""
        b = b"Ephemeral Bytes"
        go_slice = GoSlice.from_bytes(b)

        # Check if the private attribute exists
        assert hasattr(go_slice, "_keep_alive_ref")

        # Ensure it holds the correct bytes
        assert getattr(go_slice, "_keep_alive_ref").value == b


class TestToBytes:
    """Test suite for to_bytes function."""

    def test_basic_conversion(self) -> None:
        """Verifies that a c_void_p converts to bytes correctly."""
        original = b"Hello, World!"
        # Create a buffer
        buff = ctypes.create_string_buffer(original)
        # Get pointer and length
        ptr = ctypes.cast(buff, ctypes.c_void_p)
        length = ctypes.c_int32(len(original))

        result = to_bytes(ptr, length)
        assert result == original

    def test_empty_conversion(self) -> None:
        """Verifies that an empty buffer converts to empty bytes."""
        original = b""
        buff = ctypes.create_string_buffer(original)
        ptr = ctypes.cast(buff, ctypes.c_void_p)
        length = ctypes.c_int32(0)

        result = to_bytes(ptr, length)
        assert result == original

    def test_partial_conversion(self) -> None:
        """Verifies that we can read a subset of the buffer."""
        original = b"Hello, World!"
        buff = ctypes.create_string_buffer(original)
        ptr = ctypes.cast(buff, ctypes.c_void_p)
        # Read only "Hello"
        length = ctypes.c_int32(5)

        result = to_bytes(ptr, length)
        assert result == b"Hello"
