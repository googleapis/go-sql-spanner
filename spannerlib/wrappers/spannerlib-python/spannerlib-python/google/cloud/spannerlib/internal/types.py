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
"""CTypes definitions for interacting with the Go library."""
import ctypes
import logging
from typing import Optional

# Configure logger
logger = logging.getLogger(__name__)


def to_bytes(msg: ctypes.c_void_p, len: ctypes.c_int32) -> bytes:
    """Converts shared lib msg to a bytes."""
    return ctypes.string_at(msg, len)


class GoString(ctypes.Structure):
    """Represents a Go string for C interop.

    This structure maps to the standard Go string header:
    struct String { byte* str; int len; };

    Attributes:
        p (ctypes.c_char_p): Pointer to the first byte of the string data.
        n (ctypes.c_int64): Length of the string.
    """

    _fields_ = [
        ("p", ctypes.c_char_p),
        ("n", ctypes.c_int64),
    ]

    def __str__(self) -> str:
        """Decodes the GoString back to a Python string."""
        if not self.p or self.n == 0:
            return ""
        # We must specify the length to read exactly n bytes, as Go strings
        # are not null-terminated.
        return ctypes.string_at(self.p, self.n).decode("utf-8")

    @classmethod
    def from_str(cls, s: Optional[str]) -> "GoString":
        """Creates a GoString from a Python string safely.

        CRITICAL: This method attaches the encoded bytes to the structure
        instance to prevent Python's Garbage Collector from freeing the
        memory while Go is using it.

        Args:
            s (str): The Python string.

        Returns:
            GoString: The C-compatible structure.
        """
        if s is None:
            return cls(None, 0)

        try:
            encoded_s = s.encode("utf-8")
        except UnicodeError as e:
            logger.error("Failed to encode string for Go interop: %s", e)
            raise

        # Create the structure instance
        instance = cls(encoded_s, len(encoded_s))

        # Monkey-patch the bytes object onto the instance to keep the reference
        # alive. This prevents the GC from reaping 'encoded_s' while 'instance'
        # exists.
        setattr(instance, "_keep_alive_ref", encoded_s)

        return instance


class GoSlice(ctypes.Structure):
    """Represents a Go slice for C interop.

    This structure maps to the standard Go slice header:
    struct Slice { void* data; int64 len; int64 cap; };

    Attributes:
        data (ctypes.c_void_p): Pointer to the first element of the slice.
        len (ctypes.c_longlong): Length of the slice.
        cap (ctypes.c_longlong): Capacity of the slice.
    """

    _fields_ = [
        ("data", ctypes.c_void_p),
        ("len", ctypes.c_longlong),
        ("cap", ctypes.c_longlong),
    ]

    @classmethod
    def from_str(cls, s: Optional[str]) -> "GoSlice":
        """Converts a Python string to a GoSlice (byte slice).

        Args:
            s (str): The Python string to convert.

        Returns:
            GoSlice: The C-compatible structure representing a []byte.
        """
        if s is None:
            return cls(None, 0, 0)

        encoded_s = s.encode("utf-8")
        n = len(encoded_s)

        # Create a C-compatible mutable buffer from the bytes.
        # Note: create_string_buffer creates a mutable copy.
        # This is safe because:
        # 1. It matches Go's []byte which is mutable.
        # 2. It isolates the original Python object from modification.
        buffer = ctypes.create_string_buffer(encoded_s)

        # Create the GoSlice
        instance = cls(
            data=ctypes.cast(buffer, ctypes.c_void_p),
            # For a new slice from a string, len and cap are the same
            len=n,
            cap=n,
        )

        # Keep a reference to the buffer to prevent garbage collection
        setattr(instance, "_keep_alive_ref", buffer)

        return instance

    @classmethod
    def from_bytes(cls, b: bytes) -> "GoSlice":
        """Converts Python bytes to a GoSlice (byte slice).

        Args:
            b (bytes): The Python bytes to convert.

        Returns:
            GoSlice: The C-compatible structure representing a []byte.
        """
        n = len(b)

        # Create a C-compatible mutable buffer from the bytes.
        # Note: create_string_buffer creates a mutable copy.
        # This is safe because:
        # 1. It matches Go's []byte which is mutable.
        # 2. It isolates the original Python object from modification.
        buffer = ctypes.create_string_buffer(b)

        # Create the GoSlice
        instance = cls(
            data=ctypes.cast(buffer, ctypes.c_void_p),
            len=n,
            cap=n,
        )

        # Keep a reference to the buffer to prevent garbage collection
        setattr(instance, "_keep_alive_ref", buffer)

        return instance
