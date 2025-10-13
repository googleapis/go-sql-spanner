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
from __future__ import absolute_import

import ctypes
import logging

logger = logging.getLogger(__name__)


# Define GoString structure, matching the Go layout.
class GoString(ctypes.Structure):
    """Represents a Go string for C interop.

    Fields:
        p: Pointer to the first byte of the string data.
        n: Length of the string.
    """

    _fields_ = [("p", ctypes.c_char_p), ("n", ctypes.c_ssize_t)]


# Define common return structure from Go functions.
class GoReturn(ctypes.Structure):
    """Represents the common return structure from Go functions.

    Fields:
        pinner_id: ID for managing memory in Go (r0).
        error_code: Error code, 0 for success (r1).
        object_id: ID of the created object in Go, if any (r2).
        msg_len: Length of the error message (r3).
        msg: Pointer to the error message string, if any (r4).
    """

    _fields_ = [
        ("pinner_id", ctypes.c_longlong),  # result pinnerId - r0
        ("error_code", ctypes.c_int32),  # error code - r1
        ("object_id", ctypes.c_longlong),  # object code - r2
        ("msg_len", ctypes.c_int32),  # msg length - r3
        ("msg", ctypes.c_void_p),  # msg string - r4
    ]


class GoSlice(ctypes.Structure):
    _fields_ = [
        ("data", ctypes.c_void_p),
        ("len", ctypes.c_longlong),
        ("cap", ctypes.c_longlong),
    ]


def to_go_string(s: str) -> GoString:
    """Converts a Python string to a GoString.

    Args:
        s: The Python string to convert.

    Returns:
        GoString: A GoString instance."""
    encoded_s = s.encode("utf-8")
    return GoString(encoded_s, len(encoded_s))


def to_go_slice(s: str) -> GoSlice:
    """Converts a Python string to a GoSlice."""
    encoded_s = s.encode("utf-8")
    n = len(encoded_s)

    # Create a C-compatible mutable buffer from the bytes
    # This is the memory that the GoSlice will point to.
    buffer = ctypes.create_string_buffer(encoded_s)
    # Create the GoSlice
    return GoSlice(
        data=ctypes.cast(
            buffer, ctypes.c_void_p
        ),  # Cast the buffer to a void pointer
        len=n,
        cap=n,  # For a new slice from a string, len and cap are the same
    )


def serialized_bytes_to_go_slice(serialized_bytes: bytes) -> GoSlice:
    """Converts a Python string to a GoSlice."""
    slice_len = len(serialized_bytes)
    go_slice = GoSlice(
        data=ctypes.cast(serialized_bytes, ctypes.c_void_p),
        len=slice_len,
        cap=slice_len,
    )
    go_slice._keepalive = serialized_bytes
    return go_slice


def to_bytes(msg: ctypes.c_void_p, len: ctypes.c_int32) -> bytes:
    """Converts shared lib msg to a bytes."""
    return ctypes.string_at(msg, len)
