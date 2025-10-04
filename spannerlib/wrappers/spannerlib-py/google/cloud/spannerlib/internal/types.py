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


def print_go_string(go_string: GoString):
    """Helper function to print the contents of a GoString for debugging."""
    print(f"GoString Length (n): {go_string.n}")
    if go_string.p:
        try:
            py_string = go_string.p[: go_string.n].decode("utf-8")
            print(f"GoString Content (p): {py_string}")
        except UnicodeDecodeError:
            print(f"GoString Content (p) as bytes: {go_string.p[:go_string.n]}")
    else:
        print("GoString Content (p): NULL")


def to_go_string(s: str) -> GoString:
    """Converts a Python string to a GoString.

    Args:
        s: The Python string to convert.

    Returns:
        GoString: A GoString instance."""
    encoded_s = s.encode("utf-8")
    return GoString(encoded_s, len(encoded_s))
