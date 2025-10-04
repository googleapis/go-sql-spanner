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


# Define GoString structure
class GoString(ctypes.Structure):
    """Represents a Go string.pina"""

    _fields_ = [("p", ctypes.c_char_p), ("n", ctypes.c_ssize_t)]


# Define common return structure
class GoReturn(ctypes.Structure):
    """Represents the common return structure from Go functions."""

    _fields_ = [
        ("pinner_id", ctypes.c_longlong),  # result pinnerId - r0
        ("error_code", ctypes.c_int32),  # error code - r1
        ("object_id", ctypes.c_longlong),  # object code - r2
        ("msg_len", ctypes.c_int32),  # msg length - r3
        ("msg", ctypes.c_void_p),  # msg string - r4
    ]


def print_go_string(go_string: GoString):
    """Prints the contents of a GoString."""
    print(f"Length field (n): {go_string.n}")
    print(f"Pointer field (p) as bytes: {go_string.p}")


def to_go_string(s: str) -> GoString:
    """Converts a Python string to a GoString."""
    encoded_s = s.encode("utf-8")
    return GoString(encoded_s, len(encoded_s))
