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


class GoString(ctypes.Structure):
    """Represents a Go string for C interop.

    Fields:
        p: Pointer to the first byte of the string data.
        n: Length of the string.
    """

    _fields_ = [("p", ctypes.c_char_p), ("n", ctypes.c_ssize_t)]


class GoSlice(ctypes.Structure):
    """Represents a Go slice for C interop.
    Fields:
        data: Pointer to the first element of the slice.
        len: Length of the slice.
        cap: Capacity of the slice.
    """

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
