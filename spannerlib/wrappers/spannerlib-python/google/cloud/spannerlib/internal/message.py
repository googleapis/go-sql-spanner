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

from __future__ import absolute_import

import ctypes
import logging

logger = logging.getLogger(__name__)


class Message(ctypes.Structure):
    """Represents the common return structure from SpannerLib.

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
