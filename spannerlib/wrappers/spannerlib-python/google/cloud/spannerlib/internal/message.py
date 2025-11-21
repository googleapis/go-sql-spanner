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
"""Internal message structure for spannerlib-python wrapper."""
import ctypes
import logging

from .errors import SpannerLibError

logger = logging.getLogger(__name__)


class Message(ctypes.Structure):
    """Represents the raw return structure from SpannerLib (C-Layout).

    This structure maps to the Go return values.

    Memory Safety Note:
        If 'pinner_id' is non-zero, Go is holding a reference to memory.
        This generic response must be processed and then the pinner must be
        freed via the library's free function to prevent memory leaks.

    Attributes:
        pinner_id (ctypes.c_longlong): ID for managing memory in Go (r0).
        error_code (ctypes.c_int32): Error code, 0 for success (r1).
        object_id (ctypes.c_longlong): ID of the created object in Go,
            if any (r2).
        msg_len (ctypes.c_int32): Length of the error message (r3).
        msg (ctypes.c_void_p): Pointer to the error message string,
            if any (r4).
    """

    _fields_ = [
        ("pinner_id", ctypes.c_int64),  # r0: Handle ID for Go memory pinning
        ("error_code", ctypes.c_int32),  # r1: 0 = Success, >0 = Error
        ("object_id", ctypes.c_int64),  # r2: ID of the resulting object
        ("msg_len", ctypes.c_int32),  # r3: Length of error message bytes
        ("msg", ctypes.c_void_p),  # r4: Pointer to error message bytes
    ]

    @property
    def had_error(self) -> bool:
        """Checks if the operation failed."""
        return self.error_code > 0

    @property
    def message(self) -> str:
        """Decodes the raw C-string message into a Python string safely."""
        if not self.msg or self.msg_len <= 0:
            return ""

        try:
            # Read exactly msg_len bytes from the pointer
            raw_bytes = ctypes.string_at(self.msg, self.msg_len)
            return raw_bytes.decode("utf-8")
        except UnicodeDecodeError:
            return "<Decoding Error>"

    def raise_if_error(self) -> None:
        """Raises a SpannerLibError if the response indicates failure.

        Raises:
            SpannerLibError: If error_code != 0.
        """
        if self.had_error:
            err_msg = self.message or "Unknown error occurred"
            logger.error(
                "SpannerLib operation failed: %s (Code: %d)",
                err_msg,
                self.error_code,
            )
            raise SpannerLibError(self.message, self.error_code)
