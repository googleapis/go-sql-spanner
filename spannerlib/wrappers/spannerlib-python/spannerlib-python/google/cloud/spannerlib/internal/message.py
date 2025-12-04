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
from types import TracebackType
from typing import Optional, Protocol, Type, runtime_checkable
import warnings

from .errors import SpannerLibError

logger = logging.getLogger(__name__)


@runtime_checkable
class ReleasableProtocol(Protocol):
    """Protocol for libraries that can release pinned memory."""

    def release(self, handle: int) -> int:
        """Calls the Release function from the shared object."""
        ...


class Message(ctypes.Structure):
    """Represents the raw return structure from SpannerLib (C-Layout).

    This structure maps to the Go return values.

    It acts as a 'Smart Record' that holds a reference to its parent library
    to facilitate self-cleanup.

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
        (
            "msg_len",
            ctypes.c_int32,
        ),  # r3: Length of result or error message bytes
        (
            "msg",
            ctypes.c_void_p,
        ),  # r4: Pointer to result or error message bytes
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Dependency Injection slot (not part of C structure)
        self._lib: Optional[ReleasableProtocol] = None
        self._is_released: bool = False

    def bind_library(self, lib: ReleasableProtocol) -> "Message":
        """Injects the library instance needed for cleanup.

        Args:
            lib: The ctypes library instance containing the
            'Release' function.
        """
        self._lib = lib
        return self

    def __enter__(self) -> "Message":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.release()

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

    def release(self) -> None:
        """Releases memory using the injected library instance."""
        if getattr(self, "_is_released", False):
            return

        self._is_released = True

        # 1. Check if we have something to free
        if self.pinner_id == 0:
            return

        # 2. Check if we have the tool to free it
        lib = getattr(self, "_lib", None)
        if lib is None:
            logger.critical(
                "Message (pinner=%d) cannot be released! "
                "Library dependency was not injected via bind_library().",
                self.pinner_id,
            )
            return

        # 3. Execute Safe Release
        try:
            self._lib.release(self.pinner_id)
            logger.debug("Invoked %s.release(%d)", self._lib, self.pinner_id)
        except ctypes.ArgumentError as e:
            logger.exception("Native release failed: %s", e)
            # We do not re-raise here to ensure __exit__ completes cleanly
        except Exception as e:
            logger.exception("Unexpected error during release: %s", e)
            # We do not re-raise here to ensure __exit__ completes cleanly

    def __del__(self, _warnings=warnings) -> None:
        """Finalizer: The Safety Net.

        Checks if the resource was leaked. If so, issues a ResourceWarning
        and attempts a last-ditch cleanup.
        """

        if getattr(self, "pinner_id", 0) != 0 and not getattr(
            self, "_is_released", False
        ):
            try:
                warnings.warn(
                    "Unclosed SpannerLib Message"
                    f"(pinner_id={self.pinner_id})",
                    ResourceWarning,
                )
            except Exception:
                pass

            try:
                self.release()
            except Exception:
                pass
