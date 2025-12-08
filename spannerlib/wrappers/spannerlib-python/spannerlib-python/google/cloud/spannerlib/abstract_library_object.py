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
"""Abstract base class for SpannerLib objects."""

from abc import ABC, abstractmethod
from typing import Optional
import warnings

from .internal.spannerlib_protocol import SpannerLibProtocol


class ObjectClosedError(RuntimeError):
    """Raised when an operation is attempted on a closed/disposed object."""


class AbstractLibraryObject(ABC):
    """
    Base class for all objects created by SpannerLib.

    Implements the Context Manager protocol (for 'with' statements)
    to handle automatic resource cleanup.
    """

    def __init__(self, spannerlib: SpannerLibProtocol, oid: int) -> None:
        """
        Initializes the AbstractLibraryObject.

        Args:
            spannerlib: The Spanner library instance.
            oid: The unique identifier for this object.
        """
        self._spannerlib: SpannerLibProtocol = spannerlib
        self._oid: int = oid
        self._is_disposed: bool = False

    @property
    def spannerlib(self) -> SpannerLibProtocol:
        """Returns the associated Spanner library instance."""
        return self._spannerlib

    @property
    def oid(self) -> int:
        """Returns the object ID."""
        return self._oid

    @property
    def closed(self) -> bool:
        """Returns True if the object is closed/disposed."""
        return self._is_disposed

    def _check_disposed(self) -> None:
        """
        Checks if the object has been disposed.

        Raises:
            ObjectClosedError: If the object has already been closed/disposed.
        """
        if self._is_disposed:
            raise ObjectClosedError(
                f"{self.__class__.__name__} has already been disposed."
            )

    def _mark_disposed(self) -> None:
        """Marks the object as disposed."""
        self._is_disposed = True

    # -------------------------------------------------------------------------
    # Synchronous Disposal (Context Manager)
    # -------------------------------------------------------------------------
    def close(self) -> None:
        """
        Closes the object and releases resources.
        """
        self._dispose()

    def __enter__(self) -> "AbstractLibraryObject":
        """Enters the runtime context related to this object."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[object],
    ) -> None:
        """Exits the runtime context and closes the object."""
        self.close()

    def _dispose(self) -> None:
        """
        Internal disposal logic.
        """
        if self._is_disposed:
            return

        try:
            if self._oid > 0:
                self._close_lib_object()
        finally:
            self._is_disposed = True

    # -------------------------------------------------------------------------
    # Abstract Methods
    # -------------------------------------------------------------------------
    @abstractmethod
    def _close_lib_object(self) -> None:
        """
        Closes the underlying library object.

        Must be implemented by concrete subclasses to call the corresponding
        Close function in SpannerLib.
        """
        pass

    # -------------------------------------------------------------------------
    # Finalizer
    # -------------------------------------------------------------------------
    def __del__(self) -> None:
        """
        Finalizer that attempts to clean up resources if not explicitly closed.
        """
        if not self._is_disposed:
            warnings.warn(
                f"Unclosed {self.__class__.__name__} (ID: {self._oid}). "
                "Use 'with' or 'async with' to manage resources.",
                ResourceWarning,
                stacklevel=2,
            )
            self._dispose()
