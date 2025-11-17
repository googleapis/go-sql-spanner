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

from abc import ABC, abstractmethod
import logging

from google.cloud.spannerlib.internal.spannerlib import SpannerLib

logger = logging.getLogger(__name__)


class AbstractLibraryObject(ABC):
    """Abstract base class for objects that are managed by the Go library.

    This class provides a common interface for releasing resources in the Go library.
    """

    def __init__(self, id: int, lib: SpannerLib):
        """Initializes the AbstractLibraryObject.

        Args:
            id (int): The ID for this library object in the Go library.
            lib (SpannerLib): The SpannerLib instance.
        """
        self._id = id
        self._lib = lib
        self._closed = False

    def __del__(self):
        if not self._closed:
            logger.warning(
                f"Resource {self.__class__.__name__} with ID {self._id} was not explicitly closed. "
                "This may lead to resource leaks in the Go runtime."
            )

    def __enter__(self):
        return self

    def __exit__(self, etype, value, traceback):
        self.close()

    @abstractmethod
    def close(self):
        """An abstract method that must be implemented by subclasses."""
        pass

    @property
    def id(self) -> int:
        """int: The ID for this library object in the Go library."""
        return self._id

    @property
    def lib(self) -> SpannerLib:
        """SpannerLib: The SpannerLib instance."""
        return self._lib

    @property
    def closed(self) -> bool:
        """bool: True if the library object is closed, False otherwise."""
        return self._closed

    def release(self) -> None:
        """Releases the object in the Go library.

        This method calls the Release function in the Go library to free the resources
        associated with this object.
        """
        if self._id == 0:
            return
        try:
            if self.lib:
                self.lib.Release(self._id)
                logger.debug(f"Released {self._id}")
                self._closed = True
        except Exception as e:
            logger.warning(f"Error releasing {self._id}: {e}")
