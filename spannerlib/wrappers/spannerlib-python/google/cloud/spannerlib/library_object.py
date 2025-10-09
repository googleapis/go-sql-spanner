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

"""Module for the AbstractLibraryObject class."""
from __future__ import absolute_import

import logging

from google.cloud.spannerlib.internal.spannerlib import Spannerlib

logger = logging.getLogger(__name__)


class AbstractLibraryObject:
    """Abstract base class for objects that are managed by the Go library.

    This class provides a common interface for releasing resources in the Go library.
    """

    def __init__(self, id):
        """Initializes the AbstractLibraryObject.

        Args:
            id: The ID for this library object in the Go library.
        """
        self._id = id
        self._closed = False

    @property
    def id(self):
        """Returns the ID for this library object in the Go library."""
        return self._id

    @id.setter
    def id(self, value):
        """Sets the ID."""
        self._id = value

    @property
    def closed(self):
        """Returns True if the library object is closed, False otherwise."""
        return self._closed

    @closed.setter
    def closed(self, value):
        """Sets the closed state of the library object."""
        self._closed = value

    def _release(self):
        """Releases the object in the Go library.

        This method calls the Release function in the Go library to free the resources
        associated with this object.
        """
        if self._id == 0:
            return
        try:
            lib = Spannerlib.get_instance().lib
            if lib:
                lib.Release(self._id)
                logger.debug(f"Released {self._id}")
                self._closed = True
        except Exception as e:
            logger.warning(f"Error releasing {self._id}: {e}")
