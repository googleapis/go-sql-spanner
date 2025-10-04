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

import logging

from .internal.spannerlib import Spannerlib

logger = logging.getLogger(__name__)


class AbstractLibraryObject:
    """Abstract base class for objects that are managed by the Go library.

    This class provides a common interface for releasing resources in the Go library.
    """

    def __init__(self, id):
        """Initializes the AbstractLibraryObject.

        Args:
            id: The pinner ID for this object in the Go library.
        """
        self._id = id

    @property
    def id(self):
        """Returns the pinner ID for this object in the Go library."""
        return self._id

    def release(self):
        """Releases the object in the Go library.

        This method calls the Release function in the Go library to free the resources
        associated with this object.
        """
        try:
            lib = Spannerlib.get_instance().lib
            if lib:
                lib.Release(self._id)
                logger.debug(f"Released pinnerId {self._id}")
        except Exception as e:
            logger.warning(f"Error releasing pinnerId {self._id}: {e}")
