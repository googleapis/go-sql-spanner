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

"""Module for the Pool class, representing a connection pool to Spanner."""
from __future__ import absolute_import

import logging

from google.cloud.spannerlib.abstract_library_object import (
    AbstractLibraryObject,
)
from google.cloud.spannerlib.internal.spannerlib import SpannerLib
from google.cloud.spannerlib.internal.types import to_go_string

logger = logging.getLogger(__name__)


class Pool(AbstractLibraryObject):
    """Manages a pool of connections to the Spanner database.

    This class wraps the connection pool handle from the underlying Go library,
    providing methods to create connections and manage the pool lifecycle.
    """

    def close(self) -> None:
        """Closes the connection pool and releases resources in the Go library.

        If the pool is already closed, this method does nothing.
        """
        if not self.closed:
            logger.info(f"Closing pool ID: {self.id}")
            # Call the Go library function to close the pool.
            _ = self.lib.ClosePool(self.id)
            # Release the object in the Go library.
            self.release()
            logger.info(f"Pool ID: {self.id} closed")

    @classmethod
    def create_pool(cls, connection_string: str) -> "Pool":
        """
        Creates a new connection pool.

        Args:
            connection_string (str): The connection string for the database.

        Returns:
            Pool: A new Pool object.
        """
        logger.info(
            f"Creating pool with connection string: {connection_string}"
        )
        # Call the Go library function to create a pool.
        lib = SpannerLib()
        msg = lib.CreatePool(to_go_string(connection_string))
        pool = cls(msg.object_id, lib)
        logger.info(f"Pool created with ID: {pool.id}")
        return pool
