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
"""Module for the Connection class
representing a single connection to Spanner."""
import logging
from typing import TYPE_CHECKING

from .abstract_library_object import AbstractLibraryObject

if TYPE_CHECKING:
    from .pool import Pool

logger = logging.getLogger(__name__)


class Connection(AbstractLibraryObject):
    """Represents a single connection to the Spanner database.

    This class wraps the connection handle from the underlying Go library,
    providing methods to manage the connection lifecycle.
    """

    def __init__(self, oid: int, pool: "Pool") -> None:
        """Initializes a Connection object.

        Args:
            oid (int): The object ID (handle) of the connection in the Go
                library.
            pool (Pool): The Pool object from which this connection was
                created.
        """
        super().__init__(pool.spannerlib, oid)
        self._pool_id = pool.oid

    @property
    def pool_id(self) -> int:
        """Returns the pool ID associated with this connection."""
        return self._pool_id

    def _close_lib_object(self) -> None:
        """Internal method to close the pool in the Go library."""
        try:
            logger.info("Closing connection ID: %d", self.oid)
            # Call the Go library function to close the connection.
            with self.spannerlib.close_connection(
                self.pool_id, self.oid
            ) as msg:
                msg.bind_library(self.spannerlib)
                msg.raise_if_error()
            logger.info("Connection ID: %d closed", self.oid)
        except Exception as e:
            logger.exception("Error closing connection ID: %d", self.oid)
            raise e
