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
from .internal.errors import SpannerLibError

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
        self._pool = pool

    @property
    def pool(self) -> "Pool":
        """Returns the pool associated with this connection."""
        return self._pool

    def _close_lib_object(self) -> None:
        """Internal method to close the connection in the Go library."""
        try:
            logger.info("Closing connection ID: %d", self.oid)
            # Call the Go library function to close the connection.
            with self.spannerlib.close_connection(
                self.pool.oid, self.oid
            ) as msg:
                msg.raise_if_error()
            logger.info("Connection ID: %d closed", self.oid)
        except SpannerLibError:
            logger.exception(
                "SpannerLib error closing connection ID: %d", self.oid
            )
            raise
        except Exception as e:
            logger.exception(
                "Unexpected error closing connection ID: %d", self.oid
            )
            raise SpannerLibError(f"Unexpected error during close: {e}") from e
