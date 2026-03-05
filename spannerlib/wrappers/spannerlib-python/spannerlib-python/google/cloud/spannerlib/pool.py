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
"""Module for managing Spanner connection pools."""
import logging

from .abstract_library_object import AbstractLibraryObject
from .connection import Connection
from .internal.errors import SpannerLibError
from .internal.spannerlib import SpannerLib

logger = logging.getLogger(__name__)


class Pool(AbstractLibraryObject):
    """Manages a pool of connections to the Spanner database.

    This class wraps the connection pool handle from the underlying Go library,
    providing methods to create connections and manage the pool lifecycle.
    """

    def _close_lib_object(self) -> None:
        """Internal method to close the pool in the Go library."""
        try:
            logger.debug("Closing pool ID: %d", self.oid)
            # Call the Go library function to close the pool.
            with self.spannerlib.close_pool(self.oid) as msg:
                msg.raise_if_error()
            logger.debug("Pool ID: %d closed", self.oid)
        except SpannerLibError:
            logger.exception("SpannerLib error closing pool ID: %d", self.oid)
            raise
        except Exception as e:
            logger.exception("Unexpected error closing pool ID: %d", self.oid)
            raise SpannerLibError(f"Unexpected error during close: {e}") from e

    @classmethod
    def create_pool(cls, connection_string: str) -> "Pool":
        """Creates a new connection pool.

        Args:
            connection_string (str): The connection string for the database.

        Returns:
            Pool: A new Pool object.
        """
        logger.debug(
            "Creating pool with connection string: %s",
            connection_string,
        )
        try:
            lib = SpannerLib()
            # Call the Go library function to create a pool.
            with lib.create_pool(connection_string) as msg:
                msg.raise_if_error()
                pool = cls(lib, msg.object_id)
                logger.debug("Pool created with ID: %d", pool.oid)
        except SpannerLibError:
            logger.exception("Failed to create pool")
            raise
        except Exception as e:
            logger.exception("Unexpected error interacting with Go library")
            raise SpannerLibError(f"Unexpected error: {e}") from e
        return pool

    def create_connection(self) -> Connection:
        """
        Creates a new connection from the pool.

        Returns:
            Connection: A new Connection object.

        Raises:
            SpannerLibError: If the pool is closed.
        """
        if self.closed:
            logger.error("Attempted to create connection from a closed pool")
            raise SpannerLibError("Pool is closed")
        logger.debug("Creating connection from pool ID: %d", self.oid)
        # Call the Go library function to create a connection.
        with self.spannerlib.create_connection(self.oid) as msg:
            msg.raise_if_error()

            logger.debug(
                "Connection created with ID: %d from pool ID: %d",
                msg.object_id,
                self.oid,
            )
            return Connection(msg.object_id, self)
