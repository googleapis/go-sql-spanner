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

import logging

from .connection import Connection
from .errors import SpannerPoolError
from .internal.spannerlib import check_error, get_lib
from .internal.types import to_go_string
from .library_object import AbstractLibraryObject

logger = logging.getLogger(__name__)


class Pool(AbstractLibraryObject):
    """Manages a pool of connections to the Spanner database."""

    def __init__(self, id):
        """
        Initializes the connection pool.

        """
        super().__init__(id)
        self._pool_id = -1
        self._closed = True

    @property
    def pool_id(self):
        return self._pool_id

    @pool_id.setter
    def pool_id(self, value):
        self._pool_id = value

    @property
    def closed(self):
        return self._closed

    @closed.setter
    def closed(self, value):
        self._closed = value

    def close(self):
        """Closes the connection pool and releases resources."""
        if not self.closed:
            logger.info(f"Closing pool ID: {self.pool_id}")
            ret = get_lib().ClosePool(self.pool_id)
            check_error(ret, "ClosePool")
            self.closed = True
            logger.info(f"Pool ID: {self.pool_id} closed")
            self.release()

    def create_connection(self):
        """
        Creates a new connection from the pool.

        Returns:
            Connection: A new Connection object.

        Raises:
            SpannerPoolError: If the pool is closed.
        """
        if self._closed:
            logger.error("Attempted to create connection from a closed pool")
            raise SpannerPoolError("Pool is closed")
        logger.debug(f"Creating connection from pool ID: {self.pool_id}")
        ret = get_lib().CreateConnection(self.pool_id)
        check_error(ret, "CreateConnection")
        logger.info(
            f"Connection created with ID: {ret.object_id} from pool ID: {self.pool_id}"
        )
        return Connection(self, ret.pinner_id, ret.object_id)

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object."""
        self.close()

    def __del__(self):
        """Destructor to ensure the pool is closed."""
        if not self.closed:
            logger.warning(
                f"Pool ID: {self.pool_id} was not explicitly closed. Closing in destructor."
            )
            self.close()

    @classmethod
    def create_pool(cls, connection_string: str):
        """
        Creates a new connection pool.

        Args:
            connection_string (str): The connection string for the database.

        Returns:
            Pool: A new Pool object.
        """
        print(f"Creating pool with connection string: {connection_string}")
        ret = get_lib().CreatePool(to_go_string(connection_string))
        check_error(ret, "CreatePool")
        pool = cls(ret.pinner_id)
        pool.pool_id = ret.object_id
        pool.closed = False
        logger.info(f"Pool created with ID: {pool.pool_id}")
        return pool
