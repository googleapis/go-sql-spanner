import logging

from .internal.spannerlib import check_error, get_lib
from .library_object import AbstractLibraryObject

logger = logging.getLogger(__name__)


class Connection(AbstractLibraryObject):
    """Represents a single connection to the Spanner database."""

    def __init__(self, pool, id, conn_id: int):
        """
        Initializes a Connection.

        Args:
            pool: The parent Pool object.
            conn_id: The connection ID from the Go library.
        """
        super().__init__(id)
        self._pool = pool
        self._conn_id = conn_id
        self._closed = False
        logger.debug(
            f"Connection ID: {self.conn_id} initialized for pool ID: {self.pool.pool_id}"
        )

    @property
    def pool(self):
        return self._pool

    @property
    def conn_id(self):
        return self._conn_id

    @conn_id.setter
    def conn_id(self, value):
        self._conn_id = value

    @property
    def closed(self):
        return self._closed

    @closed.setter
    def closed(self, value):
        self._closed = value

    def close(self):
        """Closes the connection and releases resources."""
        if not self.closed:
            if self.pool.closed:
                logger.debug(
                    f"Connection ID: {self.conn_id} implicitly closed because pool is closed."
                )
                self.closed = True
                return

            logger.info(
                f"Closing connection ID: {self.conn_id} for pool ID: {self.pool.pool_id}"
            )
            ret = get_lib().CloseConnection(self.pool.pool_id, self.conn_id)
            check_error(ret, "CloseConnection")
            self.closed = True
            logger.info(f"Connection ID: {self.conn_id} closed")
            self.release()

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object."""
        self.close()

    def __del__(self):
        """Destructor to ensure the connection is closed."""
        if not self.closed:
            logger.warning(
                f"Connection ID: {self.conn_id} was not explicitly closed. Closing in destructor."
            )
            self.close()
