import logging

from .internal.spannerlib import _check_error, _lib

logger = logging.getLogger(__name__)


class Connection:
    """Represents a single connection to the Spanner database."""

    def __init__(self, pool, conn_id: int):
        """
        Initializes a Connection.

        Args:
            pool: The parent Pool object.
            conn_id: The connection ID from the Go library.
        """
        self.pool = pool
        self.conn_id = conn_id
        self._closed = False
        logger.debug(
            f"Connection ID: {self.conn_id} initialized for pool ID: {self.pool.pool_id}"
        )

    def close(self):
        """Closes the connection and releases resources."""
        if not self._closed:
            if self.pool._closed:
                logger.debug(
                    f"Connection ID: {self.conn_id} implicitly closed because pool is closed."
                )
                self._closed = True
                return

            logger.info(
                f"Closing connection ID: {self.conn_id} for pool ID: {self.pool.pool_id}"
            )
            ret = _lib.CloseConnection(self.pool.pool_id, self.conn_id)
            _check_error(ret, "CloseConnection")
            self._closed = True
            logger.info(f"Connection ID: {self.conn_id} closed")

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object."""
        self.close()

    def __del__(self):
        """Destructor to ensure the connection is closed."""
        if not self._closed:
            logger.warning(
                f"Connection ID: {self.conn_id} was not explicitly closed. Closing in destructor."
            )
            self.close()
