import logging

from .connection import Connection
from .errors import SpannerPoolError
from .internal import _check_error, _lib, to_go_string

logger = logging.getLogger(__name__)


class Pool:
    """Manages a pool of connections to the Spanner database."""

    def __init__(self, connection_string: str):
        """
        Initializes the connection pool.

        Args:
            connection_string: The Spanner database connection string.
        """
        logger.info(f"Creating pool for connection string: {connection_string}")
        go_conn_str = to_go_string(connection_string)
        ret = _lib.CreatePool(go_conn_str)
        _check_error(ret, "CreatePool")
        self.pool_id = ret.r2
        self._closed = False
        logger.info(f"Pool created with ID: {self.pool_id}")

    def close(self):
        """Closes the connection pool and releases resources."""
        if not self._closed:
            logger.info(f"Closing pool ID: {self.pool_id}")
            ret = _lib.ClosePool(self.pool_id)
            _check_error(ret, "ClosePool")
            self._closed = True
            logger.info(f"Pool ID: {self.pool_id} closed")

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
        ret = _lib.CreateConnection(self.pool_id)
        _check_error(ret, "CreateConnection")
        logger.info(
            f"Connection created with ID: {ret.r2} from pool ID: {self.pool_id}"
        )
        return Connection(self, ret.r2)

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object."""
        self.close()

    def __del__(self):
        """Destructor to ensure the pool is closed."""
        if not self._closed:
            logger.warning(
                f"Pool ID: {self.pool_id} was not explicitly closed. Closing in destructor."
            )
            self.close()
