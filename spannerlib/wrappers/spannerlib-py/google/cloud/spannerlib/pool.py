import logging

from .connection import Connection
from .errors import SpannerPoolError
from .internal.spannerlib import _check_error, get_lib
from .internal.types import GoString

logger = logging.getLogger(__name__)


class Pool:
    """Manages a pool of connections to the Spanner database."""
    _closed = True

    def __init__(self, connection_string: str):
        """
        Initializes the connection pool.

        Args:
            connection_string: The Spanner database connection string.
        """
        logger.info(f"Creating pool for connection string: {connection_string}")
        go_conn_str = GoString(connection_string.encode("utf-8"))
        ret = get_lib().CreatePool(go_conn_str)
        _check_error(ret, "CreatePool")
        self.pool_id = ret.object_id
        self._closed = False
        logger.info(f"Pool created with ID: {self.pool_id}")

    def close(self):
        """Closes the connection pool and releases resources."""
        if not self._closed:
            logger.info(f"Closing pool ID: {self.pool_id}")
            ret = get_lib().ClosePool(self.pool_id)
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
        ret = get_lib().CreateConnection(self.pool_id)
        _check_error(ret, "CreateConnection")
        logger.info(
            f"Connection created with ID: {ret.object_id} from pool ID: {self.pool_id}"
        )
        return Connection(self, ret.object_id)

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
