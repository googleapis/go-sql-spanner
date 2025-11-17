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
"""Module for the Pool class, representing a connection pool to Spanner."""
from __future__ import absolute_import

import logging

from google.cloud.spannerlib.abstract_library_object import (
    AbstractLibraryObject,
)
from google.cloud.spannerlib.internal.errors import SpannerLibError
from google.cloud.spannerlib.internal.message_handler import MessageHandler
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
            try:
                logger.info(f"Closing pool ID: {self.id}")
                # Call the Go library function to close the pool.
                msg = self.lib.ClosePool(self.id)
                err = MessageHandler.decode_error(msg, self.lib)
                if err:
                    raise err
                # Release the object in the Go library.
                self.release()
                logger.info(f"Pool ID: {self.id} closed")
            except SpannerLibError:
                logger.exception(f"SpannerLib error closing pool ID: {self.id}")
                raise
            except Exception as e:
                logger.exception(f"Unexpected error closing pool ID: {self.id}")
                raise SpannerLibError(
                    f"Unexpected error during close: {e}"
                ) from e

    @classmethod
    def create_pool(cls, connection_string: str) -> "Pool":
        """Creates a new connection pool.

        Args:
            connection_string (str): The connection string for the database.

        Returns:
            Pool: A new Pool object.
        """
        logger.info(
            f"Creating pool with connection string: {connection_string}"
        )
        try:
            lib = SpannerLib()
            # Call the Go library function to create a pool.
            msg = lib.CreatePool(to_go_string(connection_string))
            err_msg = MessageHandler.decode_error(msg, lib)
            if err_msg:
                raise SpannerLibError(err_msg)
            pool = cls(msg.object_id, lib)
            logger.info(f"Pool created with ID: {pool.id}")
        except SpannerLibError:
            logger.exception("Failed to create pool")
            raise
        except Exception as e:
            logger.exception("Unexpected error interacting with Go library")
            raise SpannerLibError(f"Unexpected error: {e}") from e
        return pool
