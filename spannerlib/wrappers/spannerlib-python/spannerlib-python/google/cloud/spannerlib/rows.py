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

import logging
from typing import TYPE_CHECKING

from .abstract_library_object import AbstractLibraryObject
from .internal.errors import SpannerLibError

if TYPE_CHECKING:
    from .connection import Connection
    from .pool import Pool

logger = logging.getLogger(__name__)


class Rows(AbstractLibraryObject):
    """Represents a result set from the Spanner database."""

    def __init__(self, oid: int, pool: "Pool", conn: "Connection") -> None:
        """Initializes a Rows object.

        Args:
            oid (int): The object ID (handle) of the row in the Go library.
            pool (Pool): The Pool object from which this row was created.
        """
        super().__init__(pool.spannerlib, oid)
        self._pool = pool
        self._conn = conn

    @property
    def pool(self) -> "Pool":
        """Returns the pool associated with this rows."""
        return self._pool

    @property
    def conn(self) -> "Connection":
        """Returns the connection associated with this rows."""
        return self._conn

    def _close_lib_object(self) -> None:
        """Internal method to close the rows in the Go library."""
        try:
            logger.info("Closing rows ID: %d", self.oid)
            # Call the Go library function to close the rows.
            with self.spannerlib.close_rows(
                self.pool.oid, self.conn.oid, self.oid
            ) as msg:
                msg.raise_if_error()
            logger.info("Rows ID: %d closed", self.oid)
        except Exception as e:
            logger.exception("Unexpected error closing rows ID: %d", self.oid)
            raise SpannerLibError(f"Unexpected error during close: {e}") from e
