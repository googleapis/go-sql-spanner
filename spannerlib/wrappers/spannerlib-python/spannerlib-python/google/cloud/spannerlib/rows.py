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

import ctypes
import logging
from typing import TYPE_CHECKING, Optional

from google.cloud.spanner_v1 import ResultSetMetadata, ResultSetStats
from google.protobuf.struct_pb2 import ListValue

from .abstract_library_object import AbstractLibraryObject
from .internal.errors import SpannerLibError

if TYPE_CHECKING:
    from .connection import Connection
    from .pool import Pool

logger = logging.getLogger(__name__)


class Rows(AbstractLibraryObject):
    """Represents a result set from the Spanner database."""

    def __init__(self, oid: int, conn: "Connection") -> None:
        """Initializes a Rows object.

        Args:
            oid (int): The object ID (handle) of the row in the Go library.
            conn (Connection): The Connection object from which this row was
                created.
        """
        super().__init__(conn.pool.spannerlib, oid)
        self._conn = conn

    @property
    def pool(self) -> "Pool":
        """Returns the pool associated with this rows."""
        return self._conn.pool

    @property
    def conn(self) -> "Connection":
        """Returns the connection associated with this rows."""
        return self._conn

    def _close_lib_object(self) -> None:
        """Internal method to close the rows in the Go library."""
        try:
            logger.debug("Closing rows ID: %d", self.oid)
            # Call the Go library function to close the rows.
            with self.spannerlib.close_rows(
                self.pool.oid, self.conn.oid, self.oid
            ) as msg:
                msg.raise_if_error()
            logger.debug("Rows ID: %d closed", self.oid)
        except SpannerLibError:
            logger.exception("SpannerLib error closing rows ID: %d", self.oid)
            raise
        except Exception as e:
            logger.exception("Unexpected error closing rows ID: %d", self.oid)
            raise SpannerLibError(f"Unexpected error during close: {e}") from e

    def next(self) -> ListValue:
        """Fetches the next row(s) from the result set.

        Returns:
            A protobuf `ListValue` object representing the next row.
            The values within the row are also protobuf `Value` objects.
            Returns None if no more rows are available.

        Raises:
            SpannerLibError: If the Rows object is closed or if parsing fails.
            SpannerLibraryError: If the Go library call fails.
        """
        if self.closed:
            raise SpannerLibError("Rows object is closed.")

        logger.debug("Fetching next row for Rows ID: %d", self.oid)
        with self.spannerlib.next(
            self.pool.oid,
            self.conn.oid,
            self.oid,
            1,
            1,
        ) as msg:
            msg.raise_if_error()
            if msg.msg_len > 0 and msg.msg:
                try:
                    proto_bytes = ctypes.string_at(msg.msg, msg.msg_len)
                    next_row = ListValue()
                    next_row.ParseFromString(proto_bytes)
                    return next_row
                except Exception as e:
                    logger.error(
                        "Failed to decode/parse row data protobuf: %s", e
                    )
                    raise SpannerLibError(f"Failed to get next row(s): {e}")
            else:
                # Assuming no message means no more rows
                logger.debug("No more rows...")
                return None

    def next_result_set(self) -> Optional[ResultSetMetadata]:
        """Advances to the next result set.

        Returns:
            ResultSetMetadata if there is a next result set, None otherwise.

        Raises:
            SpannerLibError: If the Rows object is closed.
            SpannerLibraryError: If the Go library call fails.
        """
        if self.closed:
            raise SpannerLibError("Rows object is closed.")

        logger.debug("Advancing to next result set for Rows ID: %d", self.oid)
        with self.spannerlib.next_result_set(
            self.pool.oid, self.conn.oid, self.oid
        ) as msg:
            msg.raise_if_error()
            if msg.msg_len > 0 and msg.msg:
                try:
                    proto_bytes = ctypes.string_at(msg.msg, msg.msg_len)
                    return ResultSetMetadata.deserialize(proto_bytes)
                except Exception as e:
                    logger.error(
                        "Failed to decode/parse next result set metadata: %s", e
                    )
                    raise SpannerLibError(
                        f"Failed to parse next result set metadata: {e}"
                    )
            return None

    def metadata(self) -> ResultSetMetadata:
        """Retrieves the metadata for the result set.

        Returns:
            ResultSetMetadata object containing the metadata.
        """
        if self.closed:
            raise SpannerLibError("Rows object is closed.")

        logger.debug("Getting metadata for Rows ID: %d", self.oid)
        with self.spannerlib.metadata(
            self.pool.oid, self.conn.oid, self.oid
        ) as msg:
            msg.raise_if_error()
            if msg.msg_len > 0 and msg.msg:
                try:
                    proto_bytes = ctypes.string_at(msg.msg, msg.msg_len)
                    return ResultSetMetadata.deserialize(proto_bytes)
                except Exception as e:
                    logger.error(
                        "Failed to decode/parse metadata protobuf: %s", e
                    )
                    raise SpannerLibError(f"Failed to get metadata: {e}")
        return ResultSetMetadata()

    def result_set_stats(self) -> ResultSetStats:
        """Retrieves the result set statistics.

        Returns:
            ResultSetStats object containing the statistics.
        """
        if self.closed:
            raise SpannerLibError("Rows object is closed.")

        logger.debug("Getting ResultSetStats for Rows ID: %d", self.oid)
        with self.spannerlib.result_set_stats(
            self.pool.oid, self.conn.oid, self.oid
        ) as msg:
            msg.raise_if_error()
            if msg.msg_len > 0 and msg.msg:
                try:
                    proto_bytes = ctypes.string_at(msg.msg, msg.msg_len)
                    return ResultSetStats.deserialize(proto_bytes)
                except Exception as e:
                    logger.error(
                        "Failed to decode/parse ResultSetStats protobuf: %s", e
                    )
                    raise SpannerLibError(f"Failed to get ResultSetStats: {e}")
        return ResultSetStats()

    def update_count(self) -> int:
        """Retrieves the update count.

        Returns:
            int representing the update count.
        """
        stats = self.result_set_stats()

        if stats._pb.WhichOneof("row_count") == "row_count_exact":
            return stats.row_count_exact
        if stats._pb.WhichOneof("row_count") == "row_count_lower_bound":
            return stats.row_count_lower_bound

        return -1
