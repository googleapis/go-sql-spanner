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

"""Module for the Rows class, representing the result of execute statement."""

import ctypes
import logging
from typing import Any

from google.cloud.spanner_v1 import ResultSetMetadata
from google.protobuf.struct_pb2 import ListValue

from google.cloud.spannerlib.internal.spannerlib import check_error, get_lib
from google.cloud.spannerlib.library_object import AbstractLibraryObject

logger = logging.getLogger(__name__)


class Rows(AbstractLibraryObject):
    """Represents the result of an executed SQL statement."""

    def __init__(self, id: int, pool: Any, conn: Any):
        super().__init__(id)
        self._pool = pool
        self._conn = conn

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object, ensuring the rows are closed."""
        self.close()

    @property
    def pool(self):
        """Returns the parent Pool object."""
        return self._pool

    @property
    def conn(self):
        """Returns the parent Connection object."""
        return self._conn

    def close(self):
        """Closes the rows and releases resources in the Go library.

        If the Rows object is already closed, this method does nothing.
        It also checks if the parent pool or conn is closed.
        """
        if not self.closed:
            if self.pool.closed:
                logger.debug(
                    f"Rows ID: {self.id} implicitly closed because pool is closed."
                )
                self.closed = True
                return
            if self.conn.closed:
                logger.debug(
                    f"Rows ID: {self.id} implicitly closed because connection is closed."
                )
                self.closed = True
                return

            logger.info(
                f"Closing rows ID: {self.id} for pool ID: {self.pool.id} and connection ID: {self.conn.id}"
            )
            # Call the Go library function to close the connection.
            ret = get_lib().CloseRows(self.pool.id, self.conn.id, self.id)
            check_error(ret, "CloseRows")
            self.closed = True
            logger.info(f"Rows ID: {self.id} closed")
            # Release the pinner ID in the Go library.
            self._release()

    def metadata(self) -> ResultSetMetadata:
        """Retrieves the metadata for the result set.

        Returns:
            ResultSetMetadata object containing the metadata.
        """
        if self.closed:
            raise RuntimeError("Rows object is closed.")

        logger.debug(f"Getting metadata for Rows ID: {self.id}")
        ret = get_lib().Metadata(self.pool.id, self.conn.id, self.id)
        check_error(ret, "Metadata")

        if ret.msg_len > 0:
            try:
                proto_bytes = ctypes.string_at(ret.msg, ret.msg_len)
                return ResultSetMetadata.deserialize(proto_bytes)
            except Exception as e:
                logger.error(f"Failed to decode/parse metadata JSON: {e}")
                raise RuntimeError(f"Failed to get metadata: {e}")
        return ResultSetMetadata()

    def next(self) -> ListValue:
        """Fetches the next row(s) from the result set.

        Returns:
            The fetched row(s), likely as a list of lists or list of dicts,
            depending on the JSON structure returned by the Go layer.
            Returns None if no more rows are available.

        Raises:
            RuntimeError: If the Rows object is closed or if parsing fails.
            SpannerLibraryError: If the Go library call fails.
        """
        if self.closed:
            raise RuntimeError("Rows object is closed.")

        logger.debug(f"Fetching next row for Rows ID: {self.id}")
        ret = get_lib().Next(
            self.pool.id,
            self.conn.id,
            self.id,
            1,
            1,
        )
        check_error(ret, "Next")

        if ret.msg_len > 0 and ret.msg:
            try:
                proto_bytes = ctypes.string_at(ret.msg, ret.msg_len)
                next_row = ListValue()
                next_row.ParseFromString(proto_bytes)
                return next_row
            except Exception as e:
                logger.error(f"Failed to decode/parse row data JSON: {e}")
                raise RuntimeError(f"Failed to get next row(s): {e}")
        else:
            # Assuming no message means no more rows
            logger.debug("No more rows...")
            return None
