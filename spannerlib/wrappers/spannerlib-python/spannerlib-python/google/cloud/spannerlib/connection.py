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
"""Module for the Connection class
representing a single connection to Spanner."""
import logging
from typing import TYPE_CHECKING, Optional

from google.cloud.spanner_v1 import (
    BatchWriteRequest,
    CommitResponse,
    ExecuteBatchDmlRequest,
    ExecuteBatchDmlResponse,
    ExecuteSqlRequest,
    TransactionOptions,
)

from .abstract_library_object import AbstractLibraryObject
from .internal.errors import SpannerLibError
from .internal.types import to_bytes
from .rows import Rows

if TYPE_CHECKING:
    from .pool import Pool

logger = logging.getLogger(__name__)


class Connection(AbstractLibraryObject):
    """Represents a single connection to the Spanner database.

    This class wraps the connection handle from the underlying Go library,
    providing methods to manage the connection lifecycle.
    """

    def __init__(self, oid: int, pool: "Pool") -> None:
        """Initializes a Connection object.

        Args:
            oid (int): The object ID (handle) of the connection in the Go
                library.
            pool (Pool): The Pool object from which this connection was
                created.
        """
        super().__init__(pool.spannerlib, oid)
        self._pool = pool

    @property
    def pool(self) -> "Pool":
        """Returns the pool associated with this connection."""
        return self._pool

    def _close_lib_object(self) -> None:
        """Internal method to close the connection in the Go library."""
        try:
            logger.debug("Closing connection ID: %d", self.oid)
            # Call the Go library function to close the connection.
            with self.spannerlib.close_connection(
                self.pool.oid, self.oid
            ) as msg:
                msg.raise_if_error()
            logger.debug("Connection ID: %d closed", self.oid)
        except SpannerLibError:
            logger.exception(
                "SpannerLib error closing connection ID: %d", self.oid
            )
            raise
        except Exception as e:
            logger.exception(
                "Unexpected error closing connection ID: %d", self.oid
            )
            raise SpannerLibError(f"Unexpected error during close: {e}") from e

    def execute(self, request: ExecuteSqlRequest) -> Rows:
        """Executes a SQL statement on the connection.

        Args:
            request (ExecuteSqlRequest): The ExecuteSqlRequest object.

        Returns:
            A Rows object representing the result of the execution.
        """
        if self.closed:
            raise SpannerLibError("Connection is closed.")

        logger.debug(
            "Executing SQL on connection ID: %d for pool ID: %d",
            self.oid,
            self.pool.oid,
        )

        request_bytes = ExecuteSqlRequest.serialize(request)

        # Call the Go library function to execute the SQL statement.
        with self.spannerlib.execute(
            self.pool.oid, self.oid, request_bytes
        ) as msg:
            msg.raise_if_error()
            logger.debug(
                "SQL execution successful on connection ID: %d."
                "Got Rows ID: %d",
                self.oid,
                msg.object_id,
            )
            return Rows(msg.object_id, self)

    def execute_batch(
        self, request: ExecuteBatchDmlRequest
    ) -> ExecuteBatchDmlResponse:
        """Executes a batch of DML statements on the connection.

        Args:
            request: The ExecuteBatchDmlRequest object.

        Returns:
            An ExecuteBatchDmlResponse object representing the result
            of the execution.
        """
        if self.closed:
            raise SpannerLibError("Connection is closed.")

        logger.debug(
            "Executing batch DML on connection ID: %d for pool ID: %d",
            self.oid,
            self.pool.oid,
        )

        request_bytes = ExecuteBatchDmlRequest.serialize(request)

        # Call the Go library function to execute the batch DML statement.
        with self.spannerlib.execute_batch(
            self.pool.oid,
            self.oid,
            request_bytes,
        ) as msg:
            msg.raise_if_error()
            logger.debug(
                "Batch DML execution successful on connection ID: %d.",
                self.oid,
            )
            response_bytes = to_bytes(msg.msg, msg.msg_len)
            return ExecuteBatchDmlResponse.deserialize(response_bytes)

    def write_mutations(
        self, request: BatchWriteRequest.MutationGroup
    ) -> Optional[CommitResponse]:
        """Writes a mutation to the connection.

        Args:
            request: The BatchWriteRequest_MutationGroup object.

        Returns:
            A CommitResponse object if the mutation was applied immediately
            (no active transaction), or None if it was buffered.
        """
        if self.closed:
            raise SpannerLibError("Connection is closed.")

        logger.debug(
            "Writing mutation on connection ID: %d for pool ID: %d",
            self.oid,
            self.pool.oid,
        )

        request_bytes = BatchWriteRequest.MutationGroup.serialize(request)

        # Call the Go library function to write the mutation.
        with self.spannerlib.write_mutations(
            self.pool.oid,
            self.oid,
            request_bytes,
        ) as msg:
            msg.raise_if_error()
            logger.debug(
                "Mutation write successful on connection ID: %d.", self.oid
            )
            if msg.msg_len > 0 and msg.msg:
                response_bytes = to_bytes(msg.msg, msg.msg_len)
                return CommitResponse.deserialize(response_bytes)
            return None

    def begin_transaction(self, options: TransactionOptions = None):
        """Begins a new transaction on the connection.

        Args:
            options: Optional transaction options from google.cloud.spanner_v1.

        Raises:
            SpannerLibError: If the connection is closed.
            SpannerLibraryError: If the Go library call fails.
        """
        if self.closed:
            raise SpannerLibError("Connection is closed.")

        logger.debug(
            "Beginning transaction on connection ID: %d for pool ID: %d",
            self.oid,
            self.pool.oid,
        )

        if options is None:
            options = TransactionOptions()

        options_bytes = TransactionOptions.serialize(options)

        with self.spannerlib.begin_transaction(
            self.pool.oid, self.oid, options_bytes
        ) as msg:
            msg.raise_if_error()
            logger.debug("Transaction started on connection ID: %d", self.oid)

    def commit(self) -> CommitResponse:
        """Commits the transaction.

        Raises:
            SpannerLibError: If the connection is closed.
            SpannerLibraryError: If the Go library call fails.

        Returns:
            A CommitResponse object.
        """
        if self.closed:
            raise SpannerLibError("Connection is closed.")

        logger.debug("Committing on connection ID: %d", self.oid)
        with self.spannerlib.commit(self.pool.oid, self.oid) as msg:
            msg.raise_if_error()
            logger.debug("Committed")
            response_bytes = to_bytes(msg.msg, msg.msg_len)
            return CommitResponse.deserialize(response_bytes)

    def rollback(self):
        """Rolls back the transaction.

        Raises:
            SpannerLibError: If the connection is closed.
            SpannerLibraryError: If the Go library call fails.
        """
        if self.closed:
            raise SpannerLibError("Connection is closed.")

        logger.debug("Rolling back on connection ID: %d", self.oid)
        with self.spannerlib.rollback(self.pool.oid, self.oid) as msg:
            msg.raise_if_error()
            logger.debug("Rolled back")
