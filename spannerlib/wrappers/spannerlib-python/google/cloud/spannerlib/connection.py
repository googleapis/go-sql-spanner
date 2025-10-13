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

"""Module for the Connection class, representing a single connection to Spanner."""

from __future__ import absolute_import

import logging
from typing import Any

from google.cloud.spanner_v1 import (
    BatchWriteRequest,
    CommitResponse,
    ExecuteBatchDmlRequest,
    ExecuteBatchDmlResponse,
    ExecuteSqlRequest,
    TransactionOptions,
)

from google.cloud.spannerlib.internal.spannerlib import check_error, get_lib
from google.cloud.spannerlib.internal.types import (
    serialized_bytes_to_go_slice,
    to_bytes,
)
from google.cloud.spannerlib.library_object import AbstractLibraryObject
from google.cloud.spannerlib.rows import Rows

logger = logging.getLogger(__name__)


class Connection(AbstractLibraryObject):
    """Represents a single connection to the Spanner database.

    This class wraps the connection handle from the underlying Go library,
    providing methods to manage the connection lifecycle.
    """

    def __init__(self, id: int, pool: Any):
        """
        Initializes a Connection.

        Args:
            pool: The parent Pool object.
            id: The pinner ID for this object in the Go library.
            id: The connection ID from the Go library.
        """
        super().__init__(id)
        self._pool = pool
        self._closed = False
        logger.debug(
            f"Connection ID: {self.id} initialized for pool ID: {self.pool.id}"
        )

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object, ensuring the connection is closed."""
        self.close()

    @property
    def pool(self):
        """Returns the parent Pool object."""
        return self._pool

    def close(self):
        """Closes the connection and releases resources in the Go library.

        If the connection is already closed, this method does nothing.
        It also checks if the parent pool is closed.
        """
        if not self.closed:
            if self.pool.closed:
                logger.debug(
                    f"Connection ID: {self.id} implicitly closed because pool is closed."
                )
                self.closed = True
                return

            logger.info(
                f"Closing connection ID: {self.id} for pool ID: {self.pool.id}"
            )
            # Call the Go library function to close the connection.
            ret = get_lib().CloseConnection(self.pool.id, self.id)
            check_error(ret, "CloseConnection")
            # Release the pinner ID in the Go library.
            self._release()
            logger.info(f"Connection ID: {self.id} closed")

    def execute(self, request: ExecuteSqlRequest) -> Rows:
        """Executes a SQL statement on the connection.

        Args:
            sql: The SQL statement to execute.

        Returns:
            A Rows object representing the result of the execution.
        """
        if self.closed:
            raise RuntimeError("Connection is closed.")

        logger.info(
            f"Executing SQL on connection ID: {self.id} for pool ID: {self.pool.id}"
        )

        request_slice = serialized_bytes_to_go_slice(
            ExecuteSqlRequest.serialize(request)
        )

        # Call the Go library function to execute the SQL statement.
        ret = get_lib().Execute(
            self.pool.id,
            self.id,
            request_slice,
        )
        check_error(ret, "Execute")
        logger.info(
            f"SQL execution successful on connection ID: {self.id}. Got Rows ID: {ret.object_id}"
        )
        return Rows(ret.object_id, self.pool, self)

    def begin_transaction(self, options: TransactionOptions = None):
        """Begins a new transaction on the connection.

        Args:
            options: Optional transaction options from google.cloud.spanner_v1.

        Raises:
            RuntimeError: If the connection is closed.
            SpannerLibraryError: If the Go library call fails.
        """
        if self.closed:
            raise RuntimeError("Connection is closed.")

        logger.info(
            f"Beginning transaction on connection ID: {self.id} for pool ID: {self.pool.id}"
        )

        if options is None:
            options = TransactionOptions()

        options_slice = serialized_bytes_to_go_slice(
            TransactionOptions.serialize(options)
        )

        ret = get_lib().BeginTransaction(self.pool.id, self.id, options_slice)
        check_error(ret, "BeginTransaction")

        logger.info(f"Transaction started on connection ID: {self.id}")

    def commit(self) -> CommitResponse:
        """Commits the transaction.

        Raises:
            RuntimeError: If the connection is closed.
            SpannerLibraryError: If the Go library call fails.

        Returns:
            A CommitResponse object.
        """
        if self.closed:
            raise RuntimeError("Connection is closed.")

        logger.info(f"Committing on connection ID: {self.id}")
        ret = get_lib().Commit(self.pool.id, self.id)
        check_error(ret, "Commit")
        logger.info("Committed")
        response_bytes = to_bytes(ret.msg, ret.msg_len)
        return CommitResponse.deserialize(response_bytes)

    def rollback(self):
        """Rolls back the transaction.

        Raises:
            RuntimeError: If the connection is closed.
            SpannerLibraryError: If the Go library call fails.
        """
        if self.closed:
            raise RuntimeError("Connection is closed.")

        logger.info(f"Rolling back on connection ID: {self.id}")
        ret = get_lib().Rollback(self.pool.id, self.id)
        check_error(ret, "Rollback")
        logger.info("Rolled back")

    def execute_batch(
        self, request: ExecuteBatchDmlRequest
    ) -> ExecuteBatchDmlResponse:
        """Executes a batch of DML statements on the connection.

        Args:
            request: The ExecuteBatchDmlRequest object.

        Returns:
            An ExecuteBatchDmlResponse object representing the result of the execution.
        """
        if self.closed:
            raise RuntimeError("Connection is closed.")

        logger.info(
            f"Executing batch DML on connection ID: {self.id} for pool ID: {self.pool.id}"
        )

        request_slice = serialized_bytes_to_go_slice(
            ExecuteBatchDmlRequest.serialize(request)
        )

        # Call the Go library function to execute the batch DML statement.
        ret = get_lib().ExecuteBatch(
            self.pool.id,
            self.id,
            request_slice,
        )
        check_error(ret, "ExecuteBatch")
        logger.info(
            f"Batch DML execution successful on connection ID: {self.id}."
        )
        response_bytes = to_bytes(ret.msg, ret.msg_len)
        return ExecuteBatchDmlResponse.deserialize(response_bytes)

    def write_mutations(
        self, request: BatchWriteRequest.MutationGroup
    ) -> CommitResponse:
        """Writes a mutation to the connection.

        Args:
            request: The BatchWriteRequest_MutationGroup object.

        Returns:
            A CommitResponse object.
        """
        if self.closed:
            raise RuntimeError("Connection is closed.")

        logger.info(
            f"Writing mutation on connection ID: {self.id} for pool ID: {self.pool.id}"
        )

        request_slice = serialized_bytes_to_go_slice(
            BatchWriteRequest.MutationGroup.serialize(request)
        )

        # Call the Go library function to write the mutation.
        ret = get_lib().WriteMutations(
            self.pool.id,
            self.id,
            request_slice,
        )
        check_error(ret, "WriteMutations")
        logger.info(f"Mutation write successful on connection ID: {self.id}.")
        response_bytes = to_bytes(ret.msg, ret.msg_len)
        return CommitResponse.deserialize(response_bytes)
