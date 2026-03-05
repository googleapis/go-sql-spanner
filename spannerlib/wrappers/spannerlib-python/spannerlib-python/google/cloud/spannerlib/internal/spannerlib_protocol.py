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
"""Protocol defining the expected interface for the Spanner library."""
from typing import Protocol, runtime_checkable

from .message import Message


@runtime_checkable
class SpannerLibProtocol(Protocol):
    """
    Protocol defining the expected interface for the Spanner library
    dependency.
    """

    def release(self, handle: int) -> int:
        """Calls the Release function from the shared library."""
        ...

    def create_pool(self, conn_str: str) -> "Message":
        """Calls the CreatePool function from the shared library."""
        ...

    def close_pool(self, pool_handle: int) -> "Message":
        """Calls the ClosePool function from the shared library."""
        ...

    def create_connection(self, pool_handle: int) -> Message:
        """Calls the CreateConnection function from the shared library."""
        ...

    def close_connection(self, pool_handle: int, conn_handle: int) -> Message:
        """Calls the CloseConnection function from the shared library."""
        ...

    def execute(
        self, pool_handle: int, conn_handle: int, request: bytes
    ) -> Message:
        """Calls the Execute function from the shared library."""
        ...

    def execute_batch(
        self, pool_handle: int, conn_handle: int, request: bytes
    ) -> Message:
        """Calls the ExecuteBatch function from the shared library."""
        ...

    def next(
        self,
        pool_handle: int,
        conn_handle: int,
        rows_handle: int,
        num_rows: int,
        encode_row_option: int,
    ) -> Message:
        """Calls the Next function from the shared library."""
        ...

    def close_rows(
        self, pool_handle: int, conn_handle: int, rows_handle: int
    ) -> Message:
        """Calls the CloseRows function from the shared library."""
        ...

    def next_result_set(
        self, pool_handle: int, conn_handle: int, rows_handle: int
    ) -> Message:
        """Calls the NextResultSet function from the shared library."""
        ...

    def metadata(
        self, pool_handle: int, conn_handle: int, rows_handle: int
    ) -> Message:
        """Calls the Metadata function from the shared library."""
        ...

    def result_set_stats(
        self, pool_handle: int, conn_handle: int, rows_handle: int
    ) -> Message:
        """Calls the ResultSetStats function from the shared library."""
        ...

    def begin_transaction(
        self, pool_handle: int, conn_handle: int, tx_opts: bytes
    ) -> Message:
        """Calls the BeginTransaction function from the shared library."""
        ...

    def commit(self, pool_handle: int, conn_handle: int) -> Message:
        """Calls the Commit function from the shared library."""
        ...

    def rollback(self, pool_handle: int, conn_handle: int) -> Message:
        """Calls the Rollback function from the shared library."""
        ...

    def write_mutations(
        self, pool_handle: int, conn_handle: int, request: bytes
    ) -> Message:
        """Calls the WriteMutations function from the shared library."""
        ...
