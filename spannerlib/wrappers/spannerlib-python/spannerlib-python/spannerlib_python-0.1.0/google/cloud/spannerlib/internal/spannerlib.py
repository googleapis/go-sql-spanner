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
"""Module for interacting with the SpannerLib shared library."""

from contextlib import contextmanager
import ctypes
from importlib.resources import as_file, files
import logging
from pathlib import Path
import platform
from typing import ClassVar, Final, Generator, Optional

from .errors import SpannerLibError
from .message import Message
from .types import GoSlice, GoString

logger = logging.getLogger(__name__)

CURRENT_PACKAGE: Final[str] = __package__ or "google.cloud.spannerlib.internal"
LIB_DIR_NAME: Final[str] = "lib"


@contextmanager
def get_shared_library(
    library_name: str, subdirectory: str = LIB_DIR_NAME
) -> Generator[Path, None, None]:
    """
    Context manager to yield a physical path to a shared library.

    Compatible with Python 3.8+ and Zip/Egg imports.
    """
    try:

        package_root = files(CURRENT_PACKAGE)
        resource_ref = package_root.joinpath(subdirectory, library_name)

        with as_file(resource_ref) as lib_path:
            yield lib_path

    except (ImportError, TypeError) as e:
        raise FileNotFoundError(
            f"Could not resolve resource '{library_name}'"
            f" in '{CURRENT_PACKAGE}'"
        ) from e


class SpannerLib:
    """
    A Singleton wrapper for the SpannerLib shared library.
    """

    _lib_handle: ClassVar[Optional[ctypes.CDLL]] = None
    _instance: ClassVar[Optional["SpannerLib"]] = None

    def __new__(cls) -> "SpannerLib":
        if cls._instance is None:
            cls._instance = super(SpannerLib, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self) -> None:
        """
        Internal initialization logic. Called only once by __new__.
        """
        if SpannerLib._lib_handle is not None:
            return

        self._load_library()

    def _load_library(self) -> None:
        """
        Internal method to load the shared library.
        """
        filename: str = SpannerLib._get_lib_filename()

        with get_shared_library(filename) as lib_path:
            # Sanity check: Ensure the file actually exists
            # before handing to ctypes
            if not lib_path.exists():
                raise SpannerLibError(
                    f"Library path does not exist: {lib_path}"
                )

            try:
                # ctypes requires a string path
                SpannerLib._lib_handle = ctypes.CDLL(str(lib_path))
                self._configure_signatures()

                logger.debug(
                    "Successfully loaded shared library: %s", str(lib_path)
                )

            except (OSError, FileNotFoundError) as e:
                logger.critical(
                    "Failed to load native library at %s", str(lib_path)
                )
                SpannerLib._lib_handle = None
                raise SpannerLibError(
                    f"Could not load native dependency '{lib_path.name}': {e}"
                ) from e

    @staticmethod
    def _get_lib_filename() -> str:
        """
        Returns the filename of the shared library based on the OS
        and architecture.
        """
        system_name = platform.system()
        machine_name = platform.machine().lower()

        if system_name == "Windows":
            os_part = "win"
            ext = "dll"
        elif system_name == "Darwin":
            os_part = "osx"
            ext = "dylib"
        elif system_name == "Linux":
            os_part = "linux"
            ext = "so"
        else:
            raise SpannerLibError(
                f"Unsupported operating system: {system_name}"
            )

        if machine_name in ("amd64", "x86_64"):
            arch_part = "x64"
        elif machine_name in ("arm64", "aarch64"):
            arch_part = "arm64"
        else:
            raise SpannerLibError(f"Unsupported architecture: {machine_name}")

        return f"{os_part}-{arch_part}/spannerlib.{ext}"

    def _configure_signatures(self) -> None:
        """
        Defines the argument and return types for the C functions.
        """
        lib = SpannerLib._lib_handle
        if lib is None:
            raise SpannerLibError(
                "Library handle is None during configuration."
            )

        try:
            # 1. Release
            # Corresponds to:
            # GoInt32 Release(GoInt64 pinnerId);
            if hasattr(lib, "Release"):
                lib.Release.argtypes = [ctypes.c_longlong]
                lib.Release.restype = ctypes.c_int32

            # 2. CreatePool
            # Corresponds to:
            # CreatePool_return CreatePool(GoString connectionString);
            if hasattr(lib, "CreatePool"):
                lib.CreatePool.argtypes = [GoString]
                lib.CreatePool.restype = Message

            # 3. ClosePool
            # Corresponds to:
            # ClosePool_return ClosePool(GoInt64 poolId);
            if hasattr(lib, "ClosePool"):
                lib.ClosePool.argtypes = [ctypes.c_longlong]
                lib.ClosePool.restype = Message

            # 4. CreateConnection
            # Corresponds to:
            # CreateConnection_return CreateConnection(GoInt64 poolId);
            if hasattr(lib, "CreateConnection"):
                lib.CreateConnection.argtypes = [ctypes.c_longlong]
                lib.CreateConnection.restype = Message

            # 5. CloseConnection
            # Corresponds to:
            # CloseConnection_return CloseConnection(GoInt64 poolId,
            # GoInt64 connId);
            if hasattr(lib, "CloseConnection"):
                lib.CloseConnection.argtypes = [
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                ]
                lib.CloseConnection.restype = Message

            # 6. Execute
            # Corresponds to:
            # Execute_return Execute(GoInt64 poolId, GoInt64 connectionId,
            # GoSlice statement);
            if hasattr(lib, "Execute"):
                lib.Execute.argtypes = [
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                    GoSlice,
                ]
                lib.Execute.restype = Message

            # 7. ExecuteBatch
            # Corresponds to:
            # ExecuteBatch_return ExecuteBatch(GoInt64 poolId,
            # GoInt64 connectionId, GoSlice statements);
            if hasattr(lib, "ExecuteBatch"):
                lib.ExecuteBatch.argtypes = [
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                    GoSlice,
                ]
                lib.ExecuteBatch.restype = Message

            # 8. Next
            # Corresponds to:
            # Next_return Next(GoInt64 poolId, GoInt64 connId,
            # GoInt64 rowsId, GoInt32 numRows, GoInt32 encodeRowOption);
            if hasattr(lib, "Next"):
                lib.Next.argtypes = [
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                    ctypes.c_int32,
                    ctypes.c_int32,
                ]
                lib.Next.restype = Message

            # 9. CloseRows
            # Corresponds to:
            # CloseRows_return CloseRows(GoInt64 poolId, GoInt64 connId,
            # GoInt64 rowsId);
            if hasattr(lib, "CloseRows"):
                lib.CloseRows.argtypes = [
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                ]
                lib.CloseRows.restype = Message

            # 10. Metadata
            # Corresponds to:
            # Metadata_return Metadata(GoInt64 poolId, GoInt64 connId,
            # GoInt64 rowsId);
            if hasattr(lib, "Metadata"):
                lib.Metadata.argtypes = [
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                ]
                lib.Metadata.restype = Message

            # 11. ResultSetStats
            # Corresponds to:
            # ResultSetStats_return ResultSetStats(GoInt64 poolId,
            # GoInt64 connId, GoInt64 rowsId);
            if hasattr(lib, "ResultSetStats"):
                lib.ResultSetStats.argtypes = [
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                ]
                lib.ResultSetStats.restype = Message

            # 12. BeginTransaction
            # Corresponds to:
            # BeginTransaction_return BeginTransaction(GoInt64 poolId,
            # GoInt64 connectionId, GoSlice txOpts);
            if hasattr(lib, "BeginTransaction"):
                lib.BeginTransaction.argtypes = [
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                    GoSlice,
                ]
                lib.BeginTransaction.restype = Message

            # 13. Commit
            # Corresponds to:
            # Commit_return Commit(GoInt64 poolId, GoInt64 connectionId);
            if hasattr(lib, "Commit"):
                lib.Commit.argtypes = [
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                ]
                lib.Commit.restype = Message

            # 14. Rollback
            # Corresponds to:
            # Rollback_return Rollback(GoInt64 poolId, GoInt64 connectionId);
            if hasattr(lib, "Rollback"):
                lib.Rollback.argtypes = [
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                ]
                lib.Rollback.restype = Message

            # 15. WriteMutations
            # Corresponds to:
            # WriteMutations_return WriteMutations(GoInt64 poolId,
            # GoInt64 connectionId, GoSlice mutationsBytes);
            if hasattr(lib, "WriteMutations"):
                lib.WriteMutations.argtypes = [
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                    GoSlice,
                ]
                lib.WriteMutations.restype = Message

            # 16. NextResultSet
            # Corresponds to:
            # NextResultSet_return NextResultSet(GoInt64 poolId,
            # GoInt64 connId, GoInt64 rowsId)
            if hasattr(lib, "NextResultSet"):
                lib.NextResultSet.argtypes = [
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                    ctypes.c_longlong,
                ]
                lib.NextResultSet.restype = Message

        except AttributeError as e:
            raise SpannerLibError(
                f"Symbol missing in native library: {e}"
            ) from e

    @property
    def lib(self) -> ctypes.CDLL:
        """Returns the loaded shared library handle."""
        if self._lib_handle is None:
            raise SpannerLibError(
                "SpannerLib has not been initialized correctly."
            )
        return self._lib_handle

    def release(self, handle: int) -> int:
        """Calls the Release function from the shared library.

        Args:
            handle: The handle to release.

        Returns:
            int: The result of the release operation.
        """
        return self.lib.Release(ctypes.c_longlong(handle))

    def create_pool(self, conn_str: str) -> Message:
        """Calls the CreatePool function from the shared library.

        Args:
            conn_str: The connection string.

        Returns:
            Message: The result containing the pool handle.
        """
        go_str = GoString.from_str(conn_str)
        msg = self.lib.CreatePool(go_str)
        return msg.bind_library(self)

    def close_pool(self, pool_handle: int) -> Message:
        """Calls the ClosePool function from the shared library.

        Args:
            pool_handle: The pool ID.

        Returns:
            Message: The result of the close operation.
        """
        msg = self.lib.ClosePool(ctypes.c_longlong(pool_handle))
        return msg.bind_library(self)

    def create_connection(self, pool_handle: int) -> Message:
        """Calls the CreateConnection function from the shared library.

        Args:
            pool_handle: The pool ID.

        Returns:
            Message: The result containing the connection handle.
        """
        msg = self.lib.CreateConnection(ctypes.c_longlong(pool_handle))
        return msg.bind_library(self)

    def close_connection(self, pool_handle: int, conn_handle: int) -> Message:
        """Calls the CloseConnection function from the shared library.

        Args:
            pool_handle: The pool ID.
            conn_handle: The connection ID.

        Returns:
            Message: The result of the close operation.
        """
        msg = self.lib.CloseConnection(
            ctypes.c_longlong(pool_handle), ctypes.c_longlong(conn_handle)
        )
        return msg.bind_library(self)

    def execute(
        self, pool_handle: int, conn_handle: int, request: bytes
    ) -> Message:
        """Calls the Execute function from the shared library.

        Args:
            pool_handle: The pool ID.
            conn_handle: The connection ID.
            request: The serialized ExecuteSqlRequest request.

        Returns:
            Message: The result of the execution.
        """
        go_slice = GoSlice.from_bytes(request)
        msg = self.lib.Execute(
            ctypes.c_longlong(pool_handle),
            ctypes.c_longlong(conn_handle),
            go_slice,
        )
        return msg.bind_library(self)

    def execute_batch(
        self, pool_handle: int, conn_handle: int, request: bytes
    ) -> Message:
        """Calls the ExecuteBatch function from the shared library.

        Args:
            pool_handle: The pool ID.
            conn_handle: The connection ID.
            request: The serialized ExecuteBatchDmlRequest request.

        Returns:
            Message: The result of the execution.
        """
        go_slice = GoSlice.from_bytes(request)
        msg = self.lib.ExecuteBatch(
            ctypes.c_longlong(pool_handle),
            ctypes.c_longlong(conn_handle),
            go_slice,
        )
        return msg.bind_library(self)

    def next(
        self,
        pool_handle: int,
        conn_handle: int,
        rows_handle: int,
        num_rows: int,
        encode_row_option: int,
    ) -> Message:
        """Calls the Next function from the shared library.

        Args:
            pool_handle: The pool ID.
            conn_handle: The connection ID.
            rows_handle: The rows ID.
            num_rows: The number of rows to fetch.
            encode_row_option: Option for row encoding.

        Returns:
            Message: The result containing the rows.
        """
        msg = self.lib.Next(
            ctypes.c_longlong(pool_handle),
            ctypes.c_longlong(conn_handle),
            ctypes.c_longlong(rows_handle),
            ctypes.c_int32(num_rows),
            ctypes.c_int32(encode_row_option),
        )
        return msg.bind_library(self)

    def next_result_set(
        self,
        pool_handle: int,
        conn_handle: int,
        rows_handle: int,
    ) -> Message:
        """Calls the NextResultSet function from the shared library.

        Args:
            pool_handle: The pool ID.
            conn_handle: The connection ID.
            rows_handle: The rows ID.

        Returns:
            Message: Returns the ResultSetMetadata of the next result set,
            or None if there are no more result sets.
        """
        msg = self.lib.NextResultSet(
            ctypes.c_longlong(pool_handle),
            ctypes.c_longlong(conn_handle),
            ctypes.c_longlong(rows_handle),
        )
        return msg.bind_library(self)

    def close_rows(
        self, pool_handle: int, conn_handle: int, rows_handle: int
    ) -> Message:
        """Calls the CloseRows function from the shared library.

        Args:
            pool_handle: The pool ID.
            conn_handle: The connection ID.
            rows_handle: The rows ID.

        Returns:
            Message: The result of the close operation.
        """
        msg = self.lib.CloseRows(
            ctypes.c_longlong(pool_handle),
            ctypes.c_longlong(conn_handle),
            ctypes.c_longlong(rows_handle),
        )
        return msg.bind_library(self)

    def metadata(
        self, pool_handle: int, conn_handle: int, rows_handle: int
    ) -> Message:
        """Calls the Metadata function from the shared library.

        Args:
            pool_handle: The pool ID.
            conn_handle: The connection ID.
            rows_handle: The rows ID.

        Returns:
            Message: The result containing the metadata.
        """
        msg = self.lib.Metadata(
            ctypes.c_longlong(pool_handle),
            ctypes.c_longlong(conn_handle),
            ctypes.c_longlong(rows_handle),
        )
        return msg.bind_library(self)

    def result_set_stats(
        self, pool_handle: int, conn_handle: int, rows_handle: int
    ) -> Message:
        """Calls the ResultSetStats function from the shared library.

        Args:
            pool_handle: The pool ID.
            conn_handle: The connection ID.
            rows_handle: The rows ID.

        Returns:
            Message: The result containing the stats.
        """
        msg = self.lib.ResultSetStats(
            ctypes.c_longlong(pool_handle),
            ctypes.c_longlong(conn_handle),
            ctypes.c_longlong(rows_handle),
        )
        return msg.bind_library(self)

    def begin_transaction(
        self, pool_handle: int, conn_handle: int, tx_opts: bytes
    ) -> Message:
        """Calls the BeginTransaction function from the shared library.

        Args:
            pool_handle: The pool ID.
            conn_handle: The connection ID.
            tx_opts: The serialized TransactionOptions.

        Returns:
            Message: The result of the transaction begin.
        """
        go_slice = GoSlice.from_bytes(tx_opts)
        msg = self.lib.BeginTransaction(
            ctypes.c_longlong(pool_handle),
            ctypes.c_longlong(conn_handle),
            go_slice,
        )
        return msg.bind_library(self)

    def commit(self, pool_handle: int, conn_handle: int) -> Message:
        """Calls the Commit function from the shared library.

        Args:
            pool_handle: The pool ID.
            conn_handle: The connection ID.

        Returns:
            Message: The result of the commit.
        """
        msg = self.lib.Commit(
            ctypes.c_longlong(pool_handle), ctypes.c_longlong(conn_handle)
        )
        return msg.bind_library(self)

    def rollback(self, pool_handle: int, conn_handle: int) -> Message:
        """Calls the Rollback function from the shared library.

        Args:
            pool_handle: The pool ID.
            conn_handle: The connection ID.

        Returns:
            Message: The result of the rollback.
        """
        msg = self.lib.Rollback(
            ctypes.c_longlong(pool_handle), ctypes.c_longlong(conn_handle)
        )
        return msg.bind_library(self)

    def write_mutations(
        self, pool_handle: int, conn_handle: int, request: bytes
    ) -> Message:
        """Calls the WriteMutations function from the shared library.

        Args:
            pool_handle: The pool ID.
            conn_handle: The connection ID.
            request: The serialized Mutation request.
            (BatchWriteRequest.MutationGroup)

        Returns:
            Message: The result of the write operation.
        """
        go_slice = GoSlice.from_bytes(request)
        msg = self.lib.WriteMutations(
            ctypes.c_longlong(pool_handle),
            ctypes.c_longlong(conn_handle),
            go_slice,
        )
        return msg.bind_library(self)
