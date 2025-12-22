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
from google.cloud.spanner_v1 import ExecuteSqlRequest

from . import errors
from .types import _type_code_to_dbapi_type


def check_not_closed(function):
    """`Cursor` class methods decorator.

    Raise an exception if the cursor is closed.

    :raises: :class:`InterfaceError` if the cursor is closed.
    """

    def wrapper(cursor, *args, **kwargs):
        if cursor._closed:
            raise errors.InterfaceError("Cursor is closed")

        return function(cursor, *args, **kwargs)

    return wrapper


class Cursor:
    def __init__(self, connection):
        self._connection = connection
        self._rows = None  # Holds the google.cloud.spannerlib.rows.Rows object
        self._closed = False
        self.arraysize = 1
        self._rowcount = -1

    @property
    def description(self):
        if not self._rows:
            return None

        try:
            metadata = self._rows.metadata()
            if not metadata or not metadata.row_type:
                return None

            desc = []
            for field in metadata.row_type.fields:
                desc.append(
                    (
                        field.name,
                        _type_code_to_dbapi_type(field.type.code),
                        None,  # display_size
                        None,  # internal_size
                        None,  # precision
                        None,  # scale
                        True,  # null_ok
                    )
                )
            return tuple(desc)
        except Exception:
            return None

    @property
    def rowcount(self):
        return self._rowcount

    @check_not_closed
    def execute(self, operation, parameters=None):
        # 1. Prepare parameters (Convert dict/tuple to Protobuf
        #    Struct/ListValue)
        # 2. Build Request
        request = ExecuteSqlRequest(sql=operation)
        # ... attach params to request ...

        try:
            # Delegate to the internal wrapper connection
            self._rows = self._connection._internal_conn.execute(request)

            # Check if we have fields (SELECT query) or not (DML/DDL)
            # If we have fields, we don't want to consume the stream
            # for stats yet.
            if self.description:
                self._rowcount = -1
            else:
                # Update rowcount if it's a DML statement
                update_count = self._rows.update_count()
                if update_count != -1:
                    self._rowcount = update_count

        except Exception as e:
            raise e

    @check_not_closed
    def executemany(self, operation, seq_of_parameters):
        total_rowcount = -1
        accumulated = False

        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
            if self._rowcount != -1:
                if not accumulated:
                    total_rowcount = 0
                    accumulated = True
                total_rowcount += self._rowcount

        self._rowcount = total_rowcount

    @check_not_closed
    def fetchone(self):
        if not self._rows:
            raise errors.ProgrammingError("No result set available")

        # Rows.next() returns a protobuf ListValue
        row = self._rows.next()
        if row is None:
            return None
        return tuple(row)  # Convert ListValue to Python tuple

    @check_not_closed
    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize

        results = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            results.append(row)
        return results

    @check_not_closed
    def fetchall(self):
        results = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            results.append(row)
        return results

    def close(self):
        self._closed = True
        if self._rows:
            self._rows.close()

    @check_not_closed
    def nextset(self):
        """Skip to the next available set of results."""
        if not self._rows:
            return None

        try:
            next_metadata = self._rows.next_result_set()
            if next_metadata:
                return True
            return None
        except Exception:
            return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @check_not_closed
    def setinputsizes(self, sizes):
        """Predefine memory areas for parameters.
        This operation is a no-op implementation.
        """
        pass

    @check_not_closed
    def setoutputsize(self, size, column=None):
        """Set a column buffer size.
        This operation is a no-op implementation.
        """
        pass

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    @check_not_closed
    def callproc(self, procname, parameters=None):
        """Call a stored database procedure with the given name.

        This method is not supported by Spanner.
        """
        raise errors.NotSupportedError("Stored procedures are not supported.")
