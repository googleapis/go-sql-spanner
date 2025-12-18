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
from google.cloud.spannerlib.pool import Pool

from . import errors
from .cursor import Cursor


def check_not_closed(function):
    """`Connection` class methods decorator.

    Raise an exception if the connection is closed.

    :raises: :class:`InterfaceError` if the connection is closed.
    """

    def wrapper(connection, *args, **kwargs):
        if connection._closed:
            raise errors.InterfaceError("Connection is closed")

        return function(connection, *args, **kwargs)

    return wrapper


class Connection:
    def __init__(self, internal_connection):
        """
        args:
            internal_connection: An instance of
                google.cloud.spannerlib.Connection
        """
        self._internal_conn = internal_connection
        self._closed = False

    @check_not_closed
    def cursor(self):
        return Cursor(self)
    
    @check_not_closed
    def begin(self):
        try:
            self._internal_conn.begin()
        except Exception as e:
            raise errors.map_spanner_error(e)

    @check_not_closed
    def commit(self):
        try:
            self._internal_conn.commit()
        except Exception as e:
            raise errors.map_spanner_error(e)

    @check_not_closed
    def rollback(self):
        try:
            self._internal_conn.rollback()
        except Exception as e:
            raise errors.map_spanner_error(e)

    def close(self):
        if not self._closed:
            self._internal_conn.close()
            self._closed = True


def connect(connection_string, **kwargs):
    # Create the pool
    pool = Pool.create_pool(connection_string)

    # Create the low-level connection
    internal_conn = pool.create_connection()

    return Connection(internal_conn)
