# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
DBAPI 2.0 Compliance Test
"""
import unittest
from unittest.mock import MagicMock


class DBAPI20ComplianceTestBase(unittest.TestCase):

    __test__ = False
    driver = None
    errors = None
    connect_args = ()  # List of arguments to pass to connect
    connect_kw_args = {}  # Keyword arguments for connect

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_module_attributes(self):
        """Test module-level attributes."""
        self.assertTrue(hasattr(self.driver, "apilevel"))
        self.assertTrue(hasattr(self.driver, "threadsafety"))
        self.assertTrue(hasattr(self.driver, "paramstyle"))
        self.assertTrue(hasattr(self.driver, "connect"))

    def test_apilevel(self):
        try:
            # Must exist
            apilevel = self.driver.apilevel
            # Must equal 2.0
            self.assertEqual(apilevel, "2.0")
        except AttributeError:
            self.fail("Driver doesn't define apilevel")

    def test_threadsafety(self):
        try:
            # Must exist
            threadsafety = self.driver.threadsafety
            # Must be a valid value
            self.assertTrue(threadsafety in (0, 1, 2, 3))
        except AttributeError:
            self.fail("Driver doesn't define threadsafety")

    def test_paramstyle(self):
        try:
            # Must exist
            paramstyle = self.driver.paramstyle
            # Must be a valid value
            self.assertTrue(
                paramstyle
                in ("qmark", "numeric", "named", "format", "pyformat")
            )
        except AttributeError:
            self.fail("Driver doesn't define paramstyle")

    def test_exceptions(self):
        """Test exception hierarchy."""
        self.assertTrue(issubclass(self.errors.Warning, Exception))
        self.assertTrue(issubclass(self.errors.Error, Exception))
        self.assertTrue(
            issubclass(self.errors.InterfaceError, self.errors.Error)
        )
        self.assertTrue(
            issubclass(self.errors.DatabaseError, self.errors.Error)
        )
        self.assertTrue(
            issubclass(self.errors.DataError, self.errors.DatabaseError)
        )
        self.assertTrue(
            issubclass(self.errors.OperationalError, self.errors.DatabaseError)
        )
        self.assertTrue(
            issubclass(self.errors.IntegrityError, self.errors.DatabaseError)
        )
        self.assertTrue(
            issubclass(self.errors.InternalError, self.errors.DatabaseError)
        )
        self.assertTrue(
            issubclass(self.errors.ProgrammingError, self.errors.DatabaseError)
        )
        self.assertTrue(
            issubclass(self.errors.NotSupportedError, self.errors.DatabaseError)
        )

    def test_type_objects(self):
        """Test type objects."""
        self.assertTrue(hasattr(self.driver, "STRING"))
        self.assertTrue(hasattr(self.driver, "BINARY"))
        self.assertTrue(hasattr(self.driver, "NUMBER"))
        self.assertTrue(hasattr(self.driver, "DATETIME"))
        self.assertTrue(hasattr(self.driver, "ROWID"))

    def test_constructors(self):
        """Test type constructors."""
        self.assertTrue(hasattr(self.driver, "Date"))
        self.assertTrue(hasattr(self.driver, "Time"))
        self.assertTrue(hasattr(self.driver, "Timestamp"))
        self.assertTrue(hasattr(self.driver, "DateFromTicks"))
        self.assertTrue(hasattr(self.driver, "TimeFromTicks"))
        self.assertTrue(hasattr(self.driver, "TimestampFromTicks"))
        self.assertTrue(hasattr(self.driver, "Binary"))

    def test_connection_attributes(self):
        """Test Connection object attributes/methods."""
        # Mock connection internal
        mock_internal = MagicMock()
        conn = self.driver.Connection(mock_internal)

        self.assertTrue(hasattr(conn, "close"))
        self.assertTrue(hasattr(conn, "commit"))
        self.assertTrue(hasattr(conn, "rollback"))
        self.assertTrue(hasattr(conn, "cursor"))
        # Optional but checked because we added it
        self.assertTrue(hasattr(conn, "messages"))

    def test_cursor_attributes(self):
        """Test Cursor object attributes/methods."""
        mock_conn = MagicMock()
        cursor = self.driver.Cursor(mock_conn)

        self.assertTrue(hasattr(cursor, "description"))
        self.assertTrue(hasattr(cursor, "rowcount"))
        self.assertTrue(hasattr(cursor, "callproc"))
        self.assertTrue(hasattr(cursor, "close"))
        self.assertTrue(hasattr(cursor, "execute"))
        self.assertTrue(hasattr(cursor, "executemany"))
        self.assertTrue(hasattr(cursor, "fetchone"))
        self.assertTrue(hasattr(cursor, "fetchmany"))
        self.assertTrue(hasattr(cursor, "fetchall"))
        self.assertTrue(hasattr(cursor, "nextset"))
        self.assertTrue(hasattr(cursor, "arraysize"))
        self.assertTrue(hasattr(cursor, "setinputsizes"))
        self.assertTrue(hasattr(cursor, "setoutputsize"))

        # Test iterator
        self.assertTrue(hasattr(cursor, "__iter__"))
        self.assertTrue(hasattr(cursor, "__next__"))

        # Test callproc raising NotSupportedError
        with self.assertRaises(self.errors.NotSupportedError):
            cursor.callproc("proc")

    def _connect(self):
        try:
            r = self.driver.connect(*self.connect_args, **self.connect_kw_args)
        except AttributeError:
            self.fail("No connect method found in self.driver module")
        return r
