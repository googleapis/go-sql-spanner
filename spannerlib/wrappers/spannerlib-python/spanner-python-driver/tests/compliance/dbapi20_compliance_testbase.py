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
import time
import unittest
from unittest.mock import MagicMock

from .sql_factory import SQLFactory


def encode(s: str) -> bytes:
    return s.encode("utf-8")


def decode(b: bytes) -> str:
    return b.decode("utf-8")


class DBAPI20ComplianceTestBase(unittest.TestCase):

    __test__ = False
    driver = None
    errors = None
    connect_args = ()  # List of arguments to pass to connect
    connect_kw_args = {}  # Keyword arguments for connect
    dialect = "GoogleSQL"

    @property
    def sql_factory(self):
        return SQLFactory.get_factory(self.dialect)

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.cleanup()

    def tearDown(self):
        self.cleanup()

    def cleanup(self):
        try:
            con = self._connect()
            try:
                cur = con.cursor()
                for ddl in self.sql_factory.stmt_ddl_drop_all_cmds:
                    try:
                        cur.execute(ddl)
                        con.commit()
                    except self.driver.Error:
                        # Assume table didn't exist. Other tests will check if
                        # execute is busted.
                        pass
            finally:
                con.close()
        except Exception:
            pass

    def _execute_select1(self, cur):
        cur.execute(self.sql_factory.stmt_dql_select_1)

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

    def test_connect(self):
        conn = self._connect()
        conn.close()

    def test_cursor(self):
        con = self._connect()
        try:
            curr = con.cursor()
            self.assertIsNotNone(curr)
        finally:
            con.close()

    def test_cursor_isolation(self):
        con = self._connect()
        try:
            # Make sure cursors created from the same connection have
            # the documented transaction isolation level
            cur1 = con.cursor()
            cur2 = con.cursor()
            cur1.execute(self.sql_factory.stmt_ddl_create_table1)
            # DDL usually requires a clean slate or commit in some test envs
            con.commit()
            cur1.execute(
                self.sql_factory.stmt_dml_insert_table1(
                    "1, 'Innocent Alice', 100"
                )
            )
            con.commit()
            cur2.execute(self.sql_factory.stmt_dql_select_cols_table1("name"))
            users = cur2.fetchone()

            self.assertEqual(len(users), 1)
            self.assertEqual(users[0], "Innocent Alice")
        finally:
            con.close()

    def test_close(self):
        con = self._connect()
        try:
            cur = con.cursor()
        finally:
            con.close()

        # cursor.execute should raise an Error if called after connection
        # closed

        self.assertRaises(self.driver.Error, self._execute_select1, cur)

        # connection.commit should raise an Error if called after connection'
        # closed.'
        self.assertRaises(self.driver.Error, con.commit)

    def test_non_idempotent_close(self):
        con = self._connect()
        con.close()
        # connection.close should raise an Error if called more than once
        # reasonable persons differ about the usefulness of this test
        # and this feature
        self.assertRaises(self.driver.Error, con.close)

    def test_execute_select1(self):
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(self.sql_factory.stmt_dql_select_1)
            self.assertEqual(cur.fetchone(), ("1",))
        finally:
            con.close()

    def test_rollback(self):
        con = self._connect()
        try:
            # If rollback is defined, it should either work or throw
            # the documented exception
            if hasattr(con, "rollback"):
                try:
                    con.rollback()
                except self.driver.NotSupportedError:
                    pass
        finally:
            con.close()

    def test_commit(self):
        con = self._connect()
        try:
            # Commit must work, even if it doesn't do anything
            con.commit()
        finally:
            con.close()

    def test_description(self):
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(self.sql_factory.stmt_ddl_create_table1)

            self.assertEqual(
                cur.description,
                None,
                "cursor.description should be none after executing a "
                "statement that can return no rows (such as DDL)",
            )
            cur.execute(self.sql_factory.stmt_dql_select_cols_table1("name"))
            self.assertEqual(
                len(cur.description),
                1,
                "cursor.description describes too many columns",
            )
            self.assertEqual(
                len(cur.description[0]),
                7,
                "cursor.description[x] tuples must have 7 elements",
            )
            self.assertEqual(
                cur.description[0][0].lower(),
                "name",
                "cursor.description[x][0] must return column name",
            )
            self.assertEqual(
                cur.description[0][1],
                self.driver.STRING,
                "cursor.description[x][1] must return column type. Got %r"
                % cur.description[0][1],
            )

            # Make sure self.description gets reset
            cur.execute(self.sql_factory.stmt_ddl_create_table2)
            self.assertEqual(
                cur.description,
                None,
                "cursor.description not being set to None when executing "
                "no-result statements (eg. DDL)",
            )
        finally:
            con.close()

    def test_arraysize(self):
        # Not much here - rest of the tests for this are in test_fetchmany
        con = self._connect()
        try:
            cur = con.cursor()
            self.assertTrue(
                hasattr(cur, "arraysize"),
                "cursor.arraysize must be defined",
            )
        finally:
            con.close()

    def test_Date(self):
        d1 = self.driver.Date(2002, 12, 25)
        d2 = self.driver.DateFromTicks(
            time.mktime((2002, 12, 25, 0, 0, 0, 0, 0, 0))
        )
        # Can we assume this? API doesn't specify, but it seems implied
        self.assertEqual(str(d1), str(d2))

    def test_Time(self):
        # 1. Create the target time
        t1 = self.driver.Time(13, 45, 30)

        # 2. Create ticks using Local Time (mktime is local)
        # We use a dummy date (2001-01-01)
        target_tuple = (2001, 1, 1, 13, 45, 30, 0, 0, 0)
        ticks = time.mktime(target_tuple)

        t2 = self.driver.TimeFromTicks(ticks)

        # CHECK 1: Ensure they are the same type (likely datetime.time)
        self.assertIsInstance(t1, type(t2))

        # CHECK 2: Compare value semantics, not string representation
        # This avoids format differences but still requires timezone alignment
        self.assertEqual(t1, t2)

    def test_Timestamp(self):
        t1 = self.driver.Timestamp(2002, 12, 25, 13, 45, 30)
        t2 = self.driver.TimestampFromTicks(
            time.mktime((2002, 12, 25, 13, 45, 30, 0, 0, 0))
        )
        # Can we assume this? API doesn't specify, but it seems implied
        self.assertEqual(str(t1), str(t2))

    def test_Binary(self):
        s = "Something"
        b = self.driver.Binary(encode(s))
        self.assertEqual(s, decode(b))

    def test_STRING(self):
        self.assertTrue(
            hasattr(self.driver, "STRING"), "module.STRING must be defined"
        )

    def test_BINARY(self):
        self.assertTrue(
            hasattr(self.driver, "BINARY"), "module.BINARY must be defined."
        )

    def test_NUMBER(self):
        self.assertTrue(
            hasattr(self.driver, "NUMBER"), "module.NUMBER must be defined."
        )

    def test_DATETIME(self):
        self.assertTrue(
            hasattr(self.driver, "DATETIME"), "module.DATETIME must be defined."
        )

    def test_ROWID(self):
        self.assertTrue(
            hasattr(self.driver, "ROWID"), "module.ROWID must be defined."
        )
