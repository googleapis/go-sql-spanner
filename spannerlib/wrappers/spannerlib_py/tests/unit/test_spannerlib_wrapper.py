import os
import sys
import unittest
from unittest import mock

from google.cloud.spannerlib.internal.spannerlib import _GoReturn

from google.cloud.spannerlib import Pool

# Adjust path to import from src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

TEST_CONNECTION_STRING = (
    "projects/test-project/instances/test-instance/databases/test-database"
)


class TestSpannerLib(unittest.TestCase):
    @mock.patch("google.cloud.spannerlib.pool._lib")
    def test_pool_creation_and_close(self, mock_pool_lib):
        mock_pool_lib.CreatePool.return_value = _GoReturn(
            r0=1, r1=0, r2=1, r3=0, r4=None
        )
        mock_pool_lib.ClosePool.return_value = _GoReturn(
            r0=0, r1=0, r2=0, r3=0, r4=None
        )

        pool = Pool(TEST_CONNECTION_STRING)
        self.assertEqual(pool.pool_id, 1)
        self.assertFalse(pool._closed)
        pool.close()
        self.assertTrue(pool._closed)
        mock_pool_lib.CreatePool.assert_called_once()
        mock_pool_lib.ClosePool.assert_called_once_with(1)

    @mock.patch("google.cloud.spannerlib.connection._lib")
    @mock.patch("google.cloud.spannerlib.pool._lib")
    def test_connection_creation_and_close(self, mock_pool_lib, mock_conn_lib):
        mock_pool_lib.CreatePool.return_value = _GoReturn(
            r0=1, r1=0, r2=1, r3=0, r4=None
        )
        mock_pool_lib.CreateConnection.return_value = _GoReturn(
            r0=101, r1=0, r2=101, r3=0, r4=None
        )
        mock_conn_lib.CloseConnection.return_value = _GoReturn(
            r0=0, r1=0, r2=0, r3=0, r4=None
        )
        mock_pool_lib.ClosePool.return_value = _GoReturn(
            r0=0, r1=0, r2=0, r3=0, r4=None
        )

        with Pool(TEST_CONNECTION_STRING) as pool:
            conn = pool.create_connection()
            self.assertEqual(conn.conn_id, 101)
            self.assertFalse(conn._closed)
            conn.close()
            self.assertTrue(conn._closed)
            mock_pool_lib.CreateConnection.assert_called_once_with(1)
            mock_conn_lib.CloseConnection.assert_called_once_with(1, 101)

        mock_pool_lib.ClosePool.assert_called_once_with(1)

    @mock.patch("google.cloud.spannerlib.connection._lib")
    @mock.patch("google.cloud.spannerlib.pool._lib")
    def test_connection_with_statement(self, mock_pool_lib, mock_conn_lib):
        mock_pool_lib.CreatePool.return_value = _GoReturn(
            r0=1, r1=0, r2=1, r3=0, r4=None
        )
        mock_pool_lib.CreateConnection.return_value = _GoReturn(
            r0=101, r1=0, r2=101, r3=0, r4=None
        )
        mock_conn_lib.CloseConnection.return_value = _GoReturn(
            r0=0, r1=0, r2=0, r3=0, r4=None
        )
        mock_pool_lib.ClosePool.return_value = _GoReturn(
            r0=0, r1=0, r2=0, r3=0, r4=None
        )

        with Pool(TEST_CONNECTION_STRING) as pool:
            with pool.create_connection() as conn:
                self.assertEqual(conn.conn_id, 101)
            mock_conn_lib.CloseConnection.assert_called_once_with(1, 101)
        mock_pool_lib.ClosePool.assert_called_once_with(1)


if __name__ == "__main__":
    unittest.main()
