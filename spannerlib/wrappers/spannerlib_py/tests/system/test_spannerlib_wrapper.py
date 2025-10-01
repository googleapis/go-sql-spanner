import os
import sys
import unittest

# Adjust path to import from src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from google.cloud.spannerlib import Pool

# To run these E2E tests against a Cloud Spanner Emulator:
# 1. Start the emulator: gcloud emulators spanner start
#       docker pull gcr.io/cloud-spanner-emulator/emulator
#       docker run -p 9010:9010 -p 9020:9020 -d gcr.io/cloud-spanner-emulator/emulator
# 2. Set the environment variable: export SPANNER_EMULATOR_HOST=localhost:9010
# 3. Create a test instance and database in the emulator:
#       gcloud spanner instances create test-instance --config=emulator-config --description="Test Instance" --nodes=1
#    gcloud spanner databases create testdb --instance=test-instance
# 4. Run the tests: python3 -m unittest src/tests/e2e/test_spannerlib_wrapper.py
#
# You can also override the connection string by setting SPANNER_CONNECTION_STRING.

TEST_ON_PROD = False

EMULATOR_TEST_CONNECTION_STRING = "projects/test-project/instances/test-instance/databases/testdb?autoConfigEmulator=true"
PROD_TEST_CONNECTION_STRING = (
    "projects/span-cloud-testing/instances/asapha-test/databases/testdb"
)

TEST_CONNECTION_STRING = (
    PROD_TEST_CONNECTION_STRING if TEST_ON_PROD else EMULATOR_TEST_CONNECTION_STRING
)


class TestSpannerE2E(unittest.TestCase):

    def test_pool_and_connection(self):
        pool = None  # Initialize pool to None
        try:
            # Test Pool creation
            pool = Pool(TEST_CONNECTION_STRING)
            print(f"Pool created with ID: {pool.pool_id}")  # Debug print
            self.assertIsNotNone(pool.pool_id, "Pool ID should not be None")
            self.assertNotEqual(
                pool.pool_id, 0, "Pool ID should not be 0"
            )  # Check for non-zero
            self.assertFalse(pool._closed, "Pool should not be closed initially")

            # Test Connection creation
            conn = pool.create_connection()
            print(f"Connection created with ID: {conn.conn_id}")  # Debug print
            self.assertIsNotNone(conn.conn_id, "Connection ID should not be None")
            self.assertNotEqual(conn.conn_id, 0, "Connection ID should not be 0")
            self.assertFalse(conn._closed, "Connection should not be closed initially")

            # Test Connection close
            conn.close()
            self.assertTrue(conn._closed, "Connection should be closed")

            # Test Connection context manager
            with pool.create_connection() as conn2:
                self.assertIsNotNone(conn2.conn_id)
                self.assertFalse(conn2._closed)
            self.assertTrue(
                conn2._closed, "Connection should be closed after with statement"
            )

        except Exception as e:
            self.fail(f"E2E test failed with exception: {e}")
        finally:
            # Test Pool close
            if pool:
                pool.close()
                self.assertTrue(pool._closed, "Pool should be closed")

    def test_pool_context_manager(self):
        try:
            with Pool(TEST_CONNECTION_STRING) as pool:
                self.assertIsNotNone(pool.pool_id)
                self.assertFalse(pool._closed)
                with pool.create_connection() as conn:
                    self.assertIsNotNone(conn.conn_id)
                    self.assertFalse(conn._closed)
                self.assertTrue(conn._closed)
            self.assertTrue(pool._closed)
        except Exception as e:
            self.fail(f"E2E test failed with exception: {e}")


if __name__ == "__main__":
    print("Running E2E tests... This requires a live Spanner instance or Emulator.")
    if not TEST_ON_PROD:
        # Set environment variable for Spanner Emulator
        os.environ["SPANNER_EMULATOR_HOST"] = "localhost:9010"
        print(f"Set SPANNER_EMULATOR_HOST to {os.environ['SPANNER_EMULATOR_HOST']}")
    print(f"Using Connection String: {TEST_CONNECTION_STRING}")
    unittest.main()
