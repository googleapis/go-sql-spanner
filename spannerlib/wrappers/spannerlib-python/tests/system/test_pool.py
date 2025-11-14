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
from __future__ import absolute_import

import os
import sys
import unittest

from ._helper import get_test_connection_string, setup_test_env

# Adjust path to import from src
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
)

from google.cloud.spannerlib import Pool  # noqa: E402

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


class TestPoolE2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_test_env()
        print(f"Using Connection String: {get_test_connection_string()}")

    def test_pool_creation_and_close(self):
        """Test basic pool creation and explicit close."""
        pool = Pool.create_pool(get_test_connection_string())
        self.assertIsNotNone(pool.id, "Pool ID should not be None")
        self.assertFalse(pool.closed, "Pool should not be closed initially")
        pool.close()
        self.assertTrue(pool.closed, "Pool should be closed")
        # Test closing again is safe
        pool.close()
        self.assertTrue(pool.closed, "Pool should remain closed")

    def test_pool_context_manager(self):
        """Test pool creation and closure using a context manager."""
        with Pool.create_pool(get_test_connection_string()) as pool:
            self.assertIsNotNone(pool.id)
            self.assertFalse(pool.closed)
        self.assertTrue(
            pool.closed, "Pool should be closed after exiting with block"
        )


if __name__ == "__main__":
    print(
        "Running Pool E2E tests... This requires a live Spanner instance or Emulator."
    )
    unittest.main()
