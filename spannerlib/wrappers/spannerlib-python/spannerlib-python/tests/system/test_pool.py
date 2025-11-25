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
"""System tests for the Pool class."""
import pytest

from google.cloud.spannerlib import Pool

from ._helper import get_test_connection_string, setup_test_env

# To run these E2E tests against a Cloud Spanner Emulator:
# 1. Start the emulator: gcloud emulators spanner start
# 2. Set the environment variable: export SPANNER_EMULATOR_HOST=localhost:9010
# 3. Create a test instance and database in the emulator.
# 4. Run the tests: nox -s system-3.13


@pytest.fixture(scope="module", autouse=True)
def test_env():
    """Sets up the test environment for the module."""
    setup_test_env()


class TestPoolE2E:
    """End-to-end tests for the Pool class."""

    def test_pool_creation_and_close(self) -> None:
        """Test basic pool creation and explicit close."""
        pool = Pool.create_pool(get_test_connection_string())
        assert pool.oid is not None, "Pool ID should not be None"
        assert not pool.closed, "Pool should not be closed initially"
        pool.close()
        assert pool.closed, "Pool should be closed"
        # Test closing again is safe
        pool.close()
        assert pool.closed, "Pool should remain closed"

    def test_pool_context_manager(self) -> None:
        """Test pool creation and closure using a context manager."""
        with Pool.create_pool(get_test_connection_string()) as pool:
            assert pool.oid is not None
            assert not pool.closed
        assert pool.closed, "Pool should be closed after exiting with block"
