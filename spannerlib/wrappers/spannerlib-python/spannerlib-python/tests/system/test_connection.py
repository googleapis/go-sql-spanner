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
"""System tests for the Connection class."""
import pytest

from google.cloud.spannerlib import Pool

from ._helper import get_test_connection_string, setup_test_env


@pytest.fixture(scope="module", autouse=True)
def test_env():
    """Sets up the test environment for the module."""
    setup_test_env()


@pytest.fixture(scope="module")
def pool():
    """Creates a connection pool for the test module."""
    pool = Pool.create_pool(get_test_connection_string())
    yield pool
    if pool:
        pool.close()


@pytest.fixture()
def connection(pool):
    """Creates a connection from the pool for each test."""
    conn = pool.create_connection()
    yield conn
    if conn:
        conn.close()


class TestConnectionE2E:
    """End-to-end tests for the Connection class."""

    def test_connection_creation(self, connection):
        """Tests that a connection can be created."""
        assert connection is not None
        assert connection.oid > 0
