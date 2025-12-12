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

import os

from spannermockserver import MockServerTestBase

from google.cloud.spannerlib import Pool

from ._helper import get_mockserver_connection_string


class TestPoolOnMockServer(MockServerTestBase):
    """End-to-end tests for the Pool class on mock server."""

    @classmethod
    def setup_class(cls):
        super().setup_class()
        os.environ["SPANNER_EMULATOR_HOST"] = f"localhost:{cls.port}"

    @classmethod
    def teardown_class(cls):
        if "SPANNER_EMULATOR_HOST" in os.environ:
            del os.environ["SPANNER_EMULATOR_HOST"]
        super().teardown_class()

    def test_pool_creation_and_close(self) -> None:
        pool = Pool.create_pool(get_mockserver_connection_string(self.port))
        assert pool.oid is not None, "Pool ID should not be None"
        assert not pool.closed, "Pool should not be closed initially"
        pool.close()
        assert pool.closed, "Pool should be closed"
        # Test closing again is safe
        pool.close()
        assert pool.closed, "Pool should remain closed"
