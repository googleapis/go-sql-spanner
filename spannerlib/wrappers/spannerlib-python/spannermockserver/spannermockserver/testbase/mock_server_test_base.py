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

"""
Base class for tests using the mock Spanner server.
"""
import logging
import unittest

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud.spanner_v1 import Client, FixedSizePool
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.instance import Instance
import grpc

from spannermockserver.mock_database_admin import DatabaseAdminServicer
from spannermockserver.mock_spanner import SpannerServicer, start_mock_server


class MockServerTestBase(unittest.TestCase):
    """
    Base class for tests using the mock Spanner server.
    """

    server: grpc.Server = None
    spanner_service: SpannerServicer = None
    database_admin_service: DatabaseAdminServicer = None
    port: int = None
    logger: logging.Logger = None

    def __init__(self, *args, **kwargs):
        super(MockServerTestBase, self).__init__(*args, **kwargs)
        self._client = None
        self._instance = None
        self._database = None
        self.logger = logging.getLogger("MockServerTestBase")
        self.logger.setLevel(logging.WARN)

    @classmethod
    def setup_class(cls):
        """Sets up the mock server before any tests run."""
        (
            MockServerTestBase.server,
            MockServerTestBase.spanner_service,
            MockServerTestBase.database_admin_service,
            MockServerTestBase.port,
        ) = start_mock_server()

    @classmethod
    def teardown_class(cls):
        """Tears down the mock server after all tests have run."""
        if MockServerTestBase.server is not None:
            MockServerTestBase.server.stop(grace=None)
            Client.NTH_CLIENT.reset()
            MockServerTestBase.server = None

    def setup_method(self, *args, **kwargs):
        """Sets up the test method."""
        self._client = None
        self._instance = None
        self._database = None

    def teardown_method(self, *args, **kwargs):
        """Tears down the test method."""
        MockServerTestBase.spanner_service.clear_requests()
        MockServerTestBase.database_admin_service.clear_requests()
        mock_spanner = MockServerTestBase.spanner_service.mock_spanner
        mock_spanner.results = {}
        mock_spanner.execute_streaming_sql_results = {}
        mock_spanner.errors = {}

    @property
    def client(self) -> Client:
        """Returns a Spanner client connected to the mock server."""
        if self._client is None:
            self._client = Client(
                project="p",
                credentials=AnonymousCredentials(),
                client_options=ClientOptions(
                    api_endpoint="localhost:" + str(MockServerTestBase.port),
                ),
            )
        return self._client

    @property
    def instance(self) -> Instance:
        """Returns a Spanner instance."""
        if self._instance is None:
            self._instance = self.client.instance("test-instance")
        return self._instance

    @property
    def database(self) -> Database:
        """Returns a Spanner database."""
        if self._database is None:
            self._database = self.instance.database(
                "test-database",
                pool=FixedSizePool(size=10),
                enable_interceptors_in_tests=True,
                logger=self.logger,
            )
        return self._database
