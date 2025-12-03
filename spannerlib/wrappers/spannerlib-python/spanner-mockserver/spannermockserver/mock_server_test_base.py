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
import logging

from google.api_core import fixtures
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud.spanner_v1 import Client
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.instance import Instance
import grpc

from .mock_database_admin import DatabaseAdminServicer
from .mock_spanner import SpannerServicer, start_mock_server


class MockServerTestBase(fixtures.TestBase):
    """Base class for mock server tests."""

    server: grpc.Server = None
    spanner_service: SpannerServicer = None
    database_admin_service: DatabaseAdminServicer = None
    port: int = None
    logger: logging.Logger = None

    @classmethod
    def setup_class(cls):
        MockServerTestBase.logger = logging.getLogger("level warning")
        MockServerTestBase.logger.setLevel(logging.WARN)
        (
            MockServerTestBase.server,
            MockServerTestBase.spanner_service,
            MockServerTestBase.database_admin_service,
            MockServerTestBase.port,
        ) = start_mock_server()

    @classmethod
    def teardown_class(cls):
        if MockServerTestBase.server is not None:
            MockServerTestBase.server.stop(grace=None)
            MockServerTestBase.server = None

    def setup_method(self):
        self._client = None
        self._instance = None
        self._database = None
        _ = self.database

    def teardown_method(self):
        MockServerTestBase.spanner_service.clear_requests()
        MockServerTestBase.database_admin_service.clear_requests()

    @property
    def client(self) -> Client:
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
        if self._instance is None:
            self._instance = self.client.instance("i")
        return self._instance

    @property
    def database(self) -> Database:
        logger = logging.getLogger("level warning")
        logger.setLevel(logging.WARN)
        if self._database is None:
            self._database = self.instance.database("d", logger=logger)
        return self._database
