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

from ._helpers import is_multiplexed_enabled


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

    def assert_requests_sequence(
        self,
        requests,
        expected_types,
        transaction_type,
        allow_multiple_batch_create=True,
    ):
        """Assert that the requests sequence matches the expected types,
        accounting for multiplexed sessions and retries.

        Args:
            requests: List of requests from spanner_service.requests
            expected_types: List of expected request types (excluding session creation requests)
            transaction_type: TransactionType enum value to check multiplexed session status
            allow_multiple_batch_create: If True, skip all leading BatchCreateSessionsRequest
            and one optional CreateSessionRequest
        """
        from google.cloud.spanner_v1 import (
            BatchCreateSessionsRequest,
            CreateSessionRequest,
        )

        mux_enabled = is_multiplexed_enabled(transaction_type)
        idx = 0
        # Skip all leading BatchCreateSessionsRequest (for retries)
        if allow_multiple_batch_create:
            while idx < len(requests) and isinstance(
                requests[idx], BatchCreateSessionsRequest
            ):
                idx += 1
            # For multiplexed, optionally skip a CreateSessionRequest
            if (
                mux_enabled
                and idx < len(requests)
                and isinstance(requests[idx], CreateSessionRequest)
            ):
                idx += 1
        else:
            if mux_enabled:
                self.assertTrue(
                    isinstance(requests[idx], BatchCreateSessionsRequest),
                    f"Expected BatchCreateSessionsRequest at index {idx}, got {type(requests[idx])}",
                )
                idx += 1
                self.assertTrue(
                    isinstance(requests[idx], CreateSessionRequest),
                    f"Expected CreateSessionRequest at index {idx}, got {type(requests[idx])}",
                )
                idx += 1
            else:
                self.assertTrue(
                    isinstance(requests[idx], BatchCreateSessionsRequest),
                    f"Expected BatchCreateSessionsRequest at index {idx}, got {type(requests[idx])}",
                )
                idx += 1
        # Check the rest of the expected request types
        for expected_type in expected_types:
            self.assertTrue(
                isinstance(requests[idx], expected_type),
                f"Expected {expected_type} at index {idx}, got {type(requests[idx])}",
            )
            idx += 1
        self.assertEqual(
            idx, len(requests), f"Expected {idx} requests, got {len(requests)}"
        )

    def adjust_request_id_sequence(
        self, expected_segments, requests, transaction_type
    ):
        """Adjust expected request ID sequence numbers based on actual session creation requests.

        Args:
            expected_segments: List of expected (method, (sequence_numbers)) tuples
            requests: List of actual requests from spanner_service.requests
            transaction_type: TransactionType enum value to check multiplexed session status

        Returns:
            List of adjusted expected segments with corrected sequence numbers
        """
        from google.cloud.spanner_v1 import (
            BatchCreateSessionsRequest,
            BeginTransactionRequest,
            CreateSessionRequest,
            ExecuteSqlRequest,
        )

        # Count session creation requests that come before the first non-session request
        session_requests_before = 0
        for req in requests:
            if isinstance(
                req, (BatchCreateSessionsRequest, CreateSessionRequest)
            ):
                session_requests_before += 1
            elif isinstance(req, (ExecuteSqlRequest, BeginTransactionRequest)):
                break

        # For multiplexed sessions, we expect 2 session requests (BatchCreateSessions + CreateSession)
        # For non-multiplexed, we expect 1 session request (BatchCreateSessions)
        mux_enabled = is_multiplexed_enabled(transaction_type)
        expected_session_requests = 2 if mux_enabled else 1
        extra_session_requests = (
            session_requests_before - expected_session_requests
        )

        # Adjust sequence numbers based on extra session requests
        adjusted_segments = []
        for method, seq_nums in expected_segments:
            # Adjust the sequence number (5th element in the tuple)
            adjusted_seq_nums = list(seq_nums)
            adjusted_seq_nums[4] += extra_session_requests
            adjusted_segments.append((method, tuple(adjusted_seq_nums)))

        return adjusted_segments
