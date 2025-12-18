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
"""Tests for connection.py"""

import os
import pytest
from google.cloud.spanner_python_driver import connect
from ._helper import get_test_connection_string

class TestConnectMethod:
    """Tests for the connection.py module."""

    def test_connect(self):
        """Test the connect method."""
        connection_string = get_test_connection_string()
        connection = connect(connection_string)

        try:
            assert connection is not None
            cursor = connection.cursor()
            assert cursor is not None
        finally:
            connection.close()