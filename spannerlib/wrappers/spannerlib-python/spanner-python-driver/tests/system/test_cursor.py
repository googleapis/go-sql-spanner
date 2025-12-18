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
"""Tests for cursor.py"""
from google.cloud.spanner_python_driver import connect

from ._helper import get_test_connection_string


class TestCursor:

    def test_execute(self):
        """Test the execute method."""
        connection_string = get_test_connection_string()

        # Test Context Manager
        with connect(connection_string) as connection:
            assert connection is not None

            # Test Cursor Context Manager
            with connection.cursor() as cursor:
                assert cursor is not None

                # Test execute and fetchone
                cursor.execute("SELECT 1 AS col1")
                assert cursor.description is not None
                assert cursor.description[0][0] == "col1"
                assert (
                    cursor.description[0][1] == "INT64"
                )  # TypeCode.INT64 maps to 'INT64' string as per our types.py

                result = cursor.fetchone()
                assert result == ("1",)
