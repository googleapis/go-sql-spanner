#!/usr/bin/env python

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
import os

EMULATOR_TEST_CONNECTION_STRING = (
    "localhost:9010"
    "/projects/test-project"
    "/instances/test-instance"
    "/databases/testdb"
    "?autoConfigEmulator=true"
)


def setup_test_env():
    # Set environment variable for Spanner Emulator if not set
    if not os.environ.get("SPANNER_EMULATOR_HOST"):
        os.environ["SPANNER_EMULATOR_HOST"] = "localhost:9010"
        print(
            f"Set SPANNER_EMULATOR_HOST to {os.environ['SPANNER_EMULATOR_HOST']}"
        )
