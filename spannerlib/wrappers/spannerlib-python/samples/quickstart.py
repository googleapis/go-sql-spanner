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
from google.cloud.spannerlib import Pool, SpannerLibError  # noqa: E402

from ._helper import EMULATOR_TEST_CONNECTION_STRING, setup_test_env


def run_quickstart(test_connection_string):
    try:
        pool = Pool.create_pool(test_connection_string)
        print(f"Successfully created pool with ID: {pool.id}")
    except SpannerLibError as e:
        print(f"Error creating pool: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    setup_test_env()
    run_quickstart(EMULATOR_TEST_CONNECTION_STRING)
