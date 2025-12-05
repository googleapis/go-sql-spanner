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
Helper functions for testing.
"""

from os import getenv

from google.cloud.spanner_v1.database_sessions_manager import TransactionType


def is_multiplexed_enabled(transaction_type: TransactionType) -> bool:
    """Returns whether multiplexed sessions are enabled for the given
    transaction type.
    """

    env_var = "GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS"
    env_var_partitioned = (
        "GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_PARTITIONED_OPS"
    )
    env_var_read_write = "GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS_FOR_RW"

    def _getenv(val: str) -> bool:
        return getenv(val, "true").lower().strip() != "false"

    if transaction_type is TransactionType.READ_ONLY:
        return _getenv(env_var)
    elif transaction_type is TransactionType.PARTITIONED:
        return _getenv(env_var) and _getenv(env_var_partitioned)
    else:
        return _getenv(env_var) and _getenv(env_var_read_write)
