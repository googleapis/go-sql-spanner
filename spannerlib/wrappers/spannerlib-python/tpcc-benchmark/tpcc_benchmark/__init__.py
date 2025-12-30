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
TPC-C Benchmark Implementation for Spanner Python DBAPI.

This package contains the drivers and utilities to run a TPC-C like benchmark
against Cloud Spanner using the Python DBAPI 2.0 specification.
"""
from typing import Final

from .spanner_tpcc import SpannerTPCCDriver

__version__: Final[str] = "0.0.1"

__all__: list[str] = [
    "SpannerTPCCDriver",
]
