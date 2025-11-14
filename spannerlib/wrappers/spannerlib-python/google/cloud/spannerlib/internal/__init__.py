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

"""Internal module for the spannerlib package."""
from __future__ import absolute_import

from google.cloud.spannerlib.internal.errors import (
    SpannerError,
    SpannerLibError,
)
from google.cloud.spannerlib.internal.message import Message
from google.cloud.spannerlib.internal.spannerlib import SpannerLib
from google.cloud.spannerlib.internal.types import (
    GoSlice,
    GoString,
    to_go_string,
)

__all__ = [
    "GoSlice",
    "GoString",
    "Message",
    "SpannerLib",
    "SpannerError",
    "SpannerLibError",
    "to_go_string",
]
