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

"""Internal error types for the spannerlib package."""
from __future__ import absolute_import


class SpannerError(Exception):
    """Base exception for spannerlib_py."""

    pass


class SpannerLibError(SpannerError):
    """Error related to an underlying Go library call.
    """

    def __init__(self, message: str, error_code: int | None = None) -> None:
        """Initializes a SpannerLibraryError.

        Args:
            message (str): The error message.
            error_code (int | None): The optional error code from the Go library.
        """
        super().__init__(message)
        self.error_code = error_code
