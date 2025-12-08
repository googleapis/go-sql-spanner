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
from typing import Optional


class SpannerError(Exception):
    """Base exception for all spannerlib-python wrapper errors.

    Catching this exception guarantees catching any error raised explicitly
    by this library.
    """


_GRPC_STATUS_CODE_TO_NAME = {
    0: "OK",
    1: "CANCELLED",
    2: "UNKNOWN",
    3: "INVALID_ARGUMENT",
    4: "DEADLINE_EXCEEDED",
    5: "NOT_FOUND",
    6: "ALREADY_EXISTS",
    7: "PERMISSION_DENIED",
    8: "RESOURCE_EXHAUSTED",
    9: "FAILED_PRECONDITION",
    10: "ABORTED",
    11: "OUT_OF_RANGE",
    12: "UNIMPLEMENTED",
    13: "INTERNAL",
    14: "UNAVAILABLE",
    15: "DATA_LOSS",
    16: "UNAUTHENTICATED",
}


class SpannerLibError(SpannerError):
    """Exception raised when the underlying Go library returns an error code."""

    def __init__(self, message: str, error_code: Optional[int] = None) -> None:
        """Initializes the SpannerLibError.

        Args:
            message (str): The error description.
            error_code (Optional[int]): The gRPC status code
                (e.g., 5 for NOT_FOUND).
        """
        self.message = message
        self.error_code = error_code

        # Format the string representation for immediate clarity in logs.
        # Example: "[Err 5 (NOT_FOUND)] Object not found"
        if error_code is not None:
            status_name = _GRPC_STATUS_CODE_TO_NAME.get(error_code)
            if status_name:
                formatted_message = (
                    f"[Err {error_code} ({status_name})] {message}"
                )
            else:
                formatted_message = f"[Err {error_code}] {message}"
        else:
            formatted_message = message

        # Initialize the base Exception with the formatted message so
        # standard Python logging/printing tools show the code automatically.
        super().__init__(formatted_message)

    def __repr__(self) -> str:
        """Standard unambiguous representation for debugging."""
        return (
            f"<{self.__class__.__name__}(code={self.error_code}, "
            f"message='{self.message}')>"
        )
