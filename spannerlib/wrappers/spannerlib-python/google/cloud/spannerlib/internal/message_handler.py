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

from __future__ import absolute_import

import ctypes
import logging

from google.cloud.spannerlib.internal.message import Message
from google.cloud.spannerlib.internal.spannerlib import SpannerLib

logger = logging.getLogger(__name__)


class MessageHandler:
    def __init__(self, message: Message, lib: SpannerLib) -> None:
        self._message = message
        self._lib = lib

    @property
    def message(self) -> Message:
        return self._message

    def code(self) -> int:
        return self._message.error_code

    def has_error(self) -> str:
        return self._message.error_code != 0

    def error_message(self) -> str:
        error_msg = "Unknown error"
        try:
            go_error_msg = ctypes.cast(self._message.msg, ctypes.c_char_p).value
            if go_error_msg:
                error_msg = (
                    f": {go_error_msg.decode('utf-8', errors='replace')}"
                )
            logger.error(
                f"SpannerLib Error {self._message.error_code}: {error_msg}"
            )
        except Exception as e:
            logger.error(f"Failed to decode error message from Go: {e}")
            error_msg = f" (Failed to decode error message: {e})"

        return error_msg

    def dispose(self) -> None:
        self._lib.Release(self._message.pinner_id)
