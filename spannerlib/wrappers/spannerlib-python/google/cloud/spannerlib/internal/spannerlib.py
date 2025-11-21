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
"""Module for interacting with the SpannerLib shared library."""
from __future__ import absolute_import

import ctypes
import logging
import os
import platform

from google.cloud.spannerlib.internal.errors import SpannerLibError
from google.cloud.spannerlib.internal.message import Message
from google.cloud.spannerlib.internal.types import GoString

logger = logging.getLogger(__name__)

LIB_DIR = "lib"


class SpannerLib:
    def __init__(self) -> None:
        self._lib = None
        self._load()

    def _load(self) -> None:
        if self._lib is None:
            _lib_path = SpannerLib.get_lib_path()
            logger.info(f"Loading shared library from {_lib_path}")
            try:
                self._lib = ctypes.CDLL(_lib_path)
                self._setup_functions()
            except OSError as e:
                logger.error(
                    f"Failed to load shared library from {_lib_path}: {e}"
                )
                self._lib = None  # Ensure _lib is None if loading failed
                raise SpannerLibError(
                    f"Failed to load shared library: {e}"
                ) from e

    def _setup_functions(self) -> None:
        if self._lib is None:
            return

        # --- Function Definitions ---
        # These are set up to match the exported functions in spannerlib.h

        # Release
        self._lib.Release.argtypes = [ctypes.c_longlong]
        self._lib.Release.restype = ctypes.c_int32

        # CreatePool
        self._lib.CreatePool.argtypes = [GoString]
        self._lib.CreatePool.restype = Message

        # ClosePool
        self._lib.ClosePool.argtypes = [ctypes.c_longlong]
        self._lib.ClosePool.restype = Message

    @classmethod
    def get_lib_name(cls) -> str:
        """Gets the platform-specific library name.

        Returns:
            str: The name of the shared library file.
        """
        system = platform.system()
        if system == "Windows":
            return "spannerlib.dll"
        elif system == "Darwin":
            return "spannerlib.dylib"
        elif system == "Linux":
            return "spannerlib.so"
        else:
            raise SpannerLibError(f"Unsupported operating system: {system}")

    @classmethod
    def get_lib_path(cls) -> str:
        """Gets the absolute path to the shared library.

        Returns:
            str: The absolute file path to the shared library.
        """
        _lib_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                LIB_DIR,
                SpannerLib.get_lib_name(),
            )
        )
        return _lib_path
