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

import ctypes
import logging
from pathlib import Path
import platform
from typing import ClassVar, Final, Optional

from .errors import SpannerLibError
from .message import Message
from .types import GoString

logger = logging.getLogger(__name__)

MODULE_ROOT: Final[Path] = Path(__file__).resolve().parent
LIB_DIR_NAME: Final[str] = "lib"


class SpannerLib:
    """
    A Singleton wrapper for the SpannerLib shared library.
    """

    _lib_handle: ClassVar[Optional[ctypes.CDLL]] = None
    _instance: ClassVar[Optional["SpannerLib"]] = None

    def __new__(cls) -> "SpannerLib":
        if cls._instance is None:
            cls._instance = super(SpannerLib, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self) -> None:
        """
        Internal initialization logic. Called only once by __new__.
        """
        if SpannerLib._lib_handle is not None:
            return

        lib_path = self._get_lib_path()
        logger.debug("Attempting to load SpannerLib from: %s", lib_path)

        try:
            # ctypes requires a string path
            SpannerLib._lib_handle = ctypes.CDLL(str(lib_path))

            self._configure_signatures()

            logger.info("Successfully loaded shared library: %s", str(lib_path))

        except (OSError, FileNotFoundError) as e:
            logger.critical(
                "Failed to load native library at %s", str(lib_path)
            )
            SpannerLib._lib_handle = None
            raise SpannerLibError(
                f"Could not load native dependency '{lib_path.name}': {e}"
            ) from e

    def _configure_signatures(self) -> None:
        """
        Defines the argument and return types for the C functions.
        """
        lib = SpannerLib._lib_handle
        if lib is None:
            raise SpannerLibError(
                "Library handle is None during configuration."
            )

        try:
            # 1. Release
            if hasattr(lib, "Release"):
                lib.Release.argtypes = [ctypes.c_longlong]
                lib.Release.restype = ctypes.c_int32

            # 2. CreatePool
            if hasattr(lib, "CreatePool"):
                lib.CreatePool.argtypes = [GoString]
                lib.CreatePool.restype = Message

            # 3. ClosePool
            if hasattr(lib, "ClosePool"):
                lib.ClosePool.argtypes = [ctypes.c_longlong]
                lib.ClosePool.restype = Message

        except AttributeError as e:
            raise SpannerLibError(
                f"Symbol missing in native library: {e}"
            ) from e

    @staticmethod
    def _get_lib_path() -> Path:
        """
        Resolves the absolute path to the shared library based on the OS.
        Uses if/elif for Python 3.8 compatibility.
        """
        system_name = platform.system()

        filename: str = ""

        if system_name == "Windows":
            filename = "spannerlib.dll"
        elif system_name == "Darwin":
            filename = "spannerlib.dylib"
        elif system_name == "Linux":
            filename = "spannerlib.so"
        else:
            raise SpannerLibError(
                f"Unsupported operating system: {system_name}"
            )

        full_path = MODULE_ROOT / LIB_DIR_NAME / filename

        if not full_path.exists():
            raise SpannerLibError(
                f"Library file not found at expected path: {full_path}"
            )

        return full_path

    @property
    def lib(self) -> ctypes.CDLL:
        """Returns the loaded shared library handle."""
        if self._lib_handle is None:
            raise SpannerLibError(
                "SpannerLib has not been initialized correctly."
            )
        return self._lib_handle

    def release(self, handle: int) -> int:
        """Calls the Release function from the shared library."""
        return self.lib.Release(ctypes.c_longlong(handle))

    def create_pool(self, config_str: str) -> Message:
        """Calls the CreatePool function from the shared library."""
        config_bytes = config_str.encode("utf-8")
        go_str = GoString(config_bytes, len(config_bytes))
        return self.lib.CreatePool(go_str)

    def close_pool(self, pool_handle: int) -> Message:
        """Calls the ClosePool function from the shared library."""
        return self.lib.ClosePool(ctypes.c_longlong(pool_handle))
