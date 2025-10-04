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

import ctypes
import logging
import os
import threading

from .errors import SpannerLibraryError
from .types import GoReturn, GoString

logger = logging.getLogger(__name__)


class Spannerlib:
    _instance = None
    _lib = None
    _load_lock = threading.Lock()

    def __init__(self):
        raise RuntimeError("Call get_instance() instead")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._load_lock:
                if cls._instance is None:
                    cls._instance = cls.__new__(cls)
                    cls._instance.load()
        return cls._instance

    def load(self):
        if Spannerlib._lib is None:
            _lib_path = os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__),
                    "../../../../spannerlib-artifacts/spannerlib.so",
                )
            )
            try:
                Spannerlib._lib = ctypes.CDLL(_lib_path)
                self._setup_functions()
            except OSError as e:
                logger.error(
                    f"Failed to load shared library from {_lib_path}: {e}"
                )
                Spannerlib._lib = None  # Ensure _lib is None if loading failed
                raise SpannerLibraryError(f"Failed to load shared library: {e}")

    def _setup_functions(self):
        if Spannerlib._lib is None:
            return

        # --- Function Definitions ---
        # These are set up to match the exported functions in spannerlib.h

        # Release
        Spannerlib._lib.Release.argtypes = [ctypes.c_longlong]
        Spannerlib._lib.Release.restype = ctypes.c_int32

        # CreatePool
        Spannerlib._lib.CreatePool.argtypes = [GoString]
        Spannerlib._lib.CreatePool.restype = GoReturn

        # ClosePool
        Spannerlib._lib.ClosePool.argtypes = [ctypes.c_longlong]
        Spannerlib._lib.ClosePool.restype = GoReturn

        # CreateConnection
        Spannerlib._lib.CreateConnection.argtypes = [ctypes.c_longlong]
        Spannerlib._lib.CreateConnection.restype = GoReturn

        # CloseConnection
        Spannerlib._lib.CloseConnection.argtypes = [
            ctypes.c_longlong,
            ctypes.c_longlong,
        ]
        Spannerlib._lib.CloseConnection.restype = GoReturn

    @staticmethod
    def check_error(ret: GoReturn, func_name: str):
        """Checks the return value from Go functions for errors."""
        if ret.error_code != 0:
            error_msg = f"{func_name} failed"
            if ret.msg_len != 0:
                try:
                    # Attempt to convert the error message from bytes
                    go_error_msg = ctypes.cast(ret.msg, ctypes.c_char_p).value
                    if go_error_msg:
                        error_msg += f": {go_error_msg.decode('utf-8', errors='replace')}"
                except Exception as e:
                    error_msg += f" (Failed to decode error message: {e})"
            logger.error(error_msg)
            # Release the pinner_ids
            if ret.pinner_id != 0:
                try:
                    lib = Spannerlib.get_instance().lib
                    if lib:
                        lib.Release(ret.pinner_id)
                except Exception as e:
                    logger.warning(
                        f"Error releasing pinnerId {ret.pinner_id}: {e}"
                    )

            raise SpannerLibraryError(error_msg, error_code=ret.msg)

    @property
    def lib(self):
        if Spannerlib._lib is None:
            self.load()
        return Spannerlib._lib


# Module-level functions to interact with the singleton
def check_error(ret: GoReturn, func_name: str):
    Spannerlib.check_error(ret, func_name)


def get_lib():
    return Spannerlib.get_instance().lib


# Attempt to initialize the singleton on module load
try:
    Spannerlib.get_instance()
except SpannerLibraryError:
    logger.error("Spannerlib failed to initialize on module load.")
