import ctypes
import logging
import os

from ..errors import SpannerLibraryError
from .types import GoString, _GoReturn

logger = logging.getLogger(__name__)

# Load the shared library
_lib_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "../../../../spannerlib-artifacts/spannerlib.so"
    )
)
try:
    _lib = ctypes.CDLL(_lib_path)
except OSError as e:
    logger.error(f"Failed to load shared library from {_lib_path}: {e}")
    raise SpannerLibraryError(f"Failed to load shared library: {e}")


def _check_error(ret: _GoReturn, func_name: str):
    """Checks the return value from Go functions for errors."""
    if ret.r1 != 0:
        error_msg = f"{func_name} failed"
        if ret.r3 != 0:
            try:
                # Attempt to convert the error message from bytes
                go_error_msg = ctypes.cast(ret.r4, ctypes.c_char_p).value
                if go_error_msg:
                    error_msg += f": {go_error_msg.decode('utf-8', errors='replace')}"
            except Exception as e:
                error_msg += f" (Failed to decode error message: {e})"
        logger.error(error_msg)
        raise SpannerLibraryError(error_msg, error_code=ret.r1)
    if ret.r2 != 0:
        try:
            _lib.Release(ret.r2)
        except Exception as e:
            logger.warning(f"Error releasing pinnerId {ret.r2}: {e}")


# --- Function Definitions ---
# These are set up to match the exported functions in spannerlib.h

# Release
_lib.Release.argtypes = [ctypes.c_longlong]
_lib.Release.restype = ctypes.c_int32

# CreatePool
_lib.CreatePool.argtypes = [GoString]
_lib.CreatePool.restype = _GoReturn

# ClosePool
_lib.ClosePool.argtypes = [ctypes.c_longlong]
_lib.ClosePool.restype = _GoReturn

# CreateConnection
_lib.CreateConnection.argtypes = [ctypes.c_longlong]
_lib.CreateConnection.restype = _GoReturn

# CloseConnection
_lib.CloseConnection.argtypes = [ctypes.c_longlong, ctypes.c_longlong]
_lib.CloseConnection.restype = _GoReturn
