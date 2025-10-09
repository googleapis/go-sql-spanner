from __future__ import absolute_import

import ctypes
import logging

from google.cloud.spannerlib.internal.types import GoReturn, GoSlice, GoString

logger = logging.getLogger(__name__)


def log_go_string(go_string: GoString):
    """Helper function to logger.debug the contents of a GoString for debugging."""
    logger.debug(f"GoString Length (n): {go_string.n}")
    if go_string.p:
        try:
            py_string = go_string.p[: go_string.n].decode("utf-8")
            logger.debug(f"GoString Content (p): {py_string}")
        except UnicodeDecodeError:
            logger.debug(
                f"GoString Content (p) as bytes: {go_string.p[:go_string.n]}"
            )
    else:
        logger.debug("GoString Content (p): NULL")


def log_go_slice(slc: GoSlice):
    """Helper function to logger.debug the contents of a GoSlice for debugging."""
    logger.debug("--- GoSlice ---")
    logger.debug(f"  Len: {slc.len}, Cap: {slc.cap}")
    logger.debug(f"  Data Address: {hex(slc.data) if slc.data else 'None'}")

    # Check if the slice has any data to read
    if not slc.data or slc.len == 0:
        logger.debug("  Content: (empty)")
        logger.debug("---------------")
        return

    # Read slc.len bytes from the memory address in slc.data
    content_bytes = ctypes.string_at(slc.data, slc.len)
    logger.debug(f"  Content (raw bytes): {content_bytes}")

    # Attempt to decode the bytes as a UTF-8 string for readability
    try:
        content_string = content_bytes.decode("utf-8")
        logger.debug(f"  Content (decoded str): '{content_string}'")
    except UnicodeDecodeError:
        logger.debug("  Content (decoded str): [Data is not valid UTF-8]")


def log_go_return(go_return: GoReturn):
    """Helper function to logger.debug the contents of a GoReturn for debugging."""
    logger.debug(
        f"GoReturn: pinner_id: {go_return.pinner_id}, "
        f"error_code: {go_return.error_code}, "
        f"object_id: {go_return.object_id}, "
        f"msg_len: {go_return.msg_len}"
    )
    if go_return.msg_len:
        retrieved_bytes = ctypes.string_at(go_return.msg, go_return.msg_len)
        retrieved_string = retrieved_bytes.decode("utf-8")
        logger.debug(f"GoReturn Message: {retrieved_string}")
