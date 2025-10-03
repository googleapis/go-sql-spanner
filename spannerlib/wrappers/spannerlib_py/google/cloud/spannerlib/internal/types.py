import ctypes


# Define GoString structure
class GoString(ctypes.Structure):
    """Represents a Go string.pina"""

    _fields_ = [("p", ctypes.c_char_p), ("n", ctypes.c_ssize_t)]


# Define common return structure
class GoReturn(ctypes.Structure):
    """Represents the common return structure from Go functions."""

    _fields_ = [
        ("r0", ctypes.c_longlong),  # result pinnerId
        ("r1", ctypes.c_int32),  # error code
        ("r2", ctypes.c_longlong),  # object code
        ("r3", ctypes.c_int32),  # msg length
        ("r4", ctypes.c_void_p),  # msg string
    ]


def to_go_string(s: str) -> GoString:
    """Converts a Python string to a GoString."""
    encoded_s = s.encode("utf-8")
    return GoString(encoded_s, len(encoded_s))
