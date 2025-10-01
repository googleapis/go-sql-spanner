class SpannerError(Exception):
    """Base exception for spannerlib_py."""

    pass


class SpannerLibraryError(SpannerError):
    """Error related to the underlying Go library call."""

    def __init__(self, message, error_code=None):
        super().__init__(message)
        self.error_code = error_code


class SpannerPoolError(SpannerError):
    """Error related to Pool operations."""

    pass


class SpannerConnectionError(SpannerError):
    """Error related to Connection operations."""

    pass
