from .internal import SpannerError


class SpannerPoolError(SpannerError):
    """Error related to Pool operations."""

    pass


class SpannerConnectionError(SpannerError):
    """Error related to Connection operations."""

    pass
