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

import pytest

from google.cloud.spannerlib.internal import errors  # type: ignore


class TestSpannerErrors:
    """Test suite for Spanner error classes."""

    def test_spanner_error_inheritance(self) -> None:
        """Test that SpannerError inherits from Exception."""
        assert issubclass(errors.SpannerError, Exception)


class TestSpannerLibError:
    """Test suite for SpannerLibError class."""

    def test_spanner_lib_error_inheritance(self) -> None:
        """Test that SpannerLibError inherits from SpannerError."""
        assert issubclass(errors.SpannerLibError, errors.SpannerError)

    def test_spanner_lib_error_init_with_code(self) -> None:
        """Test SpannerLibError initialization with an error code."""
        msg = "Test error message"
        code = 101
        err = errors.SpannerLibError(msg, code)

        assert err.raw_message == msg
        assert err.error_code == code
        assert str(err) == f"[Err {code}] {msg}"

    def test_spanner_lib_error_init_without_code(self) -> None:
        """Test SpannerLibError initialization without an error code."""
        msg = "Another test error"
        err = errors.SpannerLibError(msg)

        assert err.raw_message == msg
        assert err.error_code is None
        assert str(err) == msg

    def test_spanner_lib_error_repr_with_code(self) -> None:
        """Test the __repr__ method of SpannerLibError with an error code."""
        msg = "Repr test"
        code = 404
        err = errors.SpannerLibError(msg, code)
        expected_repr = f"<SpannerLibError(code={code}, message='{msg}')>"
        assert repr(err) == expected_repr

    def test_spanner_lib_error_repr_without_code(self) -> None:
        """Test the __repr__ method of SpannerLibError without an error code."""
        msg = "Repr test no code"
        err = errors.SpannerLibError(msg)
        expected_repr = f"<SpannerLibError(code=None, message='{msg}')>"
        assert repr(err) == expected_repr

    def test_raise_spanner_error(self) -> None:
        """Test that SpannerError can be raised and caught."""
        with pytest.raises(errors.SpannerError):
            raise errors.SpannerError("Something went wrong")

    def test_raise_spanner_lib_error(self) -> None:
        """Test that SpannerLibError can be raised and caught."""
        with pytest.raises(errors.SpannerLibError):
            raise errors.SpannerLibError("Go library failed", 1)
