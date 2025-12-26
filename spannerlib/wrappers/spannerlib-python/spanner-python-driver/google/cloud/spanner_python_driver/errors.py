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
"""
Spanner Python Driver Errors.

DBAPI-defined Exceptions are defined in the following hierarchy::

    Exceptions
    |__Warning
    |__Error
       |__InterfaceError
       |__DatabaseError
          |__DataError
          |__OperationalError
          |__IntegrityError
          |__InternalError
          |__ProgrammingError
          |__NotSupportedError

"""
from google.api_core.exceptions import GoogleAPICallError


class Warning(Exception):
    """Important DB API warning."""

    pass


class Error(Exception):
    """The base class for all the DB API exceptions.

    Does not include :class:`Warning`.
    """

    def _is_error_cause_instance_of_google_api_exception(self):
        return isinstance(self.__cause__, GoogleAPICallError)

    @property
    def reason(self):
        """The reason of the error.
        Reference:
            https://cloud.google.com/apis/design/errors#error_info
        Returns:
            Union[str, None]: An optional string containing reason of the error.
        """
        return (
            self.__cause__.reason
            if self._is_error_cause_instance_of_google_api_exception()
            else None
        )

    @property
    def domain(self):
        """The logical grouping to which the "reason" belongs.
        Reference:
            https://cloud.google.com/apis/design/errors#error_info
        Returns:
            Union[str, None]: An optional string containing a logical grouping
            to which the "reason" belongs.
        """
        return (
            self.__cause__.domain
            if self._is_error_cause_instance_of_google_api_exception()
            else None
        )

    @property
    def metadata(self):
        """Additional structured details about this error.
        Reference:
            https://cloud.google.com/apis/design/errors#error_info
        Returns:
            Union[Dict[str, str], None]: An optional object containing
            structured details about the error.
        """
        return (
            self.__cause__.metadata
            if self._is_error_cause_instance_of_google_api_exception()
            else None
        )

    @property
    def details(self):
        """Information contained in google.rpc.status.details.
        Reference:
            https://cloud.google.com/apis/design/errors#error_model
            https://cloud.google.com/apis/design/errors#error_details
        Returns:
            Sequence[Any]: A list of structured objects from
            error_details.proto
        """
        return (
            self.__cause__.details
            if self._is_error_cause_instance_of_google_api_exception()
            else None
        )


class InterfaceError(Error):
    """
    Error related to the database interface
    rather than the database itself.
    """

    pass


class DatabaseError(Error):
    """Error related to the database."""

    pass


class DataError(DatabaseError):
    """
    Error due to problems with the processed data like
    division by zero, numeric value out of range, etc.
    """

    pass


class OperationalError(DatabaseError):
    """
    Error related to the database's operation, e.g. an
    unexpected disconnect, the data source name is not
    found, a transaction could not be processed, a
    memory allocation error, etc.
    """

    pass


class IntegrityError(DatabaseError):
    """
    Error for cases of relational integrity of the database
    is affected, e.g. a foreign key check fails.
    """

    pass


class InternalError(DatabaseError):
    """
    Internal database error, e.g. the cursor is not valid
    anymore, the transaction is out of sync, etc.
    """

    pass


class ProgrammingError(DatabaseError):
    """
    Programming error, e.g. table not found or already
    exists, syntax error in the SQL statement, wrong
    number of parameters specified, etc.
    """

    pass


class NotSupportedError(DatabaseError):
    """
    Error for case of a method or database API not
    supported by the database was used.
    """

    pass


def map_spanner_error(error):
    """Map SpannerLibError or GoogleAPICallError to DB API 2.0 errors."""
    from google.api_core import exceptions
    from google.cloud.spannerlib.internal.errors import SpannerLibError

    if isinstance(error, SpannerLibError):
        return DatabaseError(error)
    if isinstance(error, exceptions.AlreadyExists):
        return IntegrityError(error)
    if isinstance(error, exceptions.NotFound):
        return ProgrammingError(error)
    if isinstance(error, exceptions.InvalidArgument):
        return ProgrammingError(error)
    if isinstance(error, exceptions.FailedPrecondition):
        return OperationalError(error)
    if isinstance(error, exceptions.OutOfRange):
        return DataError(error)
    if isinstance(error, exceptions.Unauthenticated):
        return OperationalError(error)
    if isinstance(error, exceptions.PermissionDenied):
        return OperationalError(error)
    if isinstance(error, exceptions.DeadlineExceeded):
        return OperationalError(error)
    if isinstance(error, exceptions.ServiceUnavailable):
        return OperationalError(error)
    if isinstance(error, exceptions.Aborted):
        return OperationalError(error)
    if isinstance(error, exceptions.InternalServerError):
        return InternalError(error)
    if isinstance(error, exceptions.Unknown):
        return DatabaseError(error)
    if isinstance(error, exceptions.Cancelled):
        return OperationalError(error)
    if isinstance(error, exceptions.DataLoss):
        return DatabaseError(error)

    return DatabaseError(error)
