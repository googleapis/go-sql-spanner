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
"""Types."""
import datetime

def Date(year, month, day):
    return datetime.date(year, month, day)


def Time(hour, minute, second):
    return datetime.time(hour, minute, second)


def Timestamp(year, month, day, hour, minute, second):
    return datetime.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks):
    return datetime.date.fromtimestamp(ticks)


def TimeFromTicks(ticks):
    return datetime.time(
        *datetime.datetime.fromtimestamp(ticks).timetuple()[:3]
    )


def TimestampFromTicks(ticks):
    return datetime.datetime.fromtimestamp(ticks)


def Binary(string):
    return bytes(string, "utf-8") if isinstance(string, str) else bytes(string)


# Type Objects for description comparison
class DBAPITypeObject:
    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        return other in self.values


STRING = DBAPITypeObject("STRING")
BINARY = DBAPITypeObject("BYTES", "PROTO")
NUMBER = DBAPITypeObject("INT64", "FLOAT64", "NUMERIC")
DATETIME = DBAPITypeObject("TIMESTAMP", "DATE")
ROWID = DBAPITypeObject()
