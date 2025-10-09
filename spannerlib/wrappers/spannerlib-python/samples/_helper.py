#!/usr/bin/env python

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


def format_results(metadata, rows_data):
    """Formats the results as a table string."""
    if not metadata or not metadata.row_type or not metadata.row_type.fields:
        return "No column information available."

    headers = [
        field.name if field.name else "-" for field in metadata.row_type.fields
    ]
    column_widths = [len(header) for header in headers]

    # Calculate maximum width for each column
    for row in rows_data:
        for i, value in enumerate(row):
            column_widths[i] = max(column_widths[i], len(str(value)))

    header_line = " | ".join(
        header.ljust(column_widths[i]) for i, header in enumerate(headers)
    )
    separator_line = "-+-".join("-" * width for width in column_widths)

    table_lines = [header_line, separator_line]

    for row in rows_data:
        row_values = [str(value) for value in row]
        row_line = " | ".join(
            value.ljust(column_widths[i]) for i, value in enumerate(row_values)
        )
        table_lines.append(row_line)

    return "\n".join(table_lines)
