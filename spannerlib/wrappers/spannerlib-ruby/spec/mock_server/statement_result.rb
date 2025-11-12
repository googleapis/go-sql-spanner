# frozen_string_literal: true

# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "base64"
require "securerandom"

class StatementResult
  QUERY = 1
  UPDATE_COUNT = 2
  EXCEPTION = 3

  def self.create_select1_result
    create_single_int_result_set "Col1", 1
  end

  def self.create_dialect_result
    metadata = Google::Cloud::Spanner::V1::ResultSetMetadata.new(
      row_type: Google::Cloud::Spanner::V1::StructType.new(
        fields: [
          Google::Cloud::Spanner::V1::StructType::Field.new(
            name: "option_value",
            type: Google::Cloud::Spanner::V1::Type.new(
              code: Google::Cloud::Spanner::V1::TypeCode::STRING
            )
          )
        ]
      )
    )
    rows = [
      Google::Protobuf::ListValue.new(
        values: [
          Google::Protobuf::Value.new(
            string_value: "GOOGLE_STANDARD_SQL"
          )
        ]
      )
    ]
    result_set_proto = Google::Cloud::Spanner::V1::ResultSet.new(
      metadata: metadata,
      rows: rows
    )
    new(result_set_proto)
  end

  def self.create_single_int_result_set(col_name, value)
    col1 = Google::Cloud::Spanner::V1::StructType::Field.new name: col_name, type: Google::Cloud::Spanner::V1::Type.new(code: Google::Cloud::Spanner::V1::TypeCode::INT64)
    metadata = Google::Cloud::Spanner::V1::ResultSetMetadata.new
    metadata.row_type = Google::Cloud::Spanner::V1::StructType.new
    metadata.row_type.fields << col1
    result_set = Google::Cloud::Spanner::V1::ResultSet.new
    result_set.metadata = metadata
    row = Google::Protobuf::ListValue.new
    row.values << Google::Protobuf::Value.new(string_value: value.to_s)
    result_set.rows << row

    StatementResult.new(result_set)
  end

  def self.create_random_result(row_count)
    col_bool = Google::Cloud::Spanner::V1::StructType::Field.new name: "ColBool", type: Google::Cloud::Spanner::V1::Type.new(code: Google::Cloud::Spanner::V1::TypeCode::BOOL)
    col_int64 = Google::Cloud::Spanner::V1::StructType::Field.new name: "ColInt64", type: Google::Cloud::Spanner::V1::Type.new(code: Google::Cloud::Spanner::V1::TypeCode::INT64)
    col_float64 = Google::Cloud::Spanner::V1::StructType::Field.new name: "ColFloat64", type: Google::Cloud::Spanner::V1::Type.new(code: Google::Cloud::Spanner::V1::TypeCode::FLOAT64)
    col_numeric = Google::Cloud::Spanner::V1::StructType::Field.new name: "ColNumeric", type: Google::Cloud::Spanner::V1::Type.new(code: Google::Cloud::Spanner::V1::TypeCode::NUMERIC)
    col_string = Google::Cloud::Spanner::V1::StructType::Field.new name: "ColString", type: Google::Cloud::Spanner::V1::Type.new(code: Google::Cloud::Spanner::V1::TypeCode::STRING)
    col_bytes = Google::Cloud::Spanner::V1::StructType::Field.new name: "ColBytes", type: Google::Cloud::Spanner::V1::Type.new(code: Google::Cloud::Spanner::V1::TypeCode::BYTES)
    col_date = Google::Cloud::Spanner::V1::StructType::Field.new name: "ColDate", type: Google::Cloud::Spanner::V1::Type.new(code: Google::Cloud::Spanner::V1::TypeCode::DATE)
    col_timestamp = Google::Cloud::Spanner::V1::StructType::Field.new name: "ColTimestamp", type: Google::Cloud::Spanner::V1::Type.new(code: Google::Cloud::Spanner::V1::TypeCode::TIMESTAMP)
    col_json = Google::Cloud::Spanner::V1::StructType::Field.new name: "ColJson", type: Google::Cloud::Spanner::V1::Type.new(code: Google::Cloud::Spanner::V1::TypeCode::JSON)

    metadata = Google::Cloud::Spanner::V1::ResultSetMetadata.new row_type: Google::Cloud::Spanner::V1::StructType.new
    metadata.row_type.fields.push(col_bool, col_int64, col_float64, col_numeric, col_string, col_bytes, col_date,
                                  col_timestamp, col_json)
    result_set = Google::Cloud::Spanner::V1::ResultSet.new metadata: metadata

    (1..row_count).each do |_i|
      row = Google::Protobuf::ListValue.new
      row.values.push(
        random_value_or_null(Google::Protobuf::Value.new(bool_value: [true, false].sample), 10),
        random_value_or_null(Google::Protobuf::Value.new(string_value: SecureRandom.random_number(1_000_000).to_s), 10),
        random_value_or_null(Google::Protobuf::Value.new(number_value: SecureRandom.random_number), 10),
        random_value_or_null(Google::Protobuf::Value.new(string_value: SecureRandom.random_number(999_999.99).round(2).to_s), 10),
        random_value_or_null(Google::Protobuf::Value.new(string_value: SecureRandom.alphanumeric(SecureRandom.random_number(10..200))), 10),
        random_value_or_null(
          Google::Protobuf::Value.new(string_value: Base64.encode64(SecureRandom.alphanumeric(SecureRandom.random_number(10..200)))), 10
        ),
        random_value_or_null(Google::Protobuf::Value.new(string_value: format("%04d-%02d-%02d",
                                                                              SecureRandom.random_number(1900..2021),
                                                                              SecureRandom.random_number(1..12),
                                                                              SecureRandom.random_number(1..28))), 10),
        random_value_or_null(Google::Protobuf::Value.new(string_value: random_timestamp_string), 10),
        random_value_or_null(
          Google::Protobuf::Value.new(string_value: "{\"key\": \"#{SecureRandom.alphanumeric(SecureRandom.random_number(10..200))}\"}"), 10
        )
      )
      result_set.rows.push row
    end

    StatementResult.new result_set
  end

  def self.random_timestamp_string
    format("%04d-%02d-%02dT%02d:%02d:%02d.%dZ",
           SecureRandom.random_number(1900..2021),
           SecureRandom.random_number(1..12),
           SecureRandom.random_number(1..28),
           SecureRandom.random_number(0..23),
           SecureRandom.random_number(0..59),
           SecureRandom.random_number(0..59),
           SecureRandom.random_number(1..999_999_999))
  end

  def self.random_value_or_null(value, null_fraction_divisor)
    if SecureRandom.random_number(1..null_fraction_divisor) == 1
      Google::Protobuf::Value.new(null_value: "NULL_VALUE")
    else
      value
    end
  end

  attr_reader :result_type

  def initialize(result)
    case result
    when Google::Cloud::Spanner::V1::ResultSet
      @result_type = QUERY
      @result = result
    when Integer
      @result_type = UPDATE_COUNT
      @result = create_update_count_result result
    when Exception
      @result_type = EXCEPTION
      @result = result
    else
      raise ArgumentError, "result must be either a ResultSet, an Integer (update count) or an exception"
    end
  end

  def metadata(transaction = nil)
    return @result.metadata unless transaction
    return Google::Cloud::Spanner::V1::ResultSetMetadata.new transaction: transaction \
        unless @result.metadata

    metadata = @result.metadata.clone
    metadata.transaction = transaction
    metadata
  end

  def result(transaction = nil)
    res = @result.clone
    res.metadata = metadata(transaction) if res.respond_to?(:metadata=)
    res
  end

  def each(transaction = nil)
    @result.metadata = metadata transaction if @result.respond_to?(:metadata=) && transaction
    return enum_for(:each, transaction) unless block_given?

    if @result.rows.empty?
      partial = Google::Cloud::Spanner::V1::PartialResultSet.new
      partial.metadata = @result.metadata
      partial.stats = @result.stats&.clone
      yield partial
    else
      @result.rows.each do |row|
        index = 0
        partial = Google::Cloud::Spanner::V1::PartialResultSet.new
        partial.metadata = @result.metadata if index == 0
        index += 1
        partial.stats = @result.stats&.clone if index == @result.rows.length
        partial.values = row.values
        yield partial
      end
    end
  end

  def self.create_update_count_result(update_count)
    result_set = Google::Cloud::Spanner::V1::ResultSet.new
    metadata = Google::Cloud::Spanner::V1::ResultSetMetadata.new
    metadata.row_type = Google::Cloud::Spanner::V1::StructType.new
    result_set.metadata = metadata
    result_set.stats = Google::Cloud::Spanner::V1::ResultSetStats.new
    result_set.stats.row_count_exact = update_count
    new(result_set)
  end
end
