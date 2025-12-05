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

# frozen_string_literal: true

require "google/spanner/v1/result_set_pb"

module SpannerLib
  class Rows
    include Enumerable

    attr_reader :id, :connection

    def initialize(connection, rows_id)
      @connection = connection
      @id = rows_id
      @closed = false
      @metadata = nil
      @stats = nil
    end

    def each
      return enum_for(:each) unless block_given?

      while (row = self.next)
        yield row
      end
    ensure
      close
    end

    def next
      return nil if @closed

      row_data = SpannerLib.next(connection.pool_id, connection.conn_id, id, 1, 0)

      return nil if row_data.nil? || row_data.empty?

      row_data
    end

    def next_result_set
      return nil if @closed

      @stats = nil

      res = SpannerLib.next_result_set(connection.pool_id, connection.conn_id, id)

      if res.nil? || res.empty?
        close
        return nil
      end
      @metadata = Google::Cloud::Spanner::V1::ResultSetMetadata.decode(res)
    end

    def metadata
      return @metadata if @metadata
      return nil if @closed

      raw = SpannerLib.metadata(connection.pool_id, connection.conn_id, id)
      return nil if raw.nil? || raw.empty?

      @metadata = Google::Cloud::Spanner::V1::ResultSetMetadata.decode(raw)
    end

    def result_set_stats
      return @stats if @stats
      return nil if @closed

      raw = SpannerLib.result_set_stats(connection.pool_id, connection.conn_id, id)
      return nil if raw.nil? || raw.empty?

      @stats = Google::Cloud::Spanner::V1::ResultSetStats.decode(raw)
    end

    def close
      return if @closed

      SpannerLib.close_rows(connection.pool_id, connection.conn_id, id)
      @closed = true
    end
  end
end
