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

require_relative "ffi"
require_relative "rows"

class Connection
  attr_reader :pool_id, :conn_id

  def initialize(pool_id, conn_id)
    @pool_id = pool_id
    @conn_id = conn_id
  end

  # Accepts either an object that responds to `to_proto` or a raw string/bytes
  # containing the serialized mutation proto. We avoid requiring the protobuf
  # definitions at load time so specs that don't need them can run.
  def write_mutations(mutation_group)
    req_bytes = if mutation_group.respond_to?(:to_proto)
                  mutation_group.to_proto
                elsif mutation_group.is_a?(String)
                  mutation_group
                else
                  mutation_group.to_s
                end
    SpannerLib.write_mutations(@pool_id, @conn_id, req_bytes, proto_klass: Google::Cloud::Spanner::V1::CommitResponse)
  end

  # Begin a read/write transaction on this connection. Accepts TransactionOptions proto or bytes.
  # Returns message bytes (or nil) — higher-level parsing not implemented here.
  def begin_transaction(transaction_options = nil)
    bytes = if transaction_options.respond_to?(:to_proto)
              transaction_options.to_proto
            else
              transaction_options.is_a?(String) ? transaction_options : transaction_options&.to_s
            end
    SpannerLib.begin_transaction(@pool_id, @conn_id, bytes)
  end

  # Commit the current transaction. Returns CommitResponse bytes or nil.
  def commit
    SpannerLib.commit(@pool_id, @conn_id, proto_klass: Google::Cloud::Spanner::V1::CommitResponse)
  end

  # Rollback the current transaction.
  def rollback
    SpannerLib.rollback(@pool_id, @conn_id)
    nil
  end

  # Execute SQL request (expects a request object with to_proto or raw bytes). Returns message bytes (or nil).
  def execute(request)
    bytes = if request.respond_to?(:to_proto)
              request.to_proto
            else
              request.is_a?(String) ? request : request.to_s
            end
    rows_id = SpannerLib.execute(@pool_id, @conn_id, bytes)
    SpannerLib::Rows.new(self, rows_id)
  end

  # Execute batch DML/DDL request. Returns ExecuteBatchDmlResponse bytes (or nil).
  def execute_batch(request)
    bytes = if request.respond_to?(:to_proto)
              request.to_proto
            else
              request.is_a?(String) ? request : request.to_s
            end

    SpannerLib.execute_batch(@pool_id, @conn_id, bytes, proto_klass: Google::Cloud::Spanner::V1::ExecuteBatchDmlResponse)
  end

  # Closes this connection. Any active transaction on the connection is rolled back.
  def close
    SpannerLib.close_connection(@pool_id, @conn_id)
    nil
  end
end
