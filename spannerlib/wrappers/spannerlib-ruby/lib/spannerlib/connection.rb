# frozen_string_literal: true

require_relative "ffi"

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

    SpannerLib.write_mutations(@pool_id, @conn_id, req_bytes)
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
    SpannerLib.commit(@pool_id, @conn_id)
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
    SpannerLib.execute(@pool_id, @conn_id, bytes)
  end

  # Execute batch DML/DDL request. Returns ExecuteBatchDmlResponse bytes (or nil).
  def execute_batch(request)
    bytes = if request.respond_to?(:to_proto)
              request.to_proto
            else
              request.is_a?(String) ? request : request.to_s
            end
    SpannerLib.execute_batch(@pool_id, @conn_id, bytes)
  end

  # Rows helpers — return raw message bytes (caller should parse them).
  def metadata(rows_id)
    SpannerLib.metadata(@pool_id, @conn_id, rows_id)
  end

  def next_rows(rows_id, num_rows, encoding = 0)
    SpannerLib.next(@pool_id, @conn_id, rows_id, num_rows, encoding)
  end

  def result_set_stats(rows_id)
    SpannerLib.result_set_stats(@pool_id, @conn_id, rows_id)
  end

  def close_rows(rows_id)
    SpannerLib.close_rows(@pool_id, @conn_id, rows_id)
  end

  # Closes this connection. Any active transaction on the connection is rolled back.
  def close
    SpannerLib.close_connection(@pool_id, @conn_id)
    nil
  end
end
