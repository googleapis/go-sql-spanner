# frozen_string_literal: true

require_relative "ffi"
require_relative "connection"

class Pool
  attr_reader :id

  def initialize(id)
    @id = id
    @closed = false
  end

  # Create a new Pool given a DSN string. Raises SpannerLibException on failure.
  def self.create_pool(dsn)
    begin
      pool_id = SpannerLib.create_pool(dsn)
    rescue StandardError => e
      raise SpannerLibException, e.message
    end

    raise SpannerLibException, "failed to create pool" if pool_id.nil? || pool_id <= 0

    Pool.new(pool_id)
  end

  # Close this pool and free native resources.
  def close
    return if @closed

    SpannerLib.close_pool(@id)
    @closed = true
  end

  # Create a new Connection associated with this Pool.
  def create_connection
    raise SpannerLibException, "pool closed" if @closed

    begin
      conn_id = SpannerLib.create_connection(@id)
    rescue StandardError => e
      raise SpannerLibException, e.message
    end

    raise SpannerLibException, "failed to create connection" if conn_id.nil? || conn_id <= 0

    Connection.new(@id, conn_id)
  end
end
